// Package controllers contains the reconciliation logic for DeployManager.
package controllers

import (
	"context"
	_ "crypto/sha256" // register sha256 with opencontainers/go-digest
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/distribution/reference"
	digest "github.com/opencontainers/go-digest"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	deployv1alpha1 "github.com/miracle2k/deploymanager/api/v1alpha1"
)

const (
	// ImageReleaseAnnotation selects a namespace-local ImageRelease.
	ImageReleaseAnnotation = "deploy.example.com/image-release"
	// ImageReleaseContainerAnnotation identifies the named application container
	// whose image DeployManager is allowed to manage.
	ImageReleaseContainerAnnotation = "deploy.example.com/image-release-container"

	fluxImagePolicyGroup = "image.toolkit.fluxcd.io"
	fluxImagePolicyKind  = "ImagePolicy"

	imageControllerFieldManager = "deploymanager-image-controller"
)

const malformedContainerStatusName = "<missing-or-invalid>"

var errTargetNoLongerSubscribes = errors.New("workload no longer subscribes to this ImageRelease")

// ImageReleaseReconciler reconciles the durable ImageRelease state into the
// single opted-in image field of each supported workload. It intentionally has
// no authority over workload selection: the workload's own annotations opt it
// in to a release.
type ImageReleaseReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	APIReader client.Reader

	// RESTMapper is used once during setup to discover whether Flux's optional
	// ImagePolicy CRD is installed. It is injectable to make this behaviour easy
	// to test.
	RESTMapper meta.RESTMapper

	// FluxImagePolicyGVK is normally discovered from RESTMapper. Supplying it is
	// useful in tests and for installations using a non-default served version.
	FluxImagePolicyGVK schema.GroupVersionKind

	// SourceRetryInterval controls retries for expected external-source states
	// such as a not-yet-ready ImagePolicy. Defaults to one minute.
	SourceRetryInterval time.Duration
}

// SetupWithManager registers ImageRelease and supported workload watches. Flux
// ImagePolicy is watched only when its CRD is available, so DeployManager works
// normally in clusters that do not use Flux.
func (r *ImageReleaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.RESTMapper == nil {
		r.RESTMapper = mgr.GetRESTMapper()
	}

	b := ctrl.NewControllerManagedBy(mgr).
		For(&deployv1alpha1.ImageRelease{}).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.mapWorkloadToImageRelease)).
		Watches(&appsv1.StatefulSet{}, handler.EnqueueRequestsFromMapFunc(r.mapWorkloadToImageRelease)).
		Watches(&batchv1.CronJob{}, handler.EnqueueRequestsFromMapFunc(r.mapWorkloadToImageRelease))

	gvk, found := r.discoverFluxImagePolicyGVK()
	if found {
		r.FluxImagePolicyGVK = gvk
		policy := &unstructured.Unstructured{}
		policy.SetGroupVersionKind(gvk)
		b.Watches(policy, handler.EnqueueRequestsFromMapFunc(r.mapImagePolicyToImageReleases))
	} else {
		ctrl.Log.WithName("setup").Info("Flux ImagePolicy CRD not discovered; Flux watch disabled")
	}

	return b.Complete(r)
}

// Reconcile resolves the release source and applies it narrowly to every
// workload in the same namespace that explicitly subscribes to this release.
func (r *ImageReleaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var release deployv1alpha1.ImageRelease
	// Desired release state is durable in the API server. Read it directly so a
	// cache delay cannot briefly reapply an older digest after a release change.
	if err := r.apiReader().Get(ctx, req.NamespacedName, &release); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	statusBefore := release.DeepCopy()

	resolved, sourceErr := r.resolveImage(ctx, &release)
	var workloads []deployv1alpha1.WorkloadStatus
	var targetsErr error
	if sourceErr == nil {
		workloads, targetsErr = r.reconcileTargets(ctx, &release, resolved.Reference)
	}

	r.setReleaseStatus(&release, resolved, sourceErr, workloads, targetsErr)
	statusChanged := !reflect.DeepEqual(statusBefore.Status, release.Status)
	if err := r.updateStatus(ctx, statusBefore, &release); err != nil {
		return ctrl.Result{}, err
	}

	if sourceErr != nil {
		if statusChanged {
			r.eventf(&release, "Warning", "SourceResolutionFailed", "%v", sourceErr)
		}
		return ctrl.Result{RequeueAfter: r.sourceRetryInterval()}, nil
	}
	if targetsErr != nil {
		if statusChanged {
			r.eventf(&release, "Warning", "TargetUpdateFailed", "%v", targetsErr)
		}
		// Returning the error lets client-go's normal rate limiter retry update
		// failures (including RBAC failures) without treating a bad source as a
		// tight error loop.
		return ctrl.Result{}, targetsErr
	}

	if statusChanged {
		r.eventf(&release, "Normal", "TargetsUpdated", "Resolved %s and synchronized %d workload(s)", resolved.Reference, len(workloads))
	}
	// When Flux was unavailable during manager startup there is no informer to
	// add dynamically. A periodic read is a low-cost correctness backstop; when
	// Flux was present, the watch gives this path an immediate reconciliation.
	if release.Spec.ImagePolicyRef != nil {
		return ctrl.Result{RequeueAfter: r.sourceRetryInterval()}, nil
	}
	return ctrl.Result{}, nil
}

type resolvedImage struct {
	Repository string
	Tag        string
	Digest     string
	Reference  string
}

// resolveImage accepts exactly one source and returns a digest-only reference
// suitable for a Kubernetes container image field.
func (r *ImageReleaseReconciler) resolveImage(ctx context.Context, release *deployv1alpha1.ImageRelease) (resolvedImage, error) {
	if release.Spec.Image != nil && release.Spec.ImagePolicyRef != nil {
		return resolvedImage{}, fmt.Errorf("spec.image and spec.imagePolicyRef are mutually exclusive")
	}
	if release.Spec.Image == nil && release.Spec.ImagePolicyRef == nil {
		return resolvedImage{}, fmt.Errorf("one of spec.image or spec.imagePolicyRef is required")
	}

	if release.Spec.Image != nil {
		repository, digest, err := splitImmutableReference(*release.Spec.Image)
		if err != nil {
			return resolvedImage{}, fmt.Errorf("invalid spec.image: %w", err)
		}
		return resolvedImage{
			Repository: repository,
			Digest:     digest,
			Reference:  repository + "@" + digest,
		}, nil
	}

	name, err := imagePolicyReferenceName(release.Spec.ImagePolicyRef.Name)
	if err != nil {
		return resolvedImage{}, err
	}
	gvk, found := r.discoverFluxImagePolicyGVK()
	if !found {
		return resolvedImage{}, fmt.Errorf("Flux ImagePolicy CRD is not installed or was not discoverable")
	}

	policy := &unstructured.Unstructured{}
	policy.SetGroupVersionKind(gvk)
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: release.Namespace, Name: name}, policy); err != nil {
		if apierrors.IsNotFound(err) {
			return resolvedImage{}, fmt.Errorf("referenced Flux ImagePolicy %q does not exist", name)
		}
		return resolvedImage{}, fmt.Errorf("read referenced Flux ImagePolicy %q: %w", name, err)
	}

	if !fluxPolicyIsCurrentAndReady(policy) {
		return resolvedImage{}, fmt.Errorf("referenced Flux ImagePolicy %q is not Ready for its current generation", name)
	}

	latestRef, found, err := unstructured.NestedMap(policy.Object, "status", "latestRef")
	if err != nil || !found {
		return resolvedImage{}, fmt.Errorf("referenced Flux ImagePolicy %q has no status.latestRef", name)
	}
	image, _, _ := unstructured.NestedString(latestRef, "image")
	// Flux's public API currently calls this field `name`; retain `image` as
	// the primary spelling specified by DeployManager's API contract while
	// supporting both served Flux versions.
	if image == "" {
		image, _, _ = unstructured.NestedString(latestRef, "name")
	}
	tag, _, _ := unstructured.NestedString(latestRef, "tag")
	digest, _, _ := unstructured.NestedString(latestRef, "digest")
	if strings.TrimSpace(digest) == "" {
		return resolvedImage{}, fmt.Errorf("referenced Flux ImagePolicy %q has no status.latestRef.digest", name)
	}

	repository, canonicalDigest, err := composeImmutableReference(image, digest)
	if err != nil {
		return resolvedImage{}, fmt.Errorf("invalid Flux ImagePolicy %q latestRef: %w", name, err)
	}
	return resolvedImage{
		Repository: repository,
		Tag:        strings.TrimSpace(tag),
		Digest:     canonicalDigest,
		Reference:  repository + "@" + canonicalDigest,
	}, nil
}

// splitImmutableReference uses the Docker/OCI reference grammar to validate a
// supplied canonical image and removes any display tag. The resulting reference
// is always repository@sha256:... and preserves registry hosts and ports.
func splitImmutableReference(value string) (string, string, error) {
	if value == "" {
		return "", "", fmt.Errorf("image is empty")
	}
	if strings.TrimSpace(value) != value {
		return "", "", fmt.Errorf("image must not have leading or trailing whitespace")
	}
	named, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return "", "", fmt.Errorf("invalid image reference: %w", err)
	}
	canonical, ok := named.(reference.Canonical)
	if !ok {
		return "", "", fmt.Errorf("image must include an immutable digest")
	}
	return normalizedRepositoryAndDigest(canonical)
}

// composeImmutableReference validates a repository and digest. It strips a
// tag from the repository because DeployManager must write digest-only image
// references to workloads.
func composeImmutableReference(image, digest string) (string, string, error) {
	image = strings.TrimSpace(image)
	digest = strings.TrimSpace(digest)
	if image == "" {
		return "", "", fmt.Errorf("image repository is empty")
	}
	if strings.ContainsAny(image, "@ \t\r\n") {
		return "", "", fmt.Errorf("image repository is malformed")
	}
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return "", "", fmt.Errorf("invalid image repository: %w", err)
	}
	parsedDigest, err := digestpkgParse(digest)
	if err != nil {
		return "", "", fmt.Errorf("digest %q is missing or malformed", digest)
	}
	canonical, err := reference.WithDigest(reference.TrimNamed(named), parsedDigest)
	if err != nil {
		return "", "", fmt.Errorf("cannot construct immutable image reference: %w", err)
	}
	return normalizedRepositoryAndDigest(canonical)
}

// digestpkgParse is deliberately small so all resolution paths apply exactly
// the same sha256/64-hex policy after OCI syntax validation.
func digestpkgParse(value string) (digest.Digest, error) {
	parsed, err := digest.Parse(value)
	if err != nil {
		return "", err
	}
	if parsed.Algorithm() != digest.SHA256 || len(parsed.Encoded()) != 64 {
		return "", fmt.Errorf("only sha256 digests with 64 hexadecimal characters are supported")
	}
	return parsed, nil
}

func normalizedRepositoryAndDigest(canonical reference.Canonical) (string, string, error) {
	parsedDigest, err := digestpkgParse(canonical.Digest().String())
	if err != nil {
		return "", "", err
	}
	repository := reference.TrimNamed(canonical).String()
	if repository == "" {
		return "", "", fmt.Errorf("image repository is empty")
	}
	return repository, parsedDigest.String(), nil
}

func fluxPolicyIsCurrentAndReady(policy *unstructured.Unstructured) bool {
	observedGeneration, found, err := unstructured.NestedInt64(policy.Object, "status", "observedGeneration")
	if err != nil || !found || observedGeneration != policy.GetGeneration() {
		return false
	}
	conditions, found, err := unstructured.NestedSlice(policy.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}
	for _, raw := range conditions {
		condition, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		typeValue, _, _ := unstructured.NestedString(condition, "type")
		statusValue, _, _ := unstructured.NestedString(condition, "status")
		conditionGeneration, conditionObserved, conditionErr := unstructured.NestedInt64(condition, "observedGeneration")
		if typeValue == "Ready" &&
			strings.EqualFold(statusValue, "True") &&
			conditionErr == nil &&
			conditionObserved &&
			conditionGeneration == policy.GetGeneration() {
			return true
		}
	}
	return false
}

func (r *ImageReleaseReconciler) reconcileTargets(ctx context.Context, release *deployv1alpha1.ImageRelease, desiredImage string) ([]deployv1alpha1.WorkloadStatus, error) {
	var statuses []deployv1alpha1.WorkloadStatus
	var errs []error

	var deployments appsv1.DeploymentList
	if err := r.List(ctx, &deployments, client.InNamespace(release.Namespace)); err != nil {
		errs = append(errs, fmt.Errorf("list Deployments: %w", err))
	} else {
		for i := range deployments.Items {
			workload := &deployments.Items[i]
			if !subscribesTo(workload, release.Name) {
				continue
			}
			status, err := r.reconcileDeployment(ctx, workload, release.Name, desiredImage)
			if errors.Is(err, errTargetNoLongerSubscribes) {
				continue
			}
			statuses = append(statuses, status)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	var statefulSets appsv1.StatefulSetList
	if err := r.List(ctx, &statefulSets, client.InNamespace(release.Namespace)); err != nil {
		errs = append(errs, fmt.Errorf("list StatefulSets: %w", err))
	} else {
		for i := range statefulSets.Items {
			workload := &statefulSets.Items[i]
			if !subscribesTo(workload, release.Name) {
				continue
			}
			status, err := r.reconcileStatefulSet(ctx, workload, release.Name, desiredImage)
			if errors.Is(err, errTargetNoLongerSubscribes) {
				continue
			}
			statuses = append(statuses, status)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	var cronJobs batchv1.CronJobList
	if err := r.List(ctx, &cronJobs, client.InNamespace(release.Namespace)); err != nil {
		errs = append(errs, fmt.Errorf("list CronJobs: %w", err))
	} else {
		for i := range cronJobs.Items {
			workload := &cronJobs.Items[i]
			if !subscribesTo(workload, release.Name) {
				continue
			}
			status, err := r.reconcileCronJob(ctx, workload, release.Name, desiredImage)
			if errors.Is(err, errTargetNoLongerSubscribes) {
				continue
			}
			statuses = append(statuses, status)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	// The cache does not promise a stable list order. Keep status deterministic
	// so an otherwise-idempotent reconciliation never patches only to reorder
	// subscribed workload entries (which would enqueue the ImageRelease again).
	sortWorkloadStatuses(statuses)

	return statuses, joinErrors(errs)
}

func sortWorkloadStatuses(statuses []deployv1alpha1.WorkloadStatus) {
	sort.Slice(statuses, func(i, j int) bool {
		left, right := statuses[i], statuses[j]
		if left.APIVersion != right.APIVersion {
			return left.APIVersion < right.APIVersion
		}
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		return left.Container < right.Container
	})
}

func (r *ImageReleaseReconciler) reconcileDeployment(ctx context.Context, workload *appsv1.Deployment, releaseName, desiredImage string) (deployv1alpha1.WorkloadStatus, error) {
	current := &appsv1.Deployment{}
	if err := r.readCurrentWorkload(ctx, workload, current); err != nil {
		return r.workloadReadFailure("apps/v1", "Deployment", workload, err)
	}
	return r.reconcileWorkload(ctx, workloadTarget{
		APIVersion:     "apps/v1",
		Kind:           "Deployment",
		Object:         current,
		Containers:     current.Spec.Template.Spec.Containers,
		ContainersPath: "/spec/template/spec/containers",
		NativeReady:    func() bool { return deploymentReady(current) },
		SetContainerImage: func(index int, image string) {
			current.Spec.Template.Spec.Containers[index].Image = image
		},
	}, releaseName, desiredImage)
}

func (r *ImageReleaseReconciler) reconcileStatefulSet(ctx context.Context, workload *appsv1.StatefulSet, releaseName, desiredImage string) (deployv1alpha1.WorkloadStatus, error) {
	current := &appsv1.StatefulSet{}
	if err := r.readCurrentWorkload(ctx, workload, current); err != nil {
		return r.workloadReadFailure("apps/v1", "StatefulSet", workload, err)
	}
	return r.reconcileWorkload(ctx, workloadTarget{
		APIVersion:     "apps/v1",
		Kind:           "StatefulSet",
		Object:         current,
		Containers:     current.Spec.Template.Spec.Containers,
		ContainersPath: "/spec/template/spec/containers",
		NativeReady:    func() bool { return statefulSetReady(current) },
		SetContainerImage: func(index int, image string) {
			current.Spec.Template.Spec.Containers[index].Image = image
		},
	}, releaseName, desiredImage)
}

func (r *ImageReleaseReconciler) reconcileCronJob(ctx context.Context, workload *batchv1.CronJob, releaseName, desiredImage string) (deployv1alpha1.WorkloadStatus, error) {
	current := &batchv1.CronJob{}
	if err := r.readCurrentWorkload(ctx, workload, current); err != nil {
		return r.workloadReadFailure("batch/v1", "CronJob", workload, err)
	}
	return r.reconcileWorkload(ctx, workloadTarget{
		APIVersion:     "batch/v1",
		Kind:           "CronJob",
		Object:         current,
		Containers:     current.Spec.JobTemplate.Spec.Template.Spec.Containers,
		ContainersPath: "/spec/jobTemplate/spec/template/spec/containers",
		// CronJobs have no rollout-ready status. Synchronizing their Job
		// template is the useful readiness signal for future Jobs.
		NativeReady: func() bool { return true },
		SetContainerImage: func(index int, image string) {
			current.Spec.JobTemplate.Spec.Template.Spec.Containers[index].Image = image
		},
	}, releaseName, desiredImage)
}

// workloadTarget is the small adapter required to add another workload kind:
// its object, the Pod-template container location, and native readiness rule.
// It keeps all target selection and narrow image patching in one path.
type workloadTarget struct {
	APIVersion        string
	Kind              string
	Object            client.Object
	Containers        []corev1.Container
	ContainersPath    string
	NativeReady       func() bool
	SetContainerImage func(index int, image string)
}

// readCurrentWorkload avoids making authorization decisions from a possibly
// stale cached list object. APIReader is intentionally used only for the
// final, security-sensitive opt-in validation before a write.
func (r *ImageReleaseReconciler) readCurrentWorkload(ctx context.Context, source, destination client.Object) error {
	return r.apiReader().Get(ctx, types.NamespacedName{Namespace: source.GetNamespace(), Name: source.GetName()}, destination)
}

func (r *ImageReleaseReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *ImageReleaseReconciler) workloadReadFailure(apiVersion, kind string, workload client.Object, err error) (deployv1alpha1.WorkloadStatus, error) {
	status := workloadStatus(apiVersion, kind, workload.GetName(), statusContainerName(workload))
	if apierrors.IsNotFound(err) {
		return status, errTargetNoLongerSubscribes
	}
	return status, fmt.Errorf("%s/%s: read current workload before update: %w", kind, workload.GetName(), err)
}

func (r *ImageReleaseReconciler) reconcileWorkload(ctx context.Context, target workloadTarget, releaseName, desiredImage string) (deployv1alpha1.WorkloadStatus, error) {
	containerName, err := controlledContainerName(target.Object)
	status := workloadStatus(target.APIVersion, target.Kind, target.Object.GetName(), statusContainerName(target.Object))
	if err != nil {
		return status, fmt.Errorf("%s/%s: %w", target.Kind, target.Object.GetName(), err)
	}
	currentReleaseName, configured, err := referencedImageReleaseName(target.Object)
	if err != nil {
		return status, fmt.Errorf("%s/%s: %w", target.Kind, target.Object.GetName(), err)
	}
	if !configured || currentReleaseName != releaseName {
		return status, errTargetNoLongerSubscribes
	}

	index := containerIndex(target.Containers, containerName)
	if index < 0 {
		return status, fmt.Errorf("%s/%s: controlled container %q does not exist", target.Kind, target.Object.GetName(), containerName)
	}

	status.Image = target.Containers[index].Image
	imageChanged := false
	if status.Image != desiredImage {
		if err := r.applyContainerImage(ctx, target, containerName, desiredImage); err != nil {
			return status, fmt.Errorf("%s/%s: update image for container %q: %w", target.Kind, target.Object.GetName(), containerName, err)
		}
		status.Image = desiredImage
		target.SetContainerImage(index, desiredImage)
		imageChanged = true
	}
	// A successful apply has just created a new Pod template generation. Do not
	// accidentally report the old ReplicaSet/StatefulSet as ready; a later fresh
	// reconcile will evaluate native readiness after the rollout observes it.
	status.Ready = !imageChanged && target.NativeReady() && status.Image == desiredImage
	return status, nil
}

// applyContainerImage uses Server-Side Apply with a stable manager and applies
// only the selected list entry's name and image. This lets normal SSA-based
// configuration safely omit image without retaining ownership of it.
//
// target.Object was fetched directly from the API server and its resource
// version is included in the apply payload. That optimistic precondition means
// an annotation/container change between the fresh opt-in check and this apply
// is rejected instead of allowing a stale release reconciliation to mutate a
// newly repointed workload.
func (r *ImageReleaseReconciler) applyContainerImage(ctx context.Context, target workloadTarget, containerName, image string) error {
	applyObject := &unstructured.Unstructured{}
	applyObject.SetAPIVersion(target.APIVersion)
	applyObject.SetKind(target.Kind)
	applyObject.SetNamespace(target.Object.GetNamespace())
	applyObject.SetName(target.Object.GetName())
	if resourceVersion := target.Object.GetResourceVersion(); resourceVersion != "" {
		applyObject.SetResourceVersion(resourceVersion)
	}

	path := strings.Split(strings.TrimPrefix(target.ContainersPath, "/"), "/")
	if err := unstructured.SetNestedSlice(applyObject.Object, []interface{}{
		map[string]interface{}{"name": containerName, "image": image},
	}, path...); err != nil {
		return err
	}
	return r.Patch(ctx, applyObject, client.Apply, client.FieldOwner(imageControllerFieldManager), client.ForceOwnership)
}

func controlledContainerName(workload client.Object) (string, error) {
	name, configured := workload.GetAnnotations()[ImageReleaseContainerAnnotation]
	if !configured || name == "" {
		return "", fmt.Errorf("annotation %q is required", ImageReleaseContainerAnnotation)
	}
	if strings.TrimSpace(name) != name {
		return name, fmt.Errorf("annotation %q must not have leading or trailing whitespace", ImageReleaseContainerAnnotation)
	}
	if problems := validation.IsDNS1123Label(name); len(problems) > 0 {
		return name, fmt.Errorf("annotation %q must be a valid container name: %s", ImageReleaseContainerAnnotation, strings.Join(problems, "; "))
	}
	return name, nil
}

func imagePolicyReferenceName(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("spec.imagePolicyRef.name is required")
	}
	if strings.TrimSpace(name) != name {
		return "", fmt.Errorf("spec.imagePolicyRef.name must not have leading or trailing whitespace")
	}
	if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return "", fmt.Errorf("spec.imagePolicyRef.name is malformed: %s", strings.Join(problems, "; "))
	}
	return name, nil
}

// referencedImageReleaseName validates the subscription separately from
// container validation. A malformed release annotation cannot be associated
// with any ImageRelease, so the watch mapper records a workload Event for it.
func referencedImageReleaseName(workload client.Object) (string, bool, error) {
	name, configured := workload.GetAnnotations()[ImageReleaseAnnotation]
	if !configured {
		return "", false, nil
	}
	if name == "" {
		return "", true, fmt.Errorf("annotation %q is empty", ImageReleaseAnnotation)
	}
	if strings.TrimSpace(name) != name {
		return "", true, fmt.Errorf("annotation %q must not have leading or trailing whitespace", ImageReleaseAnnotation)
	}
	if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return "", true, fmt.Errorf("annotation %q is malformed: %s", ImageReleaseAnnotation, strings.Join(problems, "; "))
	}
	return name, true, nil
}

// statusContainerName preserves a valid, schema-compliant status entry even
// when the opt-in annotation itself is absent or malformed.
func statusContainerName(workload client.Object) string {
	name, configured := workload.GetAnnotations()[ImageReleaseContainerAnnotation]
	if !configured || name == "" || strings.TrimSpace(name) != name || len(validation.IsDNS1123Label(name)) > 0 {
		return malformedContainerStatusName
	}
	return name
}

func subscribesTo(workload client.Object, releaseName string) bool {
	name, configured, err := referencedImageReleaseName(workload)
	return err == nil && configured && name == releaseName
}

// containerIndex selects the managed container exclusively by its Kubernetes
// name. The resulting index is used only to read the current image; SSA uses
// the container name as the list-map key and never treats the index as identity.
func containerIndex(containers []corev1.Container, wanted string) int {
	for i := range containers {
		if containers[i].Name == wanted {
			return i
		}
	}
	return -1
}

func workloadStatus(apiVersion, kind, name, container string) deployv1alpha1.WorkloadStatus {
	return deployv1alpha1.WorkloadStatus{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       name,
		Container:  container,
	}
}

func deploymentReady(workload *appsv1.Deployment) bool {
	desired := int32(1)
	if workload.Spec.Replicas != nil {
		desired = *workload.Spec.Replicas
	}
	return workload.Status.ObservedGeneration >= workload.Generation &&
		workload.Status.UpdatedReplicas >= desired &&
		workload.Status.ReadyReplicas >= desired &&
		workload.Status.AvailableReplicas >= desired
}

func statefulSetReady(workload *appsv1.StatefulSet) bool {
	desired := int32(1)
	if workload.Spec.Replicas != nil {
		desired = *workload.Spec.Replicas
	}
	return workload.Status.ObservedGeneration >= workload.Generation &&
		workload.Status.UpdatedReplicas >= desired &&
		workload.Status.ReadyReplicas >= desired &&
		(workload.Status.UpdateRevision == "" || workload.Status.CurrentRevision == workload.Status.UpdateRevision)
}

func (r *ImageReleaseReconciler) setReleaseStatus(release *deployv1alpha1.ImageRelease, resolved resolvedImage, sourceErr error, workloads []deployv1alpha1.WorkloadStatus, targetsErr error) {
	now := metav1.Now()
	release.Status.ObservedGeneration = release.Generation
	if sourceErr == nil {
		release.Status.ResolvedImage = &deployv1alpha1.ResolvedImage{
			Image:  resolved.Repository,
			Tag:    resolved.Tag,
			Digest: resolved.Digest,
		}
		release.Status.Workloads = workloads
		setCondition(&release.Status.Conditions, metav1.Condition{
			Type:               deployv1alpha1.ImageReleaseConditionSourceResolved,
			Status:             metav1.ConditionTrue,
			Reason:             "Resolved",
			Message:            "Image source resolved to " + resolved.Reference,
			ObservedGeneration: release.Generation,
			LastTransitionTime: now,
		})
	} else {
		release.Status.ResolvedImage = nil
		release.Status.Workloads = nil
		setCondition(&release.Status.Conditions, metav1.Condition{
			Type:               deployv1alpha1.ImageReleaseConditionSourceResolved,
			Status:             metav1.ConditionFalse,
			Reason:             "ResolutionFailed",
			Message:            sourceErr.Error(),
			ObservedGeneration: release.Generation,
			LastTransitionTime: now,
		})
	}

	if sourceErr != nil {
		setCondition(&release.Status.Conditions, metav1.Condition{
			Type:               deployv1alpha1.ImageReleaseConditionTargetsUpdated,
			Status:             metav1.ConditionUnknown,
			Reason:             "WaitingForSource",
			Message:            "Targets were not updated because the image source is unresolved",
			ObservedGeneration: release.Generation,
			LastTransitionTime: now,
		})
		setCondition(&release.Status.Conditions, metav1.Condition{
			Type:               deployv1alpha1.ImageReleaseConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             "SourceResolutionFailed",
			Message:            sourceErr.Error(),
			ObservedGeneration: release.Generation,
			LastTransitionTime: now,
		})
		return
	}

	if targetsErr != nil {
		setCondition(&release.Status.Conditions, metav1.Condition{
			Type:               deployv1alpha1.ImageReleaseConditionTargetsUpdated,
			Status:             metav1.ConditionFalse,
			Reason:             "UpdateFailed",
			Message:            targetsErr.Error(),
			ObservedGeneration: release.Generation,
			LastTransitionTime: now,
		})
		setCondition(&release.Status.Conditions, metav1.Condition{
			Type:               deployv1alpha1.ImageReleaseConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             "TargetUpdateFailed",
			Message:            targetsErr.Error(),
			ObservedGeneration: release.Generation,
			LastTransitionTime: now,
		})
		return
	}

	setCondition(&release.Status.Conditions, metav1.Condition{
		Type:               deployv1alpha1.ImageReleaseConditionTargetsUpdated,
		Status:             metav1.ConditionTrue,
		Reason:             "Synchronized",
		Message:            fmt.Sprintf("Synchronized %d workload(s)", len(workloads)),
		ObservedGeneration: release.Generation,
		LastTransitionTime: now,
	})
	for _, workload := range workloads {
		if !workload.Ready {
			setCondition(&release.Status.Conditions, metav1.Condition{
				Type:               deployv1alpha1.ImageReleaseConditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             "RolloutInProgress",
				Message:            "Image source resolved and targets were synchronized; waiting for workload readiness",
				ObservedGeneration: release.Generation,
				LastTransitionTime: now,
			})
			return
		}
	}
	setCondition(&release.Status.Conditions, metav1.Condition{
		Type:               deployv1alpha1.ImageReleaseConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Ready",
		Message:            "Image source resolved and all subscribed targets are ready",
		ObservedGeneration: release.Generation,
		LastTransitionTime: now,
	})
}

func setCondition(conditions *[]metav1.Condition, condition metav1.Condition) {
	meta.SetStatusCondition(conditions, condition)
}

func (r *ImageReleaseReconciler) updateStatus(ctx context.Context, before, release *deployv1alpha1.ImageRelease) error {
	if reflect.DeepEqual(before.Status, release.Status) {
		return nil
	}
	return r.Status().Patch(ctx, release, client.MergeFrom(before))
}

func (r *ImageReleaseReconciler) sourceRetryInterval() time.Duration {
	if r.SourceRetryInterval > 0 {
		return r.SourceRetryInterval
	}
	return time.Minute
}

func (r *ImageReleaseReconciler) eventf(object client.Object, eventType, reason, messageFmt string, args ...interface{}) {
	if r.Recorder != nil {
		r.Recorder.Eventf(object, eventType, reason, messageFmt, args...)
	}
}

func (r *ImageReleaseReconciler) discoverFluxImagePolicyGVK() (schema.GroupVersionKind, bool) {
	if r.FluxImagePolicyGVK.Group != "" {
		return r.FluxImagePolicyGVK, true
	}
	if r.RESTMapper == nil {
		return schema.GroupVersionKind{}, false
	}
	mapping, err := r.RESTMapper.RESTMapping(schema.GroupKind{Group: fluxImagePolicyGroup, Kind: fluxImagePolicyKind})
	if err != nil {
		// The CRD can be installed after DeployManager starts. We cannot add an
		// informer dynamically, but resettable mappers let periodic Flux-source
		// reconciliations discover it and read it correctly.
		if resettable, ok := r.RESTMapper.(meta.ResettableRESTMapper); ok {
			resettable.Reset()
			mapping, err = r.RESTMapper.RESTMapping(schema.GroupKind{Group: fluxImagePolicyGroup, Kind: fluxImagePolicyKind})
		}
		if err != nil {
			return schema.GroupVersionKind{}, false
		}
	}
	return mapping.GroupVersionKind, true
}

func (r *ImageReleaseReconciler) mapWorkloadToImageRelease(ctx context.Context, object client.Object) []reconcile.Request {
	name, configured, err := referencedImageReleaseName(object)
	if err != nil {
		r.eventf(object, corev1.EventTypeWarning, "InvalidImageReleaseReference", "%v", err)
		return nil
	}
	if !configured {
		return nil
	}

	request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: object.GetNamespace(), Name: name}}
	var release deployv1alpha1.ImageRelease
	if err := r.Get(ctx, request.NamespacedName, &release); err != nil {
		if apierrors.IsNotFound(err) {
			r.eventf(object, corev1.EventTypeWarning, "ImageReleaseNotFound", "Referenced ImageRelease %q does not exist in namespace %q", name, object.GetNamespace())
			return nil
		}
		ctrl.LoggerFrom(ctx).Error(err, "read ImageRelease referenced by workload", "imageRelease", name, "namespace", object.GetNamespace())
		// Preserve normal retry behaviour for temporary API/cache failures.
		return []reconcile.Request{request}
	}
	return []reconcile.Request{request}
}

func (r *ImageReleaseReconciler) mapImagePolicyToImageReleases(ctx context.Context, object client.Object) []reconcile.Request {
	var releases deployv1alpha1.ImageReleaseList
	if err := r.List(ctx, &releases, client.InNamespace(object.GetNamespace())); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "list ImageReleases for Flux ImagePolicy watch", "imagePolicy", object.GetName(), "namespace", object.GetNamespace())
		return nil
	}

	requests := make([]reconcile.Request, 0)
	for i := range releases.Items {
		release := &releases.Items[i]
		if release.Spec.ImagePolicyRef == nil || strings.TrimSpace(release.Spec.ImagePolicyRef.Name) != object.GetName() {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: release.Namespace, Name: release.Name}})
	}
	return requests
}

func joinErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(parts, "; "))
}
