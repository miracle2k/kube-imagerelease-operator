package controllers

import (
	"context"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	imagereleasev1alpha1 "github.com/miracle2k/kube-imagerelease-operator/api/v1alpha1"
)

const (
	testNamespace = "test"
	testDigest    = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testImage     = "registry.example.com/team/app@" + testDigest
)

func TestReconcileDirectImageUpdatesOnlyNamedContainers(t *testing.T) {
	t.Parallel()

	release := testRelease("app", testImage)
	deployment := testDeployment("web", "app", "", "sidecar:old")
	statefulSet := testStatefulSet("worker", "app", "")
	cronJob := testCronJob("cleanup", "app", "")
	reconciler, c := testReconciler(t, release, deployment, statefulSet, cronJob)

	if _, err := reconciler.Reconcile(context.Background(), requestFor(release)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var gotDeployment appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "web"}, &gotDeployment); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got, want := containerImage(t, gotDeployment.Spec.Template.Spec.Containers, "app"), testImage; got != want {
		t.Fatalf("Deployment app image = %q, want %q", got, want)
	}
	if got, want := containerImage(t, gotDeployment.Spec.Template.Spec.Containers, "sidecar"), "sidecar:old"; got != want {
		t.Fatalf("uncontrolled sidecar image = %q, want %q", got, want)
	}

	var gotStatefulSet appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "worker"}, &gotStatefulSet); err != nil {
		t.Fatalf("get StatefulSet: %v", err)
	}
	if got, want := containerImage(t, gotStatefulSet.Spec.Template.Spec.Containers, "app"), testImage; got != want {
		t.Fatalf("StatefulSet app image = %q, want %q", got, want)
	}

	var gotCronJob batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "cleanup"}, &gotCronJob); err != nil {
		t.Fatalf("get CronJob: %v", err)
	}
	if got, want := containerImage(t, gotCronJob.Spec.JobTemplate.Spec.Template.Spec.Containers, "app"), testImage; got != want {
		t.Fatalf("CronJob app image = %q, want %q", got, want)
	}

	var gotRelease imagereleasev1alpha1.ImageRelease
	if err := c.Get(context.Background(), requestFor(release).NamespacedName, &gotRelease); err != nil {
		t.Fatalf("get ImageRelease: %v", err)
	}
	if gotRelease.Status.ResolvedImage == nil {
		t.Fatal("resolvedImage was not recorded")
	}
	if got, want := gotRelease.Status.ResolvedImage.Image, "registry.example.com/team/app"; got != want {
		t.Fatalf("resolved repository = %q, want %q", got, want)
	}
	if got, want := gotRelease.Status.ResolvedImage.Digest, testDigest; got != want {
		t.Fatalf("resolved digest = %q, want %q", got, want)
	}
	if got, want := len(gotRelease.Status.Workloads), 3; got != want {
		t.Fatalf("workload status entries = %d, want %d", got, want)
	}
	assertCondition(t, gotRelease.Status.Conditions, imagereleasev1alpha1.ImageReleaseConditionSourceResolved, metav1.ConditionTrue)
	assertCondition(t, gotRelease.Status.Conditions, imagereleasev1alpha1.ImageReleaseConditionTargetsUpdated, metav1.ConditionTrue)
	assertCondition(t, gotRelease.Status.Conditions, imagereleasev1alpha1.ImageReleaseConditionReady, metav1.ConditionFalse)
	if got, want := len(c.applyPayloads), 3; got != want {
		t.Fatalf("SSA payload count = %d, want %d", got, want)
	}
	for _, payload := range c.applyPayloads {
		assertNarrowImageApply(t, payload)
	}

	// A second pass finds the desired images and is a no-op.
	if _, err := reconciler.Reconcile(context.Background(), requestFor(release)); err != nil {
		t.Fatalf("idempotent reconcile: %v", err)
	}
}

func TestReconcileReportsMalformedContainerAnnotation(t *testing.T) {
	t.Parallel()

	release := testRelease("app", testImage)
	deployment := testDeployment("web", "", "", "")
	reconciler, c := testReconciler(t, release, deployment)

	if _, err := reconciler.Reconcile(context.Background(), requestFor(release)); err == nil {
		t.Fatal("reconcile succeeded with a missing controlled-container annotation")
	}

	var gotRelease imagereleasev1alpha1.ImageRelease
	if err := c.Get(context.Background(), requestFor(release).NamespacedName, &gotRelease); err != nil {
		t.Fatalf("get ImageRelease: %v", err)
	}
	assertCondition(t, gotRelease.Status.Conditions, imagereleasev1alpha1.ImageReleaseConditionTargetsUpdated, metav1.ConditionFalse)
	assertCondition(t, gotRelease.Status.Conditions, imagereleasev1alpha1.ImageReleaseConditionReady, metav1.ConditionFalse)
	if got, want := gotRelease.Status.Workloads[0].Container, malformedContainerStatusName; got != want {
		t.Fatalf("malformed target status container = %q, want %q", got, want)
	}
}

func TestResolveFluxImagePolicyUsesNameFallbackAndDigest(t *testing.T) {
	t.Parallel()

	policyGVK := schema.GroupVersionKind{Group: fluxImagePolicyGroup, Version: "v1beta2", Kind: fluxImagePolicyKind}
	policy := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": policyGVK.GroupVersion().String(),
		"kind":       policyGVK.Kind,
		"metadata": map[string]interface{}{
			"name":       "selected",
			"namespace":  testNamespace,
			"generation": int64(7),
		},
		"status": map[string]interface{}{
			"observedGeneration": int64(7),
			"conditions":         []interface{}{map[string]interface{}{"type": "Ready", "status": "True", "observedGeneration": int64(7)}},
			"latestRef": map[string]interface{}{
				"name":   "registry.example.com:5000/team/app",
				"tag":    "v1.4.3",
				"digest": testDigest,
			},
		},
	}}
	release := &imagereleasev1alpha1.ImageRelease{
		TypeMeta:   metav1.TypeMeta{APIVersion: imagereleasev1alpha1.GroupVersion.String(), Kind: "ImageRelease"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: testNamespace},
		Spec:       imagereleasev1alpha1.ImageReleaseSpec{ImagePolicyRef: &imagereleasev1alpha1.ImagePolicyReference{Name: "selected"}},
	}
	reconciler, _ := testReconciler(t, release, policy)
	reconciler.FluxImagePolicyGVK = policyGVK

	resolved, err := reconciler.resolveImage(context.Background(), release)
	if err != nil {
		t.Fatalf("resolve Flux ImagePolicy: %v", err)
	}
	if got, want := resolved.Reference, "registry.example.com:5000/team/app@"+testDigest; got != want {
		t.Fatalf("Flux reference = %q, want %q", got, want)
	}
	if got, want := resolved.Tag, "v1.4.3"; got != want {
		t.Fatalf("Flux tag = %q, want %q", got, want)
	}

	stalePolicy := policy.DeepCopy()
	stalePolicy.Object["status"].(map[string]interface{})["observedGeneration"] = int64(6)
	if fluxPolicyIsCurrentAndReady(stalePolicy) {
		t.Fatal("stale Flux status was accepted as Ready")
	}
	staleCondition := policy.DeepCopy()
	conditions := staleCondition.Object["status"].(map[string]interface{})["conditions"].([]interface{})
	conditions[0].(map[string]interface{})["observedGeneration"] = int64(6)
	if fluxPolicyIsCurrentAndReady(staleCondition) {
		t.Fatal("stale Flux Ready condition was accepted as Ready")
	}
}

func TestResolveFluxImagePolicyPrefersImageField(t *testing.T) {
	t.Parallel()

	policyGVK := schema.GroupVersionKind{Group: fluxImagePolicyGroup, Version: "v1", Kind: fluxImagePolicyKind}
	policy := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": policyGVK.GroupVersion().String(),
		"kind":       policyGVK.Kind,
		"metadata": map[string]interface{}{
			"name":       "selected",
			"namespace":  testNamespace,
			"generation": int64(1),
		},
		"status": map[string]interface{}{
			"observedGeneration": int64(1),
			"conditions":         []interface{}{map[string]interface{}{"type": "Ready", "status": "True", "observedGeneration": int64(1)}},
			"latestRef": map[string]interface{}{
				"image":  "registry.example.com/team/from-image",
				"name":   "registry.example.com/team/from-name",
				"digest": testDigest,
			},
		},
	}}
	release := &imagereleasev1alpha1.ImageRelease{
		TypeMeta:   metav1.TypeMeta{APIVersion: imagereleasev1alpha1.GroupVersion.String(), Kind: "ImageRelease"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: testNamespace},
		Spec:       imagereleasev1alpha1.ImageReleaseSpec{ImagePolicyRef: &imagereleasev1alpha1.ImagePolicyReference{Name: "selected"}},
	}
	reconciler, _ := testReconciler(t, release, policy)
	reconciler.FluxImagePolicyGVK = policyGVK

	resolved, err := reconciler.resolveImage(context.Background(), release)
	if err != nil {
		t.Fatalf("resolve Flux ImagePolicy: %v", err)
	}
	if got, want := resolved.Repository, "registry.example.com/team/from-image"; got != want {
		t.Fatalf("Flux repository = %q, want %q", got, want)
	}
}

func TestReconcileRejectsTagOnlyImage(t *testing.T) {
	t.Parallel()

	badImage := "registry.example.com/team/app:latest"
	release := testRelease("app", badImage)
	reconciler, c := testReconciler(t, release)

	result, err := reconciler.Reconcile(context.Background(), requestFor(release))
	if err != nil {
		t.Fatalf("reconcile invalid source: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatal("invalid source did not request a retry")
	}

	var gotRelease imagereleasev1alpha1.ImageRelease
	if err := c.Get(context.Background(), requestFor(release).NamespacedName, &gotRelease); err != nil {
		t.Fatalf("get ImageRelease: %v", err)
	}
	assertCondition(t, gotRelease.Status.Conditions, imagereleasev1alpha1.ImageReleaseConditionSourceResolved, metav1.ConditionFalse)
	assertCondition(t, gotRelease.Status.Conditions, imagereleasev1alpha1.ImageReleaseConditionReady, metav1.ConditionFalse)
	if gotRelease.Status.ResolvedImage != nil {
		t.Fatalf("invalid source unexpectedly resolved to %#v", gotRelease.Status.ResolvedImage)
	}
}

func TestImmutableReferenceValidation(t *testing.T) {
	t.Parallel()

	repository, digest, err := splitImmutableReference("registry.example.com:5000/team/app:display@" + testDigest)
	if err != nil {
		t.Fatalf("valid digest reference: %v", err)
	}
	if got, want := repository, "registry.example.com:5000/team/app"; got != want {
		t.Fatalf("repository = %q, want %q", got, want)
	}
	if got, want := digest, testDigest; got != want {
		t.Fatalf("digest = %q, want %q", got, want)
	}

	for _, value := range []string{
		"registry.example.com/team/app:latest",
		"registry.example.com/team/app@sha256:short",
		"registry.example.com/team/app@sha512:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	} {
		if _, _, err := splitImmutableReference(value); err == nil {
			t.Errorf("splitImmutableReference(%q) succeeded, want error", value)
		}
	}
}

func TestSortWorkloadStatusesIsDeterministic(t *testing.T) {
	t.Parallel()

	statuses := []imagereleasev1alpha1.WorkloadStatus{
		{APIVersion: "batch/v1", Kind: "CronJob", Name: "cleanup", Container: "app"},
		{APIVersion: "apps/v1", Kind: "Deployment", Name: "web", Container: "sidecar"},
		{APIVersion: "apps/v1", Kind: "Deployment", Name: "web", Container: "app"},
		{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "worker", Container: "app"},
	}

	sortWorkloadStatuses(statuses)
	got := make([]string, 0, len(statuses))
	for _, status := range statuses {
		got = append(got, status.APIVersion+"/"+status.Kind+"/"+status.Name+"/"+status.Container)
	}
	want := []string{
		"apps/v1/Deployment/web/app",
		"apps/v1/Deployment/web/sidecar",
		"apps/v1/StatefulSet/worker/app",
		"batch/v1/CronJob/cleanup/app",
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("status order = %#v, want %#v", got, want)
		}
	}
}

func testReconciler(t *testing.T, objects ...client.Object) (*ImageReleaseReconciler, *applyRecordingClient) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add Kubernetes scheme: %v", err)
	}
	if err := imagereleasev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add kube-imagerelease-operator scheme: %v", err)
	}
	base := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&imagereleasev1alpha1.ImageRelease{}).
		WithObjects(objects...).
		Build()
	c := &applyRecordingClient{Client: base}
	return &ImageReleaseReconciler{Client: c, APIReader: base, Scheme: scheme}, c
}

func assertNarrowImageApply(t *testing.T, payload *unstructured.Unstructured) {
	t.Helper()
	metadata, found, err := unstructured.NestedMap(payload.Object, "metadata")
	if err != nil || !found {
		t.Fatalf("applied payload has no metadata: found=%t err=%v", found, err)
	}
	if _, hasAnnotations := metadata["annotations"]; hasAnnotations {
		t.Fatalf("applied payload unexpectedly includes annotations: %#v", metadata)
	}
	path := []string{"spec", "template", "spec", "containers"}
	if payload.GetKind() == "CronJob" {
		path = []string{"spec", "jobTemplate", "spec", "template", "spec", "containers"}
	}
	containers, found, err := unstructured.NestedSlice(payload.Object, path...)
	if err != nil || !found || len(containers) != 1 {
		t.Fatalf("applied payload containers: found=%t len=%d err=%v", found, len(containers), err)
	}
	container, ok := containers[0].(map[string]interface{})
	if !ok || len(container) != 2 || container["name"] == "" || container["image"] != testImage {
		t.Fatalf("applied payload is not narrow name/image only: %#v", containers[0])
	}
}

// applyRecordingClient gives unit tests the one server-side behavior the
// controller-runtime fake client intentionally does not implement: Apply
// patches. It records the minimal apply payload and merges only its keyed
// container image into the underlying typed object.
type applyRecordingClient struct {
	client.Client
	applyPayloads []*unstructured.Unstructured
}

func (c *applyRecordingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if patch.Type() != types.ApplyPatchType {
		return c.Client.Patch(ctx, obj, patch, opts...)
	}

	payload, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("apply payload type = %T, want *unstructured.Unstructured", obj)
	}
	c.applyPayloads = append(c.applyPayloads, payload.DeepCopy())

	containersPath := []string{"spec", "template", "spec", "containers"}
	switch payload.GetKind() {
	case "CronJob":
		containersPath = []string{"spec", "jobTemplate", "spec", "template", "spec", "containers"}
	case "Deployment", "StatefulSet":
	default:
		return fmt.Errorf("unexpected applied kind %q", payload.GetKind())
	}
	entries, found, err := unstructured.NestedSlice(payload.Object, containersPath...)
	if err != nil || !found || len(entries) != 1 {
		return fmt.Errorf("unexpected applied containers: found=%t len=%d err=%v", found, len(entries), err)
	}
	entry, ok := entries[0].(map[string]interface{})
	if !ok {
		return fmt.Errorf("applied container entry = %T", entries[0])
	}
	name, _ := entry["name"].(string)
	image, _ := entry["image"].(string)
	if name == "" || image == "" {
		return fmt.Errorf("applied container has name=%q image=%q", name, image)
	}

	key := types.NamespacedName{Namespace: payload.GetNamespace(), Name: payload.GetName()}
	switch payload.GetKind() {
	case "Deployment":
		var target appsv1.Deployment
		if err := c.Client.Get(ctx, key, &target); err != nil {
			return err
		}
		if err := setNamedImage(target.Spec.Template.Spec.Containers, name, image); err != nil {
			return err
		}
		return c.Client.Update(ctx, &target)
	case "StatefulSet":
		var target appsv1.StatefulSet
		if err := c.Client.Get(ctx, key, &target); err != nil {
			return err
		}
		if err := setNamedImage(target.Spec.Template.Spec.Containers, name, image); err != nil {
			return err
		}
		return c.Client.Update(ctx, &target)
	case "CronJob":
		var target batchv1.CronJob
		if err := c.Client.Get(ctx, key, &target); err != nil {
			return err
		}
		if err := setNamedImage(target.Spec.JobTemplate.Spec.Template.Spec.Containers, name, image); err != nil {
			return err
		}
		return c.Client.Update(ctx, &target)
	}
	return nil
}

func setNamedImage(containers []corev1.Container, name, image string) error {
	for i := range containers {
		if containers[i].Name == name {
			containers[i].Image = image
			return nil
		}
	}
	return fmt.Errorf("container %q not found", name)
}

func testRelease(name, image string) *imagereleasev1alpha1.ImageRelease {
	return &imagereleasev1alpha1.ImageRelease{
		TypeMeta: metav1.TypeMeta{APIVersion: imagereleasev1alpha1.GroupVersion.String(), Kind: "ImageRelease"},
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  testNamespace,
			Generation: 1,
		},
		Spec: imagereleasev1alpha1.ImageReleaseSpec{Image: &image},
	}
}

func testDeployment(name, controlledContainer, appImage, sidecarImage string) *appsv1.Deployment {
	containers := []corev1.Container{{Name: "sidecar", Image: sidecarImage}, {Name: "app", Image: appImage}}
	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   testNamespace,
			Annotations: subscriptionAnnotations("app", controlledContainer),
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec:       corev1.PodSpec{Containers: containers},
			},
		},
	}
}

func testStatefulSet(name, controlledContainer, image string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   testNamespace,
			Annotations: subscriptionAnnotations("app", controlledContainer),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: image}}},
			},
		},
	}
}

func testCronJob(name, controlledContainer, image string) *batchv1.CronJob {
	return &batchv1.CronJob{
		TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "CronJob"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   testNamespace,
			Annotations: subscriptionAnnotations("app", controlledContainer),
		},
		Spec: batchv1.CronJobSpec{
			Schedule: "0 * * * *",
			JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyOnFailure, Containers: []corev1.Container{{Name: "app", Image: image}}},
			}}},
		},
	}
}

func subscriptionAnnotations(release, container string) map[string]string {
	annotations := map[string]string{ImageReleaseAnnotation: release}
	if container != "" {
		annotations[ImageReleaseContainerAnnotation] = container
	}
	return annotations
}

func requestFor(release *imagereleasev1alpha1.ImageRelease) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: release.Namespace, Name: release.Name}}
}

func containerImage(t *testing.T, containers []corev1.Container, name string) string {
	t.Helper()
	for _, container := range containers {
		if container.Name == name {
			return container.Image
		}
	}
	t.Fatalf("container %q not found", name)
	return ""
}

func assertCondition(t *testing.T, conditions []metav1.Condition, conditionType string, want metav1.ConditionStatus) {
	t.Helper()
	for _, condition := range conditions {
		if condition.Type == conditionType {
			if condition.Status != want {
				t.Fatalf("condition %s = %s, want %s (%s)", conditionType, condition.Status, want, condition.Message)
			}
			return
		}
	}
	t.Fatalf("condition %s not found", conditionType)
}
