package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ImageReleaseConditionReady reports whether the release source is resolved
	// and every subscribed workload was updated successfully.
	ImageReleaseConditionReady = "Ready"

	// ImageReleaseConditionSourceResolved reports whether spec resolved to an
	// immutable image digest.
	ImageReleaseConditionSourceResolved = "SourceResolved"

	// ImageReleaseConditionTargetsUpdated reports whether subscribed workloads
	// were reconciled to the resolved image.
	ImageReleaseConditionTargetsUpdated = "TargetsUpdated"
)

// ImagePolicyReference identifies an existing Flux ImagePolicy in the same
// namespace as the ImageRelease. Cross-namespace references are deliberately
// not part of this API.
type ImagePolicyReference struct {
	// Name is the name of the Flux ImagePolicy.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// ImageReleaseSpec is the desired, durable release state. Exactly one source
// must be selected: an immutable image digest or a Flux ImagePolicy.
// +kubebuilder:validation:XValidation:rule="(has(self.image) && !has(self.imagePolicyRef)) || (!has(self.image) && has(self.imagePolicyRef))",message="exactly one of image or imagePolicyRef must be specified"
type ImageReleaseSpec struct {
	// Image is an immutable OCI image reference containing a digest. A tag-only
	// reference is intentionally rejected because releases must be reproducible.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^.+@sha256:[a-fA-F0-9]{64}$`
	Image *string `json:"image,omitempty"`

	// ImagePolicyRef selects the digest reported by an existing Flux
	// image.toolkit.fluxcd.io/ImagePolicy in this ImageRelease's namespace.
	//
	// +optional
	ImagePolicyRef *ImagePolicyReference `json:"imagePolicyRef,omitempty"`
}

// ResolvedImage is the immutable image selected from the configured source.
// Image is the repository portion, while Digest is the immutable OCI digest.
// Tag is retained when a Flux ImagePolicy reported one, for display only.
type ResolvedImage struct {
	// Image is the repository portion of the resolved image reference.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Tag is the selected tag, if the source reports one. It is informational;
	// deployments always use Image@Digest.
	// +optional
	Tag string `json:"tag,omitempty"`

	// Digest is the immutable digest portion of the resolved image reference.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^sha256:[a-fA-F0-9]{64}$`
	Digest string `json:"digest"`
}

// WorkloadStatus reports the controller's last observation of one subscribed
// workload. The workload always resides in the ImageRelease's namespace.
type WorkloadStatus struct {
	// APIVersion is the workload API version, for example apps/v1.
	// +kubebuilder:validation:MinLength=1
	APIVersion string `json:"apiVersion"`

	// Kind is the workload kind, for example Deployment.
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`

	// Name is the workload name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Container is the controlled Kubernetes container name.
	// +kubebuilder:validation:MinLength=1
	Container string `json:"container"`

	// Image is the immutable image currently observed on the container.
	// +optional
	Image string `json:"image,omitempty"`

	// Ready reports whether the workload is currently ready according to its
	// native workload status.
	Ready bool `json:"ready"`
}

// ImageReleaseStatus reports the image source resolution and the state of
// workloads subscribed to this release. It is observational only; spec remains
// the durable desired state.
type ImageReleaseStatus struct {
	// ObservedGeneration is the generation most recently processed by the
	// controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ResolvedImage is the immutable image the controller resolved from spec.
	// +optional
	ResolvedImage *ResolvedImage `json:"resolvedImage,omitempty"`

	// Conditions communicates source-resolution and target-update state. The
	// controller uses Ready, SourceResolved, and TargetsUpdated condition types.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Workloads lists the subscribed workloads last observed by the controller.
	//
	// +optional
	// +listType=map
	// +listMapKey=apiVersion
	// +listMapKey=kind
	// +listMapKey=name
	// +listMapKey=container
	Workloads []WorkloadStatus `json:"workloads,omitempty"`
}

// ImageRelease is a namespaced, durable declaration of the image intended to
// run for a release. Workloads opt in independently through annotations.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=ir
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.status.resolvedImage.image`,description="Resolved image repository"
// +kubebuilder:printcolumn:name="Digest",type=string,JSONPath=`.status.resolvedImage.digest`,description="Resolved immutable digest"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`,description="Whether the release is ready"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ImageRelease struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec is the desired release source.
	Spec ImageReleaseSpec `json:"spec"`

	// Status is the controller's observation of the release and its targets.
	// +optional
	Status ImageReleaseStatus `json:"status,omitempty"`
}

// ImageReleaseList contains a list of ImageRelease resources.
// +kubebuilder:object:root=true
type ImageReleaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ImageRelease `json:"items"`
}
