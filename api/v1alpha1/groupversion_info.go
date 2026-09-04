// Package v1alpha1 contains API Schema definitions for the deploy.example.com
// v1alpha1 API group.
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the API group and version used to register ImageRelease
	// objects with a Kubernetes scheme.
	GroupVersion = schema.GroupVersion{Group: "deploy.example.com", Version: "v1alpha1"}

	// SchemeBuilder registers the types in this API group.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds the types in this group-version to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&ImageRelease{}, &ImageReleaseList{})
}
