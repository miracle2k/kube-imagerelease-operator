// Package controllers contains the reconciliation logic for kube-imagerelease-operator.
//
// +kubebuilder:rbac:groups=kube-imagerelease-operator.nix.re,resources=imagereleases,verbs=get;list;watch
// +kubebuilder:rbac:groups=kube-imagerelease-operator.nix.re,resources=imagereleases/status,verbs=get;patch;update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=image.toolkit.fluxcd.io,resources=imagepolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
package controllers
