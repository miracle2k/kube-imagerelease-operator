// Package controllers contains the reconciliation logic for DeployManager.
//
// +kubebuilder:rbac:groups=deploy.example.com,resources=imagereleases,verbs=get;list;watch
// +kubebuilder:rbac:groups=deploy.example.com,resources=imagereleases/status,verbs=get;patch;update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=image.toolkit.fluxcd.io,resources=imagepolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
package controllers
