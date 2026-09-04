# DeployManager

DeployManager keeps Kubernetes configuration changes separate from application image releases.

- Store the intended immutable image digest in an `ImageRelease`.
- Let Deployments, StatefulSets, and CronJobs opt in with annotations.
- Keep `container.image` out of normal workload manifests.
- Resolve an image directly or from an existing Flux `ImagePolicy`.

## Install

```sh
kubectl apply -f https://raw.githubusercontent.com/miracle2k/deploymanager/main/config/install.yaml
kubectl -n deploymanager-system rollout status deployment/deploymanager-controller-manager
```

## Hello World

Create an image release using your immutable digest:

```yaml
apiVersion: deploy.example.com/v1alpha1
kind: ImageRelease
metadata:
  name: hello-world
  namespace: default
spec:
  image: ghcr.io/example/hello-world@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

Then opt a named container into it. Notice that the normal configuration has no `image` field:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hello-world-web
  namespace: default
  annotations:
    deploy.example.com/image-release: hello-world
    deploy.example.com/image-release-container: app
spec:
  selector:
    matchLabels: {app.kubernetes.io/name: hello-world-web}
  template:
    metadata:
      labels: {app.kubernetes.io/name: hello-world-web}
    spec:
      containers:
        - name: app
```

Apply the `ImageRelease`, bootstrap the workload once as described below, then apply this normal configuration and inspect the resolved release:

```sh
kubectl get imagerelease hello-world -n default
```

More complete direct-image, Flux, and workload examples are in [`config/samples`](config/samples).

Flux users need an existing `ImagePolicy` that reports `status.latestRef.digest` (configure Flux digest reflection accordingly). DeployManager reads that selected digest; it does not scan registries or implement Flux image automation.

### First-create note

Kubernetes validates a container image before it persists a brand-new workload, so an image-free workload cannot be created without an admission injector. This MVP deliberately does not install one. Create the release first, replace the digest in [`config/samples/bootstrap-workload.yaml`](config/samples/bootstrap-workload.yaml), then run `kubectl create -f config/samples/bootstrap-workload.yaml` exactly once. After the controller has synchronized it, use only the image-free configuration above. Never run `kubectl apply` on the image-bearing bootstrap manifest. Existing workloads can be handed over without a placeholder image.

For an existing client-side-applied workload, first set its stored apply configuration to the final rendered image-free manifest with `kubectl apply set-last-applied -f <rendered-image-free-workload.yaml> --create-annotation=true`, then do the first normal apply. This prevents its old stored image from being removed during handoff.
