IMAGE ?= ghcr.io/miracle2k/kube-imagerelease-operator:dev

.PHONY: generate manifests bundle fmt vet test build docker-build install uninstall deploy undeploy

generate:
	go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 object paths=./...

manifests:
	go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 crd:crdVersions=v1 paths=./... output:crd:artifacts:config=config/crd/bases
	go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 rbac:roleName=kube-imagerelease-operator-manager-role paths=./... output:rbac:artifacts:config=config/rbac

bundle:
	kubectl kustomize config/default > config/install.yaml

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

vet:
	go vet ./...

test:
	go test ./...

build:
	go build -o bin/kube-imagerelease-operator ./cmd

docker-build:
	docker build -t $(IMAGE) .

install:
	kubectl apply -f config/crd/bases/kube-imagerelease-operator.nix.re_imagereleases.yaml

uninstall:
	kubectl delete -f config/crd/bases/kube-imagerelease-operator.nix.re_imagereleases.yaml

deploy:
	kubectl apply -k config/default

undeploy:
	kubectl delete -k config/default
