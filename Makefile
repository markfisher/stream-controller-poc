.PHONY: clean codegen codegen-verify build kubectl-apply

# This is ONE of the generated files (alongside everything in pkg/client)
# that serves as make dependency tracking
GENERATED_SOURCE = pkg/apis/streamcontroller/v1alpha1/zz_generated.deepcopy.go

GO_SOURCES = $(shell find pkg/apis -type f -name '*.go' ! -path $(GENERATED_SOURCE))

codegen: $(GENERATED_SOURCE)

$(GENERATED_SOURCE): $(GO_SOURCES) hack/vendor vendor
	hack/vendor/k8s.io/code-generator/generate-groups.sh all \
      github.com/projectriff/stream-controller/pkg/client \
      github.com/projectriff/stream-controller/pkg/apis \
	  "streamcontroller:v1alpha1" \
      --go-header-file  hack/boilerplate.go.txt
	hack/vendor/k8s.io/code-generator/generate-internal-groups.sh defaulter \
      github.com/projectriff/stream-controller/pkg/client \
      '' \
      github.com/projectriff/stream-controller/pkg/apis \
	  "streamcontroller:v1alpha1" \
      --go-header-file  hack/boilerplate.go.txt

codegen-verify: hack/vendor vendor
	hack/vendor/k8s.io/code-generator/generate-groups.sh all \
      github.com/projectriff/stream-controller/pkg/client \
      github.com/projectriff/stream-controller/pkg/apis \
	  "streamcontroller:v1alpha1" \
      --go-header-file  hack/boilerplate.go.txt \
      --verify-only

clean:
	rm -fR pkg/client
	rm -f $(GENERATED_SOURCE)

vendor: glide.lock
	glide install -v --force

glide.lock: glide.yaml
	glide up -v --force

hack/vendor: hack/glide.lock
	# Note the absence of -v
	cd hack && glide install

hack/glide.lock: hack/glide.yaml
	# Note the absence of -v
	cd hack && glide up

ifeq ($(KO_DOCKER_REPO)$(KO_FLAGS),)
override KO_FLAGS = -L
endif
kubectl-apply:
	kubectl apply -f config/stream-resource.yaml
	kubectl apply -f config/rbac.yaml
	ko apply $(KO_FLAGS) -f config/controller-deployment.yaml
