#!/usr/bin/env bash

# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACTS="${ARTIFACTS:-${REPO_ROOT}/_artifacts}"
mkdir -p "${ARTIFACTS}"

# Configuration
DEFAULT_GCE_ZONES="us-central1-b us-central1-c us-central1-a us-east1-c us-west1-b"
GCE_ZONES="${GCE_ZONE:-$DEFAULT_GCE_ZONES}"
GCE_MACHINE_TYPE="${GCE_MACHINE_TYPE:-n1-standard-4}"
GCE_ACCELERATOR="${GCE_ACCELERATOR:-type=nvidia-tesla-t4,count=1}"
GCE_IMAGE_FAMILY="${GCE_IMAGE_FAMILY:-common-cu129-ubuntu-2404-nvidia-580}"
GCE_IMAGE_PROJECT="${GCE_IMAGE_PROJECT:-deeplearning-platform-release}"
K8S_VERSION="${K8S_VERSION:-v1.35.0}"
GPU_OPERATOR_VERSION="${GPU_OPERATOR_VERSION:-v26.3.1}"
GANG_SCHEDULER="${GANG_SCHEDULER:-kueue}"
# gVisor (runsc) is baked into the kind node image so TestWorkloadSandboxing
# has a sandboxed RuntimeClass to exercise. Kata is not an option here: GCE
# does not offer nested virtualization on VMs with attached GPUs.
GVISOR_RELEASE_URL="${GVISOR_RELEASE_URL:-https://storage.googleapis.com/gvisor/releases/release/latest/x86_64}"
KIND_NODE_IMAGE="kindest/node-gvisor:${K8S_VERSION}"
BUILD_ID="${BUILD_ID:-$(date +%s)}"
VM_NAME="ai-conformance-e2e-${BUILD_ID}"

BOSKOS_HOST="${BOSKOS_HOST:-http://boskos.test-pods.svc.cluster.local}"
BOSKOS_RESOURCE_TYPE="${BOSKOS_RESOURCE_TYPE:-gpu-project}"
JOB_NAME="${JOB_NAME:-ai-conformance-e2e-gcp}"

# Acquire GCP Project (Prow vs Local Dev Mode)
if [[ -n "${BOSKOS_HOST:-}" && "${RUN_IN_PROW:-false}" == "true" ]]; then
    echo "=== Acquiring GCP GPU Project from Boskos ==="
    export GOPATH="${HOME}/go"
    export PATH="${GOPATH}/bin:${PATH}"

    if ! command -v boskosctl &>/dev/null; then
        echo "Installing boskosctl..."
        go install sigs.k8s.io/boskos/cmd/boskosctl@latest
    fi

    BOSKOS_RESOURCE_JSON=$(boskosctl \
        --server-url "${BOSKOS_HOST}" \
        --owner-name "${JOB_NAME}" \
        acquire \
        --type "${BOSKOS_RESOURCE_TYPE}" \
        --state free \
        --target-state busy \
        --timeout 30m)
    GCP_PROJECT=$(echo "${BOSKOS_RESOURCE_JSON}" | jq -r .name)
    export GCP_PROJECT
    echo "Acquired GCP Project: ${GCP_PROJECT}"

    # Start Boskos heartbeat in background
    boskosctl \
        --server-url "${BOSKOS_HOST}" \
        --owner-name "${JOB_NAME}" \
        heartbeat \
        --resource "${BOSKOS_RESOURCE_JSON}" \
        --period 30s &
    HEARTBEAT_PID=$!
else
    echo "=== Running in Local/Dev Mode ==="
    if [[ -z "${GCP_PROJECT:-}" ]]; then
        echo "Error: GCP_PROJECT environment variable must be set in local dev mode."
        exit 1
    fi
fi

# Cleanup handler on exit
cleanup() {
    local exit_code=$?
    echo "================================================================"
    echo "Cleaning up E2E test environment (Exit Code: ${exit_code})"
    echo "================================================================"

    if [[ -n "${HEARTBEAT_PID:-}" ]]; then
        kill "${HEARTBEAT_PID}" &>/dev/null || true
        wait "${HEARTBEAT_PID}" &>/dev/null || true
    fi

    if [[ -n "${GCP_PROJECT:-}" ]]; then
        echo "Deleting GCE VM ${VM_NAME} in project ${GCP_PROJECT}..."
        gcloud compute instances delete "${VM_NAME}" \
            --project="${GCP_PROJECT}" \
            --zone="${GCE_ZONE}" \
            --quiet || true

        if [[ -n "${BOSKOS_HOST:-}" && "${RUN_IN_PROW:-false}" == "true" ]]; then
            echo "Releasing Boskos GCP project ${GCP_PROJECT}..."
            boskosctl \
                --server-url "${BOSKOS_HOST}" \
                --owner-name "${JOB_NAME}" \
                release \
                --name "${GCP_PROJECT}" \
                --target-state dirty || true
        fi
    fi
}
trap cleanup EXIT

echo "================================================================"
echo "1. Creating GCE GPU Instance (${VM_NAME})"
echo "================================================================"
VM_CREATED=false
for zone in ${GCE_ZONES}; do
    echo "Attempting to create VM in zone: ${zone}..."
    if gcloud compute instances create "${VM_NAME}" \
        --project="${GCP_PROJECT}" \
        --zone="${zone}" \
        --machine-type="${GCE_MACHINE_TYPE}" \
        --accelerator="${GCE_ACCELERATOR}" \
        --image-family="${GCE_IMAGE_FAMILY}" \
        --image-project="${GCE_IMAGE_PROJECT}" \
        --boot-disk-size=100GB \
        --maintenance-policy=TERMINATE; then
        GCE_ZONE="${zone}"
        VM_CREATED=true
        echo "Successfully created GCE VM (${VM_NAME}) in zone ${GCE_ZONE}"
        break
    else
        echo "WARNING: Failed to create VM in zone ${zone}, trying next zone..."
    fi
done

if [[ "${VM_CREATED}" != "true" ]]; then
    echo "Error: Failed to create GCE VM in any candidate zone (${GCE_ZONES})."
    exit 1
fi

echo "Waiting for SSH to be available on ${VM_NAME}..."
for i in {1..30}; do
    if gcloud compute ssh "${VM_NAME}" --project="${GCP_PROJECT}" --zone="${GCE_ZONE}" --command="echo ready" &>/dev/null; then
        echo "SSH is ready!"
        break
    fi
    sleep 5
done

echo "================================================================"
echo "2. Setting up GPU-enabled Kubernetes (nvkind) on GCE VM"
echo "================================================================"
# Transfer codebase to VM
gcloud compute scp --project="${GCP_PROJECT}" --zone="${GCE_ZONE}" --recurse "${REPO_ROOT}" "${VM_NAME}:~/ai-conformance"

# Run setup & nvkind cluster creation on VM
gcloud compute ssh "${VM_NAME}" --project="${GCP_PROJECT}" --zone="${GCE_ZONE}" --command="bash -s" <<REMOTE_SCRIPT
set -euo pipefail

echo "Installing Docker & NVIDIA Container Toolkit..."
sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl gnupg make build-essential git jq docker.io zstd

if ! command -v nvidia-ctk >/dev/null 2>&1; then
  curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey |
    sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
  curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list |
    sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' |
    sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list > /dev/null
  sudo apt-get update -qq
  sudo apt-get install -y -qq nvidia-container-toolkit
fi

sudo usermod -aG docker \$USER

sudo nvidia-ctk runtime configure --runtime=docker --set-as-default --cdi.enabled
sudo nvidia-ctk config --set accept-nvidia-visible-devices-as-volume-mounts=true --in-place
sudo systemctl restart docker

echo "Verifying Docker GPU access..."
sudo docker run --rm --runtime=nvidia -e NVIDIA_VISIBLE_DEVICES=all ubuntu:22.04 nvidia-smi -L || true

echo "Installing Helm..."
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

echo "Installing kubectl..."
curl -fsSL -o /tmp/kubectl "https://dl.k8s.io/release/\$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 /tmp/kubectl /usr/local/bin/kubectl

echo "Installing Go, kind & nvkind..."
wget -q https://go.dev/dl/go1.24.0.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz
export PATH="/usr/local/go/bin:\${PATH}"

go install github.com/NVIDIA/nvkind/cmd/nvkind@latest
go install sigs.k8s.io/kind@latest
export PATH="\${HOME}/go/bin:\${PATH}"

echo "Building kind node image ${KIND_NODE_IMAGE} with gVisor (runsc, systrap platform)..."
mkdir -p /tmp/kind-gvisor/gvisor
pushd /tmp/kind-gvisor
wget -q "${GVISOR_RELEASE_URL}/gvisor.tar.zstd" "${GVISOR_RELEASE_URL}/gvisor.tar.zstd.sha512"
sha512sum -c gvisor.tar.zstd.sha512
tar --zstd -xf gvisor.tar.zstd -C gvisor
cat <<'EOF' > runsc.toml
[runsc_config]
# systrap needs no KVM, so it works inside a kind node container on a VM.
platform = "systrap"
EOF
cat <<EOF > Dockerfile
FROM kindest/node:${K8S_VERSION}
# runsc, containerd-shim-runsc-v1 and gvisor-bin/ must stay together.
COPY gvisor/ /usr/local/bin/
COPY runsc.toml /etc/containerd/runsc.toml
EOF
sudo docker build -t "${KIND_NODE_IMAGE}" .
popd

cat <<'EOF' > /tmp/nvkind-config.yaml.tmpl
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
featureGates:
  DynamicResourceAllocation: true
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri"]
    enable_cdi = true
  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc]
    runtime_type = "io.containerd.runsc.v1"
  [plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc.options]
    TypeUrl = "io.containerd.runsc.v1.options"
    ConfigPath = "/etc/containerd/runsc.toml"
nodes:
- role: control-plane
  kubeadmConfigPatches:
  - |
    kind: ClusterConfiguration
    apiServer:
      extraArgs:
        feature-gates: "DynamicResourceAllocation=true"
    controllerManager:
      extraArgs:
        feature-gates: "DynamicResourceAllocation=true"
    scheduler:
      extraArgs:
        feature-gates: "DynamicResourceAllocation=true"
  - |
    kind: InitConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        feature-gates: "DynamicResourceAllocation=true"
- role: worker
  kubeadmConfigPatches:
  - |
    kind: JoinConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        feature-gates: "DynamicResourceAllocation=true"
  {{- range \$gpu := until numGPUs }}
  extraMounts:
  - hostPath: /dev/null
    containerPath: /var/run/nvidia-container-devices/{{ \$gpu }}
  {{- end }}
EOF

echo "Creating nvkind cluster (DRA enabled)..."
sudo -E env PATH="\${PATH}" nvkind cluster create \
    --name ai-conformance-cluster \
    --image "${KIND_NODE_IMAGE}" \
    --config-template /tmp/nvkind-config.yaml.tmpl

kubectl wait --for=condition=Ready nodes --all --timeout=300s
kubectl label node --all nvidia.com/gpu.present=true feature.node.kubernetes.io/pci-10de.present=true --overwrite

echo "Registering gVisor RuntimeClass..."
cat <<EOF | kubectl apply -f -
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: gvisor
handler: runsc
EOF
kubectl get runtimeclass

echo "nvkind cluster GPUs:"
sudo -E env PATH="\${PATH}" nvkind cluster print-gpus --name ai-conformance-cluster || true
kubectl get nodes -o wide
REMOTE_SCRIPT

echo "================================================================"
echo "3. Deploying Cluster Prerequisites (NVIDIA DRA Driver & Kueue)"
echo "================================================================"
gcloud compute ssh "${VM_NAME}" --project="${GCP_PROJECT}" --zone="${GCE_ZONE}" --command="bash -s" <<REMOTE_STACK
set -euo pipefail
export PATH="/usr/local/go/bin:\${HOME}/go/bin:\${PATH}"

echo "Installing cert-manager..."
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.19.2/cert-manager.yaml
kubectl rollout status deployment -n cert-manager cert-manager --timeout=5m

echo "Labeling GPU nodes for DRA driver..."
kubectl label node --all nvidia.com/gpu.present=true feature.node.kubernetes.io/pci-10de.present=true --overwrite

echo "Installing NVIDIA DRA Driver..."
helm repo add nvidia https://helm.ngc.nvidia.com/nvidia
helm repo update
helm upgrade -i nvidia-dra-driver nvidia/nvidia-dra-driver-gpu \
    --namespace nvidia-dra-driver \
    --create-namespace \
    --set gpuResourcesEnabledOverride=true \
    --wait --timeout 10m

echo "Checking ResourceSlices & DeviceClasses:"
kubectl get deviceclasses || true
kubectl get resourceslices -o wide || true

if [ "${GANG_SCHEDULER}" = "kueue" ]; then
  echo "Installing Kueue..."
  kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.18.2/manifests.yaml

  echo "Waiting for Kueue controller manager to be ready..."
  kubectl rollout status deployment -n kueue-system kueue-controller-manager --timeout=5m

  echo "Creating Kueue resources (with retries for webhook readiness)..."
  for i in {1..10}; do
    cat <<EOF | kubectl apply -f - && break
apiVersion: kueue.x-k8s.io/v1beta2
kind: ResourceFlavor
metadata:
  name: e2e-flavor
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ClusterQueue
metadata:
  name: e2e-cq
spec:
  namespaceSelector: {}
  resourceGroups:
  - coveredResources: ["cpu", "memory"]
    flavors:
    - name: e2e-flavor
      resources:
      - name: "cpu"
        nominalQuota: 10
      - name: "memory"
        nominalQuota: 10Gi
---
apiVersion: v1
kind: Namespace
metadata:
  name: ai-conformance-gang-scheduling
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata:
  name: e2e-lq
  namespace: ai-conformance-gang-scheduling
spec:
  clusterQueue: e2e-cq
EOF
    echo "Webhook might not be ready yet, retrying in 5 seconds... ($i/10)"
    sleep 5
  done

  echo "Waiting for ClusterQueue to be active..."
  kubectl wait --for=condition=Active clusterqueue/e2e-cq --timeout=60s
fi

REMOTE_STACK

echo "================================================================"
echo "4. Executing AI Conformance Test Suite (test/)"
echo "================================================================"
gcloud compute ssh "${VM_NAME}" --project="${GCP_PROJECT}" --zone="${GCE_ZONE}" --command="bash -s" <<REMOTE_TEST
set -euo pipefail
export PATH="/usr/local/go/bin:\${HOME}/go/bin:\${PATH}"

cd ~/ai-conformance
mkdir -p _artifacts

echo "Running go test ./test/..."
go test -v ./test/... \
    -accelerator-type=nvidia \
    -allocation-mode=auto \
    -gang-scheduler-namespace=ai-conformance-gang-scheduling \
    -gang-job-labels="kueue.x-k8s.io/queue-name=e2e-lq" \
    -sandbox-runtime-class=gvisor \
    -json | tee _artifacts/results.json
REMOTE_TEST

# Copy test artifacts back to host
gcloud compute scp --project="${GCP_PROJECT}" --zone="${GCE_ZONE}" --recurse "${VM_NAME}:~/ai-conformance/_artifacts/*" "${ARTIFACTS}/"

echo "================================================================"
echo "AI Conformance E2E Test Run Completed Successfully!"
echo "================================================================"
