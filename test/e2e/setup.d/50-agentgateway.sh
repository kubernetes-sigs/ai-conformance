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

# Installs the Gateway API CRDs and agentgateway for TestGatewayAPIInference
# and records the matching go test flags. kind has no LoadBalancer, so the
# GatewayClass provisions ClusterIP Services.

set -o errexit
set -o nounset
set -o pipefail

GATEWAY_API_VERSION="${GATEWAY_API_VERSION:-v1.6.3}"
AGENTGATEWAY_VERSION="${AGENTGATEWAY_VERSION:-v1.7.0}"
GATEWAY_CLASS="agentgateway-clusterip"

echo "Installing Gateway API ${GATEWAY_API_VERSION} CRDs..."
kubectl apply --server-side --force-conflicts \
    -f "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/standard-install.yaml"

echo "Installing agentgateway ${AGENTGATEWAY_VERSION}..."
helm upgrade -i agentgateway-crds oci://cr.agentgateway.dev/charts/agentgateway-crds \
    --version "${AGENTGATEWAY_VERSION}" \
    --namespace agentgateway-system \
    --create-namespace
helm upgrade -i agentgateway oci://cr.agentgateway.dev/charts/agentgateway \
    --version "${AGENTGATEWAY_VERSION}" \
    --namespace agentgateway-system \
    --wait --timeout 10m

kubectl apply -f - <<MANIFEST
apiVersion: agentgateway.dev/v1alpha1
kind: AgentgatewayParameters
metadata:
  name: clusterip
  namespace: agentgateway-system
spec:
  service:
    spec:
      type: ClusterIP
---
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: ${GATEWAY_CLASS}
spec:
  controllerName: agentgateway.dev/agentgateway
  parametersRef:
    group: agentgateway.dev
    kind: AgentgatewayParameters
    name: clusterip
    namespace: agentgateway-system
MANIFEST
kubectl wait --for=condition=Accepted "gatewayclass/${GATEWAY_CLASS}" --timeout=5m

echo "-gateway-api-class=${GATEWAY_CLASS}" >>"${E2E_TEST_ARGS_FILE}"
