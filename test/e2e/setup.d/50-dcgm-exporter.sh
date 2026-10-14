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

set -o errexit -o nounset -o pipefail

DCGM_EXPORTER_NAMESPACE="${DCGM_EXPORTER_NAMESPACE:-dcgm-exporter}"
DCGM_EXPORTER_CHART_VERSION="${DCGM_EXPORTER_CHART_VERSION:-3.6.1}"

echo "Installing DCGM Exporter ${DCGM_EXPORTER_CHART_VERSION}..."
helm repo add gpu-helm-charts https://nvidia.github.io/dcgm-exporter/helm-charts
helm repo update

# runtimeClassName=nvidia: required for the exporter to see GPUs on nvkind, where
# the NVIDIA runtime is only configured on GPU nodes via the nvidia RuntimeClass.
# tolerations={}: clears the chart's default control-plane tolerations so the
# DaemonSet only lands on worker GPU nodes.
helm upgrade -i dcgm-exporter gpu-helm-charts/dcgm-exporter \
    --namespace "${DCGM_EXPORTER_NAMESPACE}" \
    --create-namespace \
    --version "${DCGM_EXPORTER_CHART_VERSION}" \
    --set serviceMonitor.enabled=false \
    --set runtimeClassName=nvidia \
    --set-json 'tolerations=[]' \
    --wait --timeout 10m

printf '%s\n' \
    "-metrics-namespace=${DCGM_EXPORTER_NAMESPACE}" \
    "-metrics-service-name=dcgm-exporter" \
    >> "${E2E_TEST_ARGS_FILE}"
echo "DCGM Exporter ready; metrics test will run"
