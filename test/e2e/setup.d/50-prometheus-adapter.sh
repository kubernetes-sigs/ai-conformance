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

# Configure the custom metrics pipeline for TestHPAAutoscaling (KAR-0057).
# 50-kube-prometheus-stack.sh runs first in lexical order.

set -o errexit
set -o nounset
set -o pipefail

PROMETHEUS_ADAPTER_VERSION="${PROMETHEUS_ADAPTER_VERSION:-5.3.0}"

echo "Creating the HPA test PodMonitor..."
kubectl apply -f - <<'PODMONITOR'
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: ai-conformance-hpa
  namespace: monitoring
  labels:
    release: kube-prometheus-stack
spec:
  namespaceSelector:
    any: true
  selector:
    matchLabels:
      ai-conformance.kubernetes.io/hpa-metric-source: "true"
  podMetricsEndpoints:
  - port: metrics
    path: /metrics
    interval: 15s
PODMONITOR

# Keep the mapping consistent with the integration contract in test/README.md.
# Prometheus Operator supplies namespace and pod labels for PodMonitor targets.
adapter_values="$(mktemp)"
trap 'rm -f "${adapter_values}"' EXIT
cat >"${adapter_values}" <<'VALUES'
prometheus:
  url: http://prometheus-operated.monitoring.svc
  port: 9090
metricsRelistInterval: 15s
rules:
  default: false
  custom:
  - seriesQuery: 'ai_conformance_hpa_queue_depth{namespace!="",pod!=""}'
    resources:
      overrides:
        namespace: {resource: "namespace"}
        pod: {resource: "pod"}
    name:
      matches: "^ai_conformance_hpa_queue_depth$"
      as: "ai_conformance_hpa_queue_depth"
    metricsQuery: 'max(<<.Series>>{<<.LabelMatchers>>}) by (<<.GroupBy>>)'
VALUES

echo "Installing prometheus-adapter ${PROMETHEUS_ADAPTER_VERSION}..."
helm upgrade -i prometheus-adapter prometheus-community/prometheus-adapter \
    --version "${PROMETHEUS_ADAPTER_VERSION}" \
    --namespace monitoring \
    --values "${adapter_values}" \
    --wait --timeout 10m

echo "Waiting for the custom metrics APIService to be available..."
kubectl wait apiservice/v1beta1.custom.metrics.k8s.io \
    --for=condition=Available --timeout=5m

echo "-hpa-custom-metric-name=ai_conformance_hpa_queue_depth" >>"${E2E_TEST_ARGS_FILE}"
