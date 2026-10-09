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

# Installs kube-prometheus-stack for TestAIServiceMetrics and records the
# matching go test flags.

set -o errexit
set -o nounset
set -o pipefail

KUBE_PROMETHEUS_STACK_VERSION="${KUBE_PROMETHEUS_STACK_VERSION:-91.9.0}"

echo "Installing kube-prometheus-stack ${KUBE_PROMETHEUS_STACK_VERSION}..."
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update prometheus-community
helm upgrade -i kube-prometheus-stack prometheus-community/kube-prometheus-stack \
    --version "${KUBE_PROMETHEUS_STACK_VERSION}" \
    --namespace monitoring \
    --create-namespace \
    --set grafana.enabled=false \
    --set alertmanager.enabled=false \
    --set nodeExporter.enabled=false \
    --set kubeStateMetrics.enabled=false \
    --wait --timeout 10m
kubectl wait -n monitoring --for=condition=Available prometheus --all --timeout=5m

echo "-service-metrics-prometheus-service=monitoring/prometheus-operated:9090" >>"${E2E_TEST_ARGS_FILE}"
echo "-service-metrics-scrape-labels=release=kube-prometheus-stack" >>"${E2E_TEST_ARGS_FILE}"
