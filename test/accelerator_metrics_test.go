// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conformance

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes"
)

var (
	metricsNamespace         *string
	metricsServiceName       *string
	metricsServicePort       *string
	metricsUtilizationMetric *string
	metricsMemoryMetric      *string
)

func init() {
	registerFlagGroup("metrics", "Accelerator Performance Metrics Flags (TestAcceleratorPerformanceMetrics)")
	metricsNamespace = flag.String("metrics-namespace", "",
		"Namespace where the accelerator metrics exporter Service lives "+
			"(e.g. gpu-operator). If empty, TestAcceleratorPerformanceMetrics is skipped.")
	metricsServiceName = flag.String("metrics-service-name", "",
		"Name of the Kubernetes Service that fronts the accelerator metrics exporter "+
			"(e.g. dcgm-exporter). If empty, TestAcceleratorPerformanceMetrics is skipped.")
	metricsServicePort = flag.String("metrics-service-port", "9400",
		"Port (name or number) on the metrics exporter Service to proxy to.")
	metricsUtilizationMetric = flag.String("metrics-utilization-metric", "",
		"Exact Prometheus metric name for per-accelerator GPU utilization "+
			"(e.g. DCGM_FI_DEV_GPU_UTIL). If empty, auto-detected by name pattern.")
	metricsMemoryMetric = flag.String("metrics-memory-metric", "",
		"Exact Prometheus metric name for per-accelerator memory usage "+
			"(e.g. DCGM_FI_DEV_FB_USED). If empty, auto-detected by name pattern.")
}

var deviceLabelKeys = []string{
	"gpu",          // NVIDIA DCGM integer device index
	"UUID",         // NVIDIA DCGM GPU UUID
	"uuid",         // lowercase UUID variant
	"device_id",    // generic device identifier
	"dev_id",       // Intel XPU Manager
	"card_id",      // AMD ROCm SMI exporter
	"GPU_I_ID",     // NVIDIA DCGM GPU instance ID (MIG)
	"minor_number", // Linux device minor number
	"pci_bus_id",   // PCI bus identifier
}

var utilizationNamePatterns = []string{"util"}

var memoryNamePatterns = []string{"fb_used", "mem_used", "memory_used", "vram_used", "hbm_used"}

// TestAcceleratorPerformanceMetrics verifies KAR-0059: Accelerator Performance Metrics.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/blob/main/kars/0059-accelerator-performance-metrics/README.md
func TestAcceleratorPerformanceMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if *metricsNamespace == "" || *metricsServiceName == "" {
		t.Skip("accelerator metrics test is not configured; if the platform does not expose per-accelerator metrics, mark accelerator_performance_metrics N/A, otherwise set -metrics-namespace and -metrics-service-name")
	}

	ctx := context.Background()
	client := getClientset(t)

	metrics, err := scrapeMetrics(ctx, client)
	if err != nil {
		t.Fatalf("FAIL: Failed to scrape metrics from %s/%s:%s: %v",
			*metricsNamespace, *metricsServiceName, *metricsServicePort, err)
	}

	t.Run("MetricsEndpointReachable", func(t *testing.T) {
		testMetricsEndpointReachable(t, metrics)
	})

	t.Run("PerAcceleratorUtilizationMetric", func(t *testing.T) {
		testPerAcceleratorMetric(t, "utilization", *metricsUtilizationMetric, utilizationNamePatterns, metrics)
	})

	t.Run("PerAcceleratorMemoryMetric", func(t *testing.T) {
		testPerAcceleratorMetric(t, "memory", *metricsMemoryMetric, memoryNamePatterns, metrics)
	})

	t.Run("OptionalMetrics", func(t *testing.T) {
		testOptionalMetrics(t, metrics)
	})
}

func testMetricsEndpointReachable(t *testing.T, metrics []prometheusMetric) {
	t.Helper()
	if len(metrics) == 0 {
		t.Fatalf("FAIL: Metrics endpoint %s/%s returned no metrics", *metricsNamespace, *metricsServiceName)
	}
	t.Logf("PASS: Metrics endpoint %s/%s returned %d metric samples", *metricsNamespace, *metricsServiceName, len(metrics))
}

func testPerAcceleratorMetric(t *testing.T, kind, exactName string, patterns []string, metrics []prometheusMetric) {
	t.Helper()
	found := findPerAcceleratorMetrics(metrics, exactName, patterns)
	if len(found) == 0 {
		if exactName != "" {
			t.Errorf("FAIL: Metric %q not found or carries no device label", exactName)
		} else {
			t.Errorf("FAIL: No per-accelerator %s metric found "+
				"(expected a metric whose name matches %v and carries a device label key from %v)",
				kind, patterns, deviceLabelKeys)
		}
		return
	}

	seen := make(map[string]bool)
	for _, m := range found {
		seen[m.name] = true
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	t.Logf("PASS: Found per-accelerator %s metric(s): %v (%d samples)", kind, names, len(found))
}

func testOptionalMetrics(t *testing.T, metrics []prometheusMetric) {
	t.Helper()
	shouldChecks := []struct {
		kind     string
		patterns []string
	}{
		{"temperature", []string{"temp", "temperature"}},
		{"power", []string{"power", "watt", "energy"}},
		{"interconnect bandwidth", []string{"nvlink", "pcie_tx", "pcie_rx", "bandwidth", "xgmi"}},
	}

	for _, check := range shouldChecks {
		found := findPerAcceleratorMetrics(metrics, "", check.patterns)
		if len(found) == 0 {
			t.Logf("SKIP (SHOULD): No per-accelerator %s metric found — hardware or virtualization layer may not expose it", check.kind)
		} else {
			t.Logf("PASS (SHOULD): Found per-accelerator %s metric (%d samples)", check.kind, len(found))
		}
	}
}

// scrapeMetrics fetches /metrics from the exporter Service via the Kubernetes
// API server proxy, avoiding any requirement for host-routable access to
// in-cluster Services.
func scrapeMetrics(ctx context.Context, client kubernetes.Interface) ([]prometheusMetric, error) {
	body, err := client.CoreV1().Services(*metricsNamespace).ProxyGet(
		"http", *metricsServiceName, *metricsServicePort, "/metrics", nil,
	).DoRaw(ctx)
	if err != nil {
		return nil, fmt.Errorf("proxy to metrics service %s/%s:%s: %w",
			*metricsNamespace, *metricsServiceName, *metricsServicePort, err)
	}
	return parsePrometheusText(string(body)), nil
}

type prometheusMetric struct {
	name   string
	labels map[string]string
	value  float64
}

func parsePrometheusText(body string) []prometheusMetric {
	var metrics []prometheusMetric
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if m, ok := parsePrometheusLine(line); ok {
			metrics = append(metrics, m)
		}
	}
	return metrics
}

func parsePrometheusLine(line string) (prometheusMetric, bool) {
	braceOpen := strings.IndexByte(line, '{')
	spaceIdx := strings.IndexByte(line, ' ')

	var name, labelsStr, rest string

	if braceOpen != -1 && (spaceIdx == -1 || braceOpen < spaceIdx) {
		name = line[:braceOpen]
		braceClose := strings.LastIndexByte(line, '}')
		if braceClose <= braceOpen {
			return prometheusMetric{}, false
		}
		labelsStr = line[braceOpen+1 : braceClose]
		rest = strings.TrimSpace(line[braceClose+1:])
	} else if spaceIdx != -1 {
		name = line[:spaceIdx]
		rest = strings.TrimSpace(line[spaceIdx+1:])
	} else {
		return prometheusMetric{}, false
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return prometheusMetric{}, false
	}

	// Extract the numeric value (first field; optional timestamp follows).
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return prometheusMetric{}, false
	}
	v, err := strconv.ParseFloat(parts[0], 64)
	if err != nil || math.IsNaN(v) {
		return prometheusMetric{}, false
	}

	return prometheusMetric{
		name:   name,
		labels: parsePrometheusLabels(labelsStr),
		value:  v,
	}, true
}

func parsePrometheusLabels(s string) map[string]string {
	labels := make(map[string]string)
	if s == "" {
		return labels
	}
	for _, pair := range splitLabelPairs(s) {
		eqIdx := strings.IndexByte(pair, '=')
		if eqIdx <= 0 {
			continue
		}
		key := strings.TrimSpace(pair[:eqIdx])
		val := strings.TrimSpace(pair[eqIdx+1:])
		if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
			val = val[1 : len(val)-1]
		}
		if key != "" && strings.TrimSpace(val) != "" {
			labels[key] = val
		}
	}
	return labels
}

func splitLabelPairs(s string) []string {
	var result []string
	var cur strings.Builder
	inQuote := false
	escaped := false
	for _, ch := range s {
		switch {
		case escaped:
			cur.WriteRune(ch)
			escaped = false
		case ch == '\\' && inQuote:
			cur.WriteRune(ch)
			escaped = true
		case ch == '"':
			inQuote = !inQuote
			cur.WriteRune(ch)
		case ch == ',' && !inQuote:
			result = append(result, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(ch)
		}
	}
	if cur.Len() > 0 {
		result = append(result, cur.String())
	}
	return result
}

func hasDeviceLabel(m prometheusMetric) bool {
	for _, key := range deviceLabelKeys {
		if _, ok := m.labels[key]; ok {
			return true
		}
	}
	return false
}

func findPerAcceleratorMetrics(metrics []prometheusMetric, exactName string, patterns []string) []prometheusMetric {
	var found []prometheusMetric
	for _, m := range metrics {
		if !hasDeviceLabel(m) {
			continue
		}
		if exactName != "" {
			if m.name == exactName {
				found = append(found, m)
			}
			continue
		}
		lower := strings.ToLower(m.name)
		for _, p := range patterns {
			if strings.Contains(lower, p) {
				found = append(found, m)
				break
			}
		}
	}
	return found
}
