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
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

var (
	metricsEndpoint          *string
	metricsUtilizationMetric *string
	metricsMemoryMetric      *string
)

func init() {
	metricsEndpoint = flag.String("metrics-endpoint", "",
		"Prometheus-compatible metrics endpoint URL exposed by the accelerator metrics solution "+
			"(e.g. http://node-ip:9400/metrics). If empty, TestAcceleratorPerformanceMetrics is skipped.")
	metricsUtilizationMetric = flag.String("metrics-utilization-metric", "",
		"Exact Prometheus metric name for per-accelerator GPU utilization "+
			"(e.g. DCGM_FI_DEV_GPU_UTIL). If empty, auto-detected by name pattern.")
	metricsMemoryMetric = flag.String("metrics-memory-metric", "",
		"Exact Prometheus metric name for per-accelerator memory usage "+
			"(e.g. DCGM_FI_DEV_FB_USED). If empty, auto-detected by name pattern.")
}

// 30 s bounds a single scrape independent of the parent test context deadline.
var metricsHTTPClient = &http.Client{Timeout: 30 * time.Second}

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

var utilizationNamePatterns = []string{"util", "utilization"}

var memoryNamePatterns = []string{"mem", "memory", "fb", "vram", "hbm"}

// TestAcceleratorPerformanceMetrics verifies KAR-0059: Accelerator Performance Metrics.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/blob/main/kars/0059-accelerator-performance-metrics/README.md
func TestAcceleratorPerformanceMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if *metricsEndpoint == "" {
		t.Skip("Skipping TestAcceleratorPerformanceMetrics: -metrics-endpoint not set")
	}

	ctx := context.Background()

	t.Run("MetricsEndpointReachable", func(t *testing.T) {
		testMetricsEndpointReachable(ctx, t)
	})

	t.Run("PerAcceleratorUtilizationMetric", func(t *testing.T) {
		testPerAcceleratorMetric(ctx, t, "utilization", *metricsUtilizationMetric, utilizationNamePatterns)
	})

	t.Run("PerAcceleratorMemoryMetric", func(t *testing.T) {
		testPerAcceleratorMetric(ctx, t, "memory", *metricsMemoryMetric, memoryNamePatterns)
	})

	t.Run("OptionalMetrics", func(t *testing.T) {
		testOptionalMetrics(ctx, t)
	})
}

func testMetricsEndpointReachable(ctx context.Context, t *testing.T) {
	metrics, err := scrapeMetrics(ctx, *metricsEndpoint)
	if err != nil {
		t.Fatalf("FAIL: Metrics endpoint %s is not reachable: %v", *metricsEndpoint, err)
	}
	t.Logf("PASS: Metrics endpoint %s returned %d metric samples", *metricsEndpoint, len(metrics))
}

func testPerAcceleratorMetric(ctx context.Context, t *testing.T, kind, exactName string, patterns []string) {
	metrics, err := scrapeMetrics(ctx, *metricsEndpoint)
	if err != nil {
		t.Fatalf("FAIL: Failed to scrape metrics endpoint: %v", err)
	}

	found := findPerAcceleratorMetrics(metrics, exactName, patterns)
	if len(found) == 0 {
		if exactName != "" {
			t.Errorf("FAIL: Metric %q not found or carries no device label at %s", exactName, *metricsEndpoint)
		} else {
			t.Errorf("FAIL: No per-accelerator %s metric found at %s "+
				"(expected a metric whose name matches %v and carries a device label key from %v)",
				kind, *metricsEndpoint, patterns, deviceLabelKeys)
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
	t.Logf("PASS: Found per-accelerator %s metric(s): %v (%d samples)", kind, names, len(found))
}

func testOptionalMetrics(ctx context.Context, t *testing.T) {
	metrics, err := scrapeMetrics(ctx, *metricsEndpoint)
	if err != nil {
		t.Fatalf("FAIL: Failed to scrape metrics endpoint: %v", err)
	}

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

func scrapeMetrics(ctx context.Context, url string) ([]prometheusMetric, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}
	resp, err := metricsHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP GET failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics endpoint returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	return parsePrometheusText(string(body)), nil
}

type prometheusMetric struct {
	name   string
	labels map[string]string
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

	var name, labelsStr string

	if braceOpen != -1 && (spaceIdx == -1 || braceOpen < spaceIdx) {
		name = line[:braceOpen]
		braceClose := strings.LastIndexByte(line, '}')
		if braceClose <= braceOpen {
			return prometheusMetric{}, false
		}
		labelsStr = line[braceOpen+1 : braceClose]
	} else if spaceIdx != -1 {
		name = line[:spaceIdx]
	} else {
		return prometheusMetric{}, false
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return prometheusMetric{}, false
	}
	return prometheusMetric{
		name:   name,
		labels: parsePrometheusLabels(labelsStr),
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
		if key != "" {
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
