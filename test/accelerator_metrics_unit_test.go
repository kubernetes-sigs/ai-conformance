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
	"testing"
)

func TestParsePrometheusText(t *testing.T) {
	input := `# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization (in %).
# TYPE DCGM_FI_DEV_GPU_UTIL gauge
DCGM_FI_DEV_GPU_UTIL{gpu="0",UUID="GPU-abc123",container="",namespace=""} 42
DCGM_FI_DEV_GPU_UTIL{gpu="1",UUID="GPU-def456",container="",namespace=""} 0
# HELP DCGM_FI_DEV_FB_USED Framebuffer memory used (in MiB).
# TYPE DCGM_FI_DEV_FB_USED gauge
DCGM_FI_DEV_FB_USED{gpu="0",UUID="GPU-abc123"} 1024
go_goroutines 5
`

	metrics := parsePrometheusText(input)
	if len(metrics) != 4 {
		t.Fatalf("expected 4 samples, got %d", len(metrics))
	}

	if metrics[0].name != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("metrics[0].name = %q, want %q", metrics[0].name, "DCGM_FI_DEV_GPU_UTIL")
	}
	if metrics[0].labels["gpu"] != "0" {
		t.Errorf("metrics[0] gpu = %q, want %q", metrics[0].labels["gpu"], "0")
	}
	if metrics[0].labels["UUID"] != "GPU-abc123" {
		t.Errorf("metrics[0] UUID = %q, want %q", metrics[0].labels["UUID"], "GPU-abc123")
	}
	if metrics[0].labels["container"] != "" {
		t.Errorf("metrics[0] container = %q, want empty string", metrics[0].labels["container"])
	}

	if metrics[2].name != "DCGM_FI_DEV_FB_USED" {
		t.Errorf("metrics[2].name = %q, want %q", metrics[2].name, "DCGM_FI_DEV_FB_USED")
	}

	if metrics[3].name != "go_goroutines" {
		t.Errorf("metrics[3].name = %q, want %q", metrics[3].name, "go_goroutines")
	}
	if len(metrics[3].labels) != 0 {
		t.Errorf("metrics[3] labels = %v, want empty", metrics[3].labels)
	}
}

func TestSplitLabelPairs(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{
			input: `gpu="0",UUID="GPU-abc"`,
			want:  []string{`gpu="0"`, `UUID="GPU-abc"`},
		},
		{
			input: `gpu="0"`,
			want:  []string{`gpu="0"`},
		},
		{
			input: `key="value,with,commas",other="val"`,
			want:  []string{`key="value,with,commas"`, `other="val"`},
		},
		{
			input: "",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := splitLabelPairs(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("splitLabelPairs(%q) = %v, want %v", tt.input, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("[%d] got %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestHasDeviceLabel(t *testing.T) {
	tests := []struct {
		name   string
		metric prometheusMetric
		want   bool
	}{
		{
			name: "DCGM metric with gpu and UUID labels",
			metric: prometheusMetric{
				name:   "DCGM_FI_DEV_GPU_UTIL",
				labels: map[string]string{"gpu": "0", "UUID": "GPU-abc"},
			},
			want: true,
		},
		{
			name: "AMD ROCm metric with card_id label",
			metric: prometheusMetric{
				name:   "rocm_gpu_utilization",
				labels: map[string]string{"card_id": "0", "serial_number": "abc"},
			},
			want: true,
		},
		{
			name: "Intel XPU Manager metric with dev_id label",
			metric: prometheusMetric{
				name:   "xpum_gpu_utilization",
				labels: map[string]string{"dev_id": "0"},
			},
			want: true,
		},
		{
			name: "go runtime metric with no device label",
			metric: prometheusMetric{
				name:   "go_goroutines",
				labels: map[string]string{},
			},
			want: false,
		},
		{
			name: "node metric with non-device labels",
			metric: prometheusMetric{
				name:   "node_cpu_seconds_total",
				labels: map[string]string{"cpu": "0", "mode": "idle"},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasDeviceLabel(tt.metric)
			if got != tt.want {
				t.Errorf("hasDeviceLabel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParsePrometheusLineValues(t *testing.T) {
	tests := []struct {
		name  string
		line  string
		valid bool
		value float64
	}{
		{
			name:  "valid integer value",
			line:  `metric_name{gpu="0"} 42`,
			valid: true,
			value: 42,
		},
		{
			name:  "valid float value",
			line:  `metric_name{gpu="0"} 0.5`,
			valid: true,
			value: 0.5,
		},
		{
			name:  "value with timestamp accepted",
			line:  `metric_name{gpu="0"} 1.5 1234567890`,
			valid: true,
			value: 1.5,
		},
		{
			name:  "NaN value rejected",
			line:  `metric_name{gpu="0"} NaN`,
			valid: false,
		},
		{
			name:  "missing value rejected",
			line:  `metric_name{gpu="0"}`,
			valid: false,
		},
		{
			name:  "no labels missing value rejected",
			line:  `metric_name`,
			valid: false,
		},
		{
			name:  "malformed value rejected",
			line:  `metric_name{gpu="0"} not-a-number`,
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, ok := parsePrometheusLine(tt.line)
			if ok != tt.valid {
				t.Errorf("parsePrometheusLine(%q): valid=%v, want %v", tt.line, ok, tt.valid)
				return
			}
			if tt.valid && m.value != tt.value {
				t.Errorf("parsePrometheusLine(%q): value=%v, want %v", tt.line, m.value, tt.value)
			}
		})
	}
}

func TestFindPerAcceleratorMetrics(t *testing.T) {
	metrics := []prometheusMetric{
		{name: "DCGM_FI_DEV_GPU_UTIL", labels: map[string]string{"gpu": "0", "UUID": "GPU-abc"}},
		{name: "DCGM_FI_DEV_GPU_UTIL", labels: map[string]string{"gpu": "1", "UUID": "GPU-def"}},
		{name: "DCGM_FI_DEV_FB_USED", labels: map[string]string{"gpu": "0", "UUID": "GPU-abc"}},
		{name: "go_goroutines", labels: map[string]string{}},
		{name: "node_cpu_seconds_total", labels: map[string]string{"cpu": "0"}},
	}

	t.Run("auto-detect utilization by pattern", func(t *testing.T) {
		found := findPerAcceleratorMetrics(metrics, "", utilizationNamePatterns)
		if len(found) != 2 {
			t.Errorf("got %d utilization samples, want 2", len(found))
		}
	})

	t.Run("auto-detect memory by pattern", func(t *testing.T) {
		found := findPerAcceleratorMetrics(metrics, "", memoryNamePatterns)
		if len(found) != 1 {
			t.Errorf("got %d memory samples, want 1", len(found))
		}
	})

	t.Run("exact name match", func(t *testing.T) {
		found := findPerAcceleratorMetrics(metrics, "DCGM_FI_DEV_GPU_UTIL", nil)
		if len(found) != 2 {
			t.Errorf("got %d samples for exact name, want 2", len(found))
		}
	})

	t.Run("exact name with no device label excluded", func(t *testing.T) {
		found := findPerAcceleratorMetrics(metrics, "go_goroutines", nil)
		if len(found) != 0 {
			t.Errorf("got %d samples (expected 0, no device label)", len(found))
		}
	})

	t.Run("no match for cpu metric despite device-like label key", func(t *testing.T) {
		found := findPerAcceleratorMetrics(metrics, "", utilizationNamePatterns)
		for _, m := range found {
			if m.name == "node_cpu_seconds_total" {
				t.Errorf("node_cpu_seconds_total should not match utilization patterns")
			}
		}
	})
}
