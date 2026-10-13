package conformance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// Unit tests for the KAR-0001 mechanism resolution, version comparison, probe
// log parsing, and the NVIDIA probe script. They use a fake clientset and a
// mock nvidia-smi and need no cluster.

func strPtr(s string) *string { return &s }

// logCapture collects logf output so tests can assert on WARNING lines.
type logCapture struct{ lines []string }

func (l *logCapture) logf(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logCapture) warnings() []string {
	var out []string
	for _, line := range l.lines {
		if strings.HasPrefix(line, "WARNING:") {
			out = append(out, line)
		}
	}
	return out
}

func gpuNodeWithLabels(name string, gpus int64, labels map[string]string) *corev1.Node {
	node := gpuNode(name, gpus, true, false)
	node.Labels = labels
	return node
}

func withDriverVersion(slice *resourcev1.ResourceSlice, deviceIndex int, driverVer string) *resourcev1.ResourceSlice {
	slice.Spec.Devices[deviceIndex].Attributes = map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
		"driverVersion": {VersionValue: strPtr(driverVer)},
	}
	return slice
}

func TestInspectPlatformDriverRuntimeMechanism(t *testing.T) {
	ctx := context.Background()
	cfg := nvidiaConfig(t)
	const nodeName = "gpu-node-1"
	gfdLabels := map[string]string{"nvidia.com/cuda.driver-version.full": "580.65.06"}
	fromDRA := PlatformDriverRuntimeInfo{Mechanism: platformMechanismDRA, DriverVersion: "580.65.6", DriverVersionKey: "driverVersion"}
	fromGFD := PlatformDriverRuntimeInfo{Mechanism: platformMechanismNodeMetadata, DriverVersion: "580.65.06", DriverVersionKey: "nvidia.com/cuda.driver-version.full"}
	sliceGR := schema.GroupResource{Group: "resource.k8s.io", Resource: "resourceslices"}

	tests := []struct {
		name    string
		mode    string
		objects []runtime.Object
		listErr error // returned by every ResourceSlice list when set
		// want is compared field by field; NodeName defaults to the test node.
		want        PlatformDriverRuntimeInfo
		wantWarning string // substring of the single expected WARNING; empty means none
		wantErr     string
	}{
		{
			name: "dra: driver version comes from ResourceSlice attributes",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithLabels(nodeName, 0, nil),
				withDriverVersion(gpuResourceSlice("slice-1", cfg.DRADriver, nodeName, 1), 0, "580.65.6"),
			},
			want: fromDRA,
		},
		{
			name: "dra: missing driver attribute falls back to node labels and warns",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithLabels(nodeName, 0, gfdLabels),
				gpuResourceSlice("slice-1", cfg.DRADriver, nodeName, 1),
			},
			want:        fromGFD,
			wantWarning: "falling back to node metadata",
		},
		{
			name: "dra: no usable devices on the node is an environment error",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithLabels(nodeName, 0, nil),
				withDriverVersion(gpuResourceSlice("slice-1", cfg.DRADriver, "gpu-node-2", 1), 0, "580.65.6"),
			},
			wantErr: "no usable DRA ResourceSlice devices",
		},
		{
			name: "dra: ResourceSlice list failure is an error",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithLabels(nodeName, 0, gfdLabels),
			},
			listErr: apierrors.NewForbidden(sliceGR, "", errors.New("rbac")),
			wantErr: "failed to list DRA ResourceSlices",
		},
		{
			name: "dra: only the highest pool generation is consulted",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithLabels(nodeName, 0, nil),
				withDriverVersion(pooledResourceSlice("old", cfg.DRADriver, nodeName, "pool", 1, 1, 1), 0, "550.54.14"),
				withDriverVersion(pooledResourceSlice("new", cfg.DRADriver, nodeName, "pool", 2, 1, 1), 0, "580.65.6"),
			},
			want: fromDRA,
		},
		{
			name: "dra: per-device node selection only consults this node's devices",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithLabels(nodeName, 0, nil),
				withDriverVersion(perDeviceResourceSlice("slice-2", cfg.DRADriver, "gpu-node-2"), 0, "550.54.14"),
				withDriverVersion(perDeviceResourceSlice("slice-1", cfg.DRADriver, nodeName), 0, "580.65.6"),
			},
			want: fromDRA,
		},
		{
			name: "device-plugin: driver version comes from node labels",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				gpuNodeWithLabels(nodeName, 1, gfdLabels),
			},
			want: fromGFD,
		},
		{
			name: "device-plugin: node annotations are accepted",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				func() *corev1.Node {
					node := gpuNodeWithLabels(nodeName, 1, nil)
					node.Annotations = gfdLabels
					return node
				}(),
			},
			want: fromGFD,
		},
		{
			name: "device-plugin: DRA attributes are preferred over node labels",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				gpuNodeWithLabels(nodeName, 1, gfdLabels),
				withDriverVersion(gpuResourceSlice("slice-1", cfg.DRADriver, nodeName, 1), 0, "580.65.6"),
			},
			want: fromDRA,
		},
		{
			name: "device-plugin: no version metadata warns",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				gpuNodeWithLabels(nodeName, 1, map[string]string{"nvidia.com/gpu.present": "true"}),
			},
			want:        PlatformDriverRuntimeInfo{Mechanism: platformMechanismNodeMetadata},
			wantWarning: "advertises no accelerator driver version",
		},
		{
			name: "device-plugin: DRA API not served falls back quietly",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				gpuNodeWithLabels(nodeName, 1, gfdLabels),
			},
			listErr: apierrors.NewNotFound(sliceGR, ""),
			want:    fromGFD,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset(tc.objects...)
			if tc.listErr != nil {
				client.PrependReactor("list", "resourceslices", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, tc.listErr
				})
			}
			logs := &logCapture{}

			got, err := inspectPlatformDriverRuntimeMechanism(ctx, client, nodeName, tc.mode, cfg, logs.logf)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			want := tc.want
			want.NodeName = nodeName
			if got != want {
				t.Errorf("info mismatch\n got: %+v\nwant: %+v", got, want)
			}

			warnings := logs.warnings()
			switch {
			case tc.wantWarning == "" && len(warnings) > 0:
				t.Errorf("unexpected WARNING(s): %q", warnings)
			case tc.wantWarning != "" && (len(warnings) != 1 || !strings.Contains(warnings[0], tc.wantWarning)):
				t.Errorf("WARNINGs = %q, want exactly one containing %q", warnings, tc.wantWarning)
			}
		})
	}
}

func TestVersionsCompatible(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"580.65.06", "580.65.06", true},
		{"580.65.6", "580.65.06", true}, // DRA SemVer normalization vs nvidia-smi
		{"580.65", "580.65.06", true},   // a shorter version is a less precise statement of the same version
		{"580", "580.65.06", true},
		{"v580.65.06", "580.65.06", true},
		{"580.65.06-rc1", "580.65.06", true},
		{"550.54.14", "580.65.06", false},
		{"580.65.07", "580.65.06", false},
		{"N/A", "580.65.06", false},
		{"N/A", "n/a", true},
		{"", "", false},
		{"", "580.65.06", false},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%q_vs_%q", tc.a, tc.b), func(t *testing.T) {
			if got := versionsCompatible(tc.a, tc.b); got != tc.want {
				t.Errorf("versionsCompatible(%q, %q) = %t, want %t", tc.a, tc.b, got, tc.want)
			}
			if got := versionsCompatible(tc.b, tc.a); got != tc.want {
				t.Errorf("versionsCompatible(%q, %q) = %t, want %t (must be symmetric)", tc.b, tc.a, got, tc.want)
			}
		})
	}
}

func TestParseDriverRuntimeProbeLogs(t *testing.T) {
	complete := strings.Join([]string{
		"RESULT: ACCELERATOR_COUNT=1",
		"RESULT: RUNTIME_CONFIG_OK=true",
		"RESULT: DRIVER_FUNCTIONAL=true",
		"RESULT: ACTUAL_DRIVER_VERSION=580.65.06",
	}, "\n")

	t.Run("complete output", func(t *testing.T) {
		got, ok := parseDriverRuntimeProbeLogs(complete + "\n")
		if !ok {
			t.Fatal("expected complete results")
		}
		want := DriverRuntimeProbeResults{
			AcceleratorCount: 1, HasAcceleratorCount: true, RuntimeConfigOK: true, DriverFunctional: true,
			ActualDriverVersion: "580.65.06", RawLogs: got.RawLogs,
		}
		if got != want {
			t.Errorf("results mismatch\n got: %+v\nwant: %+v", got, want)
		}
	})

	t.Run("partial output is incomplete", func(t *testing.T) {
		partial := strings.TrimSuffix(complete, "\nRESULT: ACTUAL_DRIVER_VERSION=580.65.06")
		if got, ok := parseDriverRuntimeProbeLogs(partial); ok {
			t.Errorf("expected incomplete results, got complete: %+v", got)
		}
	})

	t.Run("empty output is incomplete", func(t *testing.T) {
		if got, ok := parseDriverRuntimeProbeLogs(""); ok || got.HasAcceleratorCount {
			t.Errorf("expected incomplete results without a count, got ok=%t %+v", ok, got)
		}
	})
}

func TestVerifyDriverRuntimeCompatibility(t *testing.T) {
	healthy := DriverRuntimeProbeResults{
		AcceleratorCount: 1, HasAcceleratorCount: true, RuntimeConfigOK: true, DriverFunctional: true,
		ActualDriverVersion: "580.65.06",
	}
	advertised := PlatformDriverRuntimeInfo{Mechanism: platformMechanismDRA, DriverVersion: "580.65.6", DriverVersionKey: "driverVersion"}
	modify := func(base DriverRuntimeProbeResults, f func(*DriverRuntimeProbeResults)) DriverRuntimeProbeResults {
		f(&base)
		return base
	}

	tests := []struct {
		name     string
		platform PlatformDriverRuntimeInfo
		probe    DriverRuntimeProbeResults
		wantErr  string
	}{
		{name: "healthy stack matches advertised version", platform: advertised, probe: healthy},
		{name: "no advertised version skips the cross-check", platform: PlatformDriverRuntimeInfo{Mechanism: platformMechanismNodeMetadata}, probe: healthy},
		{name: "wrong device count", platform: advertised, probe: modify(healthy, func(p *DriverRuntimeProbeResults) { p.AcceleratorCount = 2 }), wantErr: "expected 1"},
		{name: "missing device count", platform: advertised, probe: modify(healthy, func(p *DriverRuntimeProbeResults) { p.HasAcceleratorCount = false }), wantErr: "expected 1"},
		{name: "tools not injected", platform: advertised, probe: modify(healthy, func(p *DriverRuntimeProbeResults) { p.RuntimeConfigOK = false }), wantErr: "failed to inject"},
		{name: "driver not functional", platform: advertised, probe: modify(healthy, func(p *DriverRuntimeProbeResults) { p.DriverFunctional = false }), wantErr: "failed to initialize"},
		{name: "driver version mismatch", platform: PlatformDriverRuntimeInfo{Mechanism: platformMechanismDRA, DriverVersion: "535.104.05"}, probe: healthy, wantErr: "driver version \"535.104.05\""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := verifyDriverRuntimeCompatibility(tc.platform, tc.probe, 1)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestNVIDIADriverProbeScript runs the NVIDIA DriverProbeScript under /bin/sh
// against synthetic device nodes and a mock nvidia-smi.
func TestNVIDIADriverProbeScript(t *testing.T) {
	cfg := nvidiaConfig(t)
	devDir := t.TempDir()
	for _, dev := range []string{"nvidia0", "nvidiactl"} {
		if err := os.WriteFile(filepath.Join(devDir, dev), nil, 0o600); err != nil {
			t.Fatalf("create synthetic device %s: %v", dev, err)
		}
	}
	devicePattern := filepath.Join(devDir, "nvidia[0-9]*")

	const healthySMI = `#!/bin/sh
echo "580.65.06"
`
	const mismatchSMI = `#!/bin/sh
echo "Failed to initialize NVML: Driver/library version mismatch"
exit 255
`

	// runProbe executes the composed probe with mockSMI installed as
	// nvidia-smi at the front of PATH (or with PATH untouched when empty).
	runProbe := func(t *testing.T, mockSMI, pattern string) DriverRuntimeProbeResults {
		t.Helper()
		path := os.Getenv("PATH")
		if mockSMI != "" {
			binDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(binDir, "nvidia-smi"), []byte(mockSMI), 0o755); err != nil {
				t.Fatalf("write mock nvidia-smi: %v", err)
			}
			path = binDir + ":" + path
		}
		cmd := exec.Command("/bin/sh", "-c", driverRuntimeProbeCommand(pattern, cfg.DriverProbeScript))
		cmd.Env = append(os.Environ(), "PATH="+path)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("probe script failed: %v\n%s", err, out)
		}
		res, complete := parseDriverRuntimeProbeLogs(string(out))
		if !complete {
			t.Fatalf("probe output is incomplete:\n%s", out)
		}
		return res
	}

	t.Run("healthy driver stack", func(t *testing.T) {
		res := runProbe(t, healthySMI, devicePattern)
		want := DriverRuntimeProbeResults{
			AcceleratorCount: 1, HasAcceleratorCount: true, RuntimeConfigOK: true, DriverFunctional: true,
			ActualDriverVersion: "580.65.06", RawLogs: res.RawLogs,
		}
		if res != want {
			t.Fatalf("results mismatch\n got: %+v\nwant: %+v", res, want)
		}
		advertised := PlatformDriverRuntimeInfo{Mechanism: platformMechanismDRA, DriverVersion: "580.65.6"}
		if err := verifyDriverRuntimeCompatibility(advertised, res, 1); err != nil {
			t.Errorf("verification unexpectedly failed: %v", err)
		}
	})

	t.Run("NVML failure is not a functional driver", func(t *testing.T) {
		res := runProbe(t, mismatchSMI, devicePattern)
		if !res.RuntimeConfigOK || res.DriverFunctional || res.ActualDriverVersion != "" {
			t.Fatalf("got %+v, want RuntimeConfigOK=true DriverFunctional=false ActualDriverVersion=\"\"", res)
		}
	})

	t.Run("no accelerator devices", func(t *testing.T) {
		res := runProbe(t, healthySMI, filepath.Join(devDir, "missing[0-9]*"))
		if res.AcceleratorCount != 0 || res.RuntimeConfigOK || res.DriverFunctional {
			t.Fatalf("got %+v, want count 0 and both checks false", res)
		}
		if err := verifyDriverRuntimeCompatibility(PlatformDriverRuntimeInfo{}, res, 1); err == nil || !strings.Contains(err.Error(), "expected 1") {
			t.Errorf("verification error = %v, want device count mismatch", err)
		}
	})

	t.Run("nvidia-smi not injected", func(t *testing.T) {
		for _, p := range []string{"/usr/bin/nvidia-smi", "/usr/local/nvidia/bin/nvidia-smi"} {
			if _, err := os.Stat(p); err == nil {
				t.Skipf("host has %s; cannot simulate a missing nvidia-smi", p)
			}
		}
		if _, err := exec.LookPath("nvidia-smi"); err == nil {
			t.Skip("host has nvidia-smi on PATH; cannot simulate a missing nvidia-smi")
		}
		res := runProbe(t, "", devicePattern)
		if res.AcceleratorCount != 1 || res.RuntimeConfigOK || res.DriverFunctional {
			t.Fatalf("got %+v, want count 1 and both checks false", res)
		}
	})
}
