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
	nodev1 "k8s.io/api/node/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// Unit tests for the KAR-0001 mechanism resolution, version comparison, probe
// log parsing, and the NVIDIA probe script. They use a fake clientset and a
// mock nvidia-smi and need no cluster.

func strPtr(s string) *string { return &s }

func int64Ptr(i int64) *int64 { return &i }

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

func gpuNodeWithRuntime(name string, gpus int64, containerRuntime string, labels map[string]string) *corev1.Node {
	node := gpuNode(name, gpus, true, false)
	node.Status.NodeInfo.ContainerRuntimeVersion = containerRuntime
	node.Labels = labels
	return node
}

func versionAttrs(driverVer, runtimeVer string) map[resourcev1.QualifiedName]resourcev1.DeviceAttribute {
	attrs := map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{}
	if driverVer != "" {
		attrs["driverVersion"] = resourcev1.DeviceAttribute{VersionValue: strPtr(driverVer)}
	}
	if runtimeVer != "" {
		attrs["cudaDriverVersion"] = resourcev1.DeviceAttribute{VersionValue: strPtr(runtimeVer)}
	}
	return attrs
}

func withVersions(slice *resourcev1.ResourceSlice, deviceIndex int, driverVer, runtimeVer string) *resourcev1.ResourceSlice {
	slice.Spec.Devices[deviceIndex].Attributes = versionAttrs(driverVer, runtimeVer)
	return slice
}

func TestInspectPlatformDriverRuntimeMechanism(t *testing.T) {
	ctx := context.Background()
	cfg := nvidiaConfig(t)
	const nodeName = "gpu-node-1"
	const containerd = "containerd://2.2.0"
	gfdLabels := map[string]string{
		"nvidia.com/cuda.driver-version.full":  "580.65.06",
		"nvidia.com/cuda.runtime-version.full": "12.9",
		"nvidia.com/gpu.present":               "true",
	}
	nvidiaRuntimeClass := &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "nvidia"}, Handler: "nvidia"}
	sliceGR := schema.GroupResource{Group: "resource.k8s.io", Resource: "resourceslices"}

	tests := []struct {
		name    string
		mode    string
		objects []runtime.Object
		listErr error // returned by every ResourceSlice list when set
		// want is compared field by field; NodeName and
		// ContainerRuntimeVersion default to the test node's values.
		want        PlatformDriverRuntimeInfo
		wantWarning string // substring of the single expected WARNING; empty means none
		wantErr     string
	}{
		{
			name: "dra: driver and runtime versions come from ResourceSlice attributes",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 0, containerd, nil),
				withVersions(gpuResourceSlice("slice-1", cfg.DRADriver, nodeName, 1), 0, "580.65.6", "12.9.0"),
				nvidiaRuntimeClass,
			},
			want: PlatformDriverRuntimeInfo{
				Mechanism: platformMechanismDRA, DriverVersion: "580.65.6", DriverVersionKey: "driverVersion",
				RuntimeVersion: "12.9.0", RuntimeVersionKey: "cudaDriverVersion", RuntimeClassName: "nvidia",
			},
		},
		{
			name: "dra: missing runtime attribute keeps the DRA mechanism and warns",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 0, containerd, nil),
				withVersions(gpuResourceSlice("slice-1", cfg.DRADriver, nodeName, 1), 0, "580.65.6", ""),
			},
			want:        PlatformDriverRuntimeInfo{Mechanism: platformMechanismDRA, DriverVersion: "580.65.6", DriverVersionKey: "driverVersion"},
			wantWarning: "no runtime version attribute",
		},
		{
			name: "dra: missing driver attribute falls back to node labels and warns",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 0, containerd, gfdLabels),
				withVersions(gpuResourceSlice("slice-1", cfg.DRADriver, nodeName, 1), 0, "", "12.9.0"),
			},
			want: PlatformDriverRuntimeInfo{
				Mechanism: platformMechanismNodeMetadata, DriverVersion: "580.65.06", DriverVersionKey: "nvidia.com/cuda.driver-version.full",
				RuntimeVersion: "12.9", RuntimeVersionKey: "nvidia.com/cuda.runtime-version.full", PresenceKey: "nvidia.com/gpu.present",
			},
			wantWarning: "falling back to node metadata",
		},
		{
			name: "dra: conflicting driver versions across devices on one node is an error",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 0, containerd, nil),
				withVersions(withVersions(gpuResourceSlice("slice-1", cfg.DRADriver, nodeName, 2), 0, "580.65.6", "12.9.0"), 1, "550.54.14", "12.9.0"),
			},
			wantErr: "inconsistent DRA driver versions",
		},
		{
			name: "dra: conflicting runtime versions across devices on one node is an error",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 0, containerd, nil),
				withVersions(withVersions(gpuResourceSlice("slice-1", cfg.DRADriver, nodeName, 2), 0, "580.65.6", "12.9.0"), 1, "580.65.6", "12.4.0"),
			},
			wantErr: "inconsistent DRA runtime versions",
		},
		{
			name: "dra: no usable devices on the node is an environment error",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 0, containerd, nil),
				withVersions(gpuResourceSlice("slice-1", cfg.DRADriver, "gpu-node-2", 1), 0, "580.65.6", "12.9.0"),
			},
			wantErr: "no usable DRA ResourceSlice devices",
		},
		{
			name: "dra: ResourceSlice list failure is an error",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 0, containerd, gfdLabels),
			},
			listErr: apierrors.NewForbidden(sliceGR, "", errors.New("rbac")),
			wantErr: "failed to list DRA ResourceSlices",
		},
		{
			name: "dra: only the highest pool generation is consulted",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 0, containerd, nil),
				withVersions(pooledResourceSlice("old", cfg.DRADriver, nodeName, "pool", 1, 1, 1), 0, "550.54.14", "12.4.0"),
				withVersions(pooledResourceSlice("new", cfg.DRADriver, nodeName, "pool", 2, 1, 1), 0, "580.65.6", "12.9.0"),
			},
			want: PlatformDriverRuntimeInfo{
				Mechanism: platformMechanismDRA, DriverVersion: "580.65.6", DriverVersionKey: "driverVersion",
				RuntimeVersion: "12.9.0", RuntimeVersionKey: "cudaDriverVersion",
			},
		},
		{
			name: "dra: per-device node selection only consults this node's devices",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 0, containerd, nil),
				withVersions(perDeviceResourceSlice("slice-1", cfg.DRADriver, nodeName), 0, "580.65.6", "12.9.0"),
				withVersions(perDeviceResourceSlice("slice-2", cfg.DRADriver, "gpu-node-2"), 0, "550.54.14", "12.4.0"),
			},
			want: PlatformDriverRuntimeInfo{
				Mechanism: platformMechanismDRA, DriverVersion: "580.65.6", DriverVersionKey: "driverVersion",
				RuntimeVersion: "12.9.0", RuntimeVersionKey: "cudaDriverVersion",
			},
		},
		{
			name: "dra: qualified attribute keys and string/int values are accepted",
			mode: allocationModeDRA,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 0, containerd, nil),
				func() *resourcev1.ResourceSlice {
					slice := gpuResourceSlice("slice-1", cfg.DRADriver, nodeName, 1)
					slice.Spec.Devices[0].Attributes = map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
						"gpu.nvidia.com/driverVersion": {StringValue: strPtr("580.65.06")},
						"cudaDriverVersion":            {IntValue: int64Ptr(13)},
					}
					return slice
				}(),
			},
			want: PlatformDriverRuntimeInfo{
				Mechanism: platformMechanismDRA, DriverVersion: "580.65.06", DriverVersionKey: "gpu.nvidia.com/driverVersion",
				RuntimeVersion: "13", RuntimeVersionKey: "cudaDriverVersion",
			},
		},
		{
			name: "device-plugin: versions come from node labels",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 1, containerd, gfdLabels),
			},
			want: PlatformDriverRuntimeInfo{
				Mechanism: platformMechanismNodeMetadata, DriverVersion: "580.65.06", DriverVersionKey: "nvidia.com/cuda.driver-version.full",
				RuntimeVersion: "12.9", RuntimeVersionKey: "nvidia.com/cuda.runtime-version.full", PresenceKey: "nvidia.com/gpu.present",
			},
		},
		{
			name: "device-plugin: node annotations are accepted",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				func() *corev1.Node {
					node := gpuNodeWithRuntime(nodeName, 1, containerd, nil)
					node.Annotations = gfdLabels
					return node
				}(),
			},
			want: PlatformDriverRuntimeInfo{
				Mechanism: platformMechanismNodeMetadata, DriverVersion: "580.65.06", DriverVersionKey: "nvidia.com/cuda.driver-version.full",
				RuntimeVersion: "12.9", RuntimeVersionKey: "nvidia.com/cuda.runtime-version.full", PresenceKey: "nvidia.com/gpu.present",
			},
		},
		{
			name: "device-plugin: DRA attributes are preferred over node labels",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 1, containerd, gfdLabels),
				withVersions(gpuResourceSlice("slice-1", cfg.DRADriver, nodeName, 1), 0, "580.65.6", "12.9.0"),
			},
			want: PlatformDriverRuntimeInfo{
				Mechanism: platformMechanismDRA, DriverVersion: "580.65.6", DriverVersionKey: "driverVersion",
				RuntimeVersion: "12.9.0", RuntimeVersionKey: "cudaDriverVersion",
			},
		},
		{
			name: "device-plugin: no version metadata warns and reports presence only",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 1, containerd, map[string]string{"nvidia.com/gpu.present": "true"}),
			},
			want:        PlatformDriverRuntimeInfo{Mechanism: platformMechanismNodeMetadata, PresenceKey: "nvidia.com/gpu.present"},
			wantWarning: "advertises no accelerator driver version",
		},
		{
			name: "device-plugin: DRA API not served falls back quietly",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 1, containerd, gfdLabels),
			},
			listErr: apierrors.NewNotFound(sliceGR, ""),
			want: PlatformDriverRuntimeInfo{
				Mechanism: platformMechanismNodeMetadata, DriverVersion: "580.65.06", DriverVersionKey: "nvidia.com/cuda.driver-version.full",
				RuntimeVersion: "12.9", RuntimeVersionKey: "nvidia.com/cuda.runtime-version.full", PresenceKey: "nvidia.com/gpu.present",
			},
		},
		{
			name: "fails when the node reports no container runtime",
			mode: allocationModeDevicePlugin,
			objects: []runtime.Object{
				gpuNodeWithRuntime(nodeName, 1, "", gfdLabels),
			},
			wantErr: "containerRuntimeVersion",
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
			want.ContainerRuntimeVersion = containerd
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
		{"12.9.0", "12.9", true},        // DRA cudaDriverVersion vs nvidia-smi CUDA Version
		{"12.9", "12.9.0", true},
		{"580", "580.65.06", true}, // major-only node label
		{"580.65.06", "580", true},
		{"580.65", "580.65.06", true},
		{"13", "13.1", true},
		{"13.1", "13", true},
		{"12.9.1", "12.9", true}, // a shorter version is a less precise statement of the same version
		{"v580.65.06", "580.65.06", true},
		{"580.65.06-rc1", "580.65.06", true},
		{"550.54.14", "580.65.06", false},
		{"12.4.0", "12.9", false},
		{"N/A", "13.0", false},
		{"N/A", "n/a", true},
		{"", "", false},
		{"", "13.0", false},
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
		"RESULT: ACTUAL_RUNTIME_VERSION=12.9",
	}, "\n")

	t.Run("complete output", func(t *testing.T) {
		got, ok := parseDriverRuntimeProbeLogs(complete + "\n")
		if !ok {
			t.Fatal("expected complete results")
		}
		want := DriverRuntimeProbeResults{
			AcceleratorCount: 1, HasAcceleratorCount: true, RuntimeConfigOK: true, DriverFunctional: true,
			ActualDriverVersion: "580.65.06", ActualRuntimeVersion: "12.9", RawLogs: got.RawLogs,
		}
		if got != want {
			t.Errorf("results mismatch\n got: %+v\nwant: %+v", got, want)
		}
	})

	t.Run("partial output is incomplete", func(t *testing.T) {
		partial := strings.TrimSuffix(complete, "\nRESULT: ACTUAL_RUNTIME_VERSION=12.9")
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
		ActualDriverVersion: "580.65.06", ActualRuntimeVersion: "12.9",
	}
	advertised := PlatformDriverRuntimeInfo{
		Mechanism: platformMechanismDRA, DriverVersion: "580.65.6", DriverVersionKey: "driverVersion",
		RuntimeVersion: "12.9.0", RuntimeVersionKey: "cudaDriverVersion",
	}
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
		{name: "healthy stack matches advertised versions", platform: advertised, probe: healthy},
		{name: "no advertised versions skips the cross-check", platform: PlatformDriverRuntimeInfo{Mechanism: platformMechanismNodeMetadata}, probe: healthy},
		{name: "wrong device count", platform: advertised, probe: modify(healthy, func(p *DriverRuntimeProbeResults) { p.AcceleratorCount = 2 }), wantErr: "expected 1"},
		{name: "missing device count", platform: advertised, probe: modify(healthy, func(p *DriverRuntimeProbeResults) { p.HasAcceleratorCount = false }), wantErr: "expected 1"},
		{name: "tools not injected", platform: advertised, probe: modify(healthy, func(p *DriverRuntimeProbeResults) { p.RuntimeConfigOK = false }), wantErr: "failed to inject"},
		{name: "driver not functional", platform: advertised, probe: modify(healthy, func(p *DriverRuntimeProbeResults) { p.DriverFunctional = false }), wantErr: "failed to initialize"},
		{name: "no runtime version", platform: advertised, probe: modify(healthy, func(p *DriverRuntimeProbeResults) { p.ActualRuntimeVersion = "" }), wantErr: "runtime API version"},
		{name: "driver version mismatch", platform: modify2(advertised, func(p *PlatformDriverRuntimeInfo) { p.DriverVersion = "535.104.05" }), probe: healthy, wantErr: "driver version \"535.104.05\""},
		{name: "runtime version mismatch", platform: modify2(advertised, func(p *PlatformDriverRuntimeInfo) { p.RuntimeVersion = "12.4.0" }), probe: healthy, wantErr: "runtime version \"12.4.0\""},
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

func modify2(base PlatformDriverRuntimeInfo, f func(*PlatformDriverRuntimeInfo)) PlatformDriverRuntimeInfo {
	f(&base)
	return base
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
if [ "$1" = "--query-gpu=driver_version" ]; then
  echo "580.65.06"
  exit 0
fi
cat <<'HEADER'
+-----------------------------------------------------------------------------------------+
| NVIDIA-SMI 580.65.06              Driver Version: 580.65.06      CUDA Version: 12.9     |
|-----------------------------------------+------------------------+----------------------+
HEADER
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
			ActualDriverVersion: "580.65.06", ActualRuntimeVersion: "12.9", RawLogs: res.RawLogs,
		}
		if res != want {
			t.Fatalf("results mismatch\n got: %+v\nwant: %+v", res, want)
		}
		advertised := PlatformDriverRuntimeInfo{Mechanism: platformMechanismDRA, DriverVersion: "580.65.6", RuntimeVersion: "12.9.0"}
		if err := verifyDriverRuntimeCompatibility(advertised, res, 1); err != nil {
			t.Errorf("verification unexpectedly failed: %v", err)
		}
	})

	t.Run("NVML failure is not a functional driver", func(t *testing.T) {
		res := runProbe(t, mismatchSMI, devicePattern)
		// ActualDriverVersion is not asserted: on a Linux host with a real
		// driver the /proc/driver/nvidia/version diagnostic fallback fills it.
		if !res.RuntimeConfigOK || res.DriverFunctional || res.ActualRuntimeVersion != "" {
			t.Fatalf("got %+v, want RuntimeConfigOK=true DriverFunctional=false ActualRuntimeVersion=\"\"", res)
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
