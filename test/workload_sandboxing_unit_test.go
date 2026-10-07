package conformance

import (
	"context"
	"errors"
	"strings"
	"testing"

	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes/fake"
)

// Unit tests for the workload sandboxing helpers (fake clientset, no cluster).

func TestParseProbeLogs(t *testing.T) {
	tests := []struct {
		name string
		logs string
		want sandboxProbeResults
	}{
		{
			name: "full pass",
			logs: `SANDBOX_PROBE: SCHEDULING=PASS
SANDBOX_INFO: KERNEL_RELEASE=4.4.0
SANDBOX_INFO: KERNEL_VERSION=Linux version 4.4.0 #1 SMP Sun Jan 10 15:06:54 PST 2016
SANDBOX_INFO: BOOT_ID=3f0c6a36-1c1e-4b7e-9a2b-8f4b0a4b2c11
SANDBOX_INFO: PID_COUNT=4
SANDBOX_PROBE: PID_ISOLATION=PASS
SANDBOX_PROBE: KERNEL_ISOLATION=PASS
SANDBOX_PROBE: FS_ISOLATION=PASS
SANDBOX_INFO: INTERFACES= eth0 lo
SANDBOX_PROBE: NET_ISOLATION=PASS
SANDBOX_PROBE: COMPLETED
`,
			want: sandboxProbeResults{
				SchedulingPassed:      true,
				PidIsolationPassed:    true,
				KernelIsolationPassed: true,
				FsIsolationPassed:     true,
				NetIsolationPassed:    true,
				ProbeCompleted:        true,
				Kernel: kernelIdentity{
					Release: "4.4.0",
					Version: "Linux version 4.4.0 #1 SMP Sun Jan 10 15:06:54 PST 2016",
					BootID:  "3f0c6a36-1c1e-4b7e-9a2b-8f4b0a4b2c11",
				},
				Interfaces: "eth0 lo",
				PidCount:   4,
			},
		},
		{
			name: "pid isolation failure and unreadable boot_id",
			logs: `SANDBOX_PROBE: SCHEDULING=PASS
SANDBOX_INFO: KERNEL_RELEASE=6.6.0
SANDBOX_INFO: BOOT_ID=unknown
SANDBOX_INFO: PID_COUNT=120
SANDBOX_PROBE: PID_ISOLATION=FAIL
SANDBOX_PROBE: KERNEL_ISOLATION=PASS
SANDBOX_PROBE: FS_ISOLATION=PASS
SANDBOX_PROBE: NET_ISOLATION=PASS
SANDBOX_PROBE: COMPLETED
`,
			want: sandboxProbeResults{
				SchedulingPassed:      true,
				KernelIsolationPassed: true,
				FsIsolationPassed:     true,
				NetIsolationPassed:    true,
				ProbeCompleted:        true,
				Kernel:                kernelIdentity{Release: "6.6.0"},
				PidCount:              120,
			},
		},
		{
			name: "network, kernel and fs failure",
			logs: `SANDBOX_PROBE: SCHEDULING=PASS
SANDBOX_PROBE: PID_ISOLATION=PASS
SANDBOX_PROBE: KERNEL_ISOLATION=FAIL
SANDBOX_PROBE: FS_ISOLATION=FAIL
SANDBOX_INFO: INTERFACES=docker0 eth0 lo
SANDBOX_PROBE: NET_ISOLATION=FAIL
SANDBOX_PROBE: COMPLETED
`,
			want: sandboxProbeResults{
				SchedulingPassed:   true,
				PidIsolationPassed: true,
				ProbeCompleted:     true,
				Interfaces:         "docker0 eth0 lo",
			},
		},
		{
			name: "probe died before completing",
			logs: `SANDBOX_PROBE: SCHEDULING=PASS
SANDBOX_INFO: KERNEL_RELEASE=6.6.0
`,
			want: sandboxProbeResults{
				SchedulingPassed: true,
				Kernel:           kernelIdentity{Release: "6.6.0"},
			},
		},
		{
			name: "empty logs",
			logs: "",
			want: sandboxProbeResults{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := *parseProbeLogs(tc.logs)
			got.RawLogs = ""
			if got != tc.want {
				t.Errorf("parseProbeLogs() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestKernelIdentitiesDiffer(t *testing.T) {
	host := kernelIdentity{
		Release: "6.6.87+",
		Version: "Linux version 6.6.87+ (builder@host) #1 SMP",
		BootID:  "aaaaaaaa-0000-0000-0000-000000000000",
	}
	tests := []struct {
		name       string
		sandboxed  kernelIdentity
		wantDiffer bool
		wantFields []string
	}{
		{
			name:       "gVisor reports its own kernel release, version and boot_id",
			sandboxed:  kernelIdentity{Release: "4.4.0", Version: "Linux version 4.4.0 #1 SMP Sun Jan 10 15:06:54 PST 2016", BootID: "bbbbbbbb-0000-0000-0000-000000000000"},
			wantDiffer: true,
			wantFields: []string{"release", "version", "boot_id"},
		},
		{
			name:       "Kata guest with the same kernel release still has a different boot_id",
			sandboxed:  kernelIdentity{Release: host.Release, Version: host.Version, BootID: "cccccccc-0000-0000-0000-000000000000"},
			wantDiffer: true,
			wantFields: []string{"boot_id"},
		},
		{
			name:       "plain runc container shares the host kernel identity",
			sandboxed:  host,
			wantDiffer: false,
		},
		{
			name:       "unreadable fields are ignored rather than counted as different",
			sandboxed:  kernelIdentity{Release: host.Release, Version: "", BootID: ""},
			wantDiffer: false,
		},
		{
			name:       "release alone is enough when nothing else is readable",
			sandboxed:  kernelIdentity{Release: "4.4.0"},
			wantDiffer: true,
			wantFields: []string{"release"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			differ, fields := kernelIdentitiesDiffer(tc.sandboxed, host)
			if differ != tc.wantDiffer {
				t.Fatalf("kernelIdentitiesDiffer() = %v, want %v (fields %v)", differ, tc.wantDiffer, fields)
			}
			if strings.Join(fields, ",") != strings.Join(tc.wantFields, ",") {
				t.Errorf("differing fields = %v, want %v", fields, tc.wantFields)
			}
		})
	}
}

func TestIsKnownSandboxedRuntime(t *testing.T) {
	tests := []struct {
		name    string
		rc      *nodev1.RuntimeClass
		want    bool
		rcName  string
		handler string
	}{
		{name: "gvisor by name", rcName: "gvisor", handler: "runsc", want: true},
		{name: "runsc by handler", rcName: "sandboxed-runtime", handler: "runsc", want: true},
		{name: "kata by name", rcName: "kata", handler: "kata", want: true},
		{name: "kata-qemu by name", rcName: "kata-qemu", handler: "kata-qemu", want: true},
		{name: "kata-clh by name", rcName: "kata-clh", handler: "kata-clh", want: true},
		{name: "AKS pod sandboxing", rcName: "kata-vm-isolation", handler: "kata-vm-isolation", want: true},
		{name: "sandboxed containers", rcName: "sandboxed-containers", handler: "kata", want: true},
		{name: "quark runtime", rcName: "quark", handler: "quark", want: true},
		{name: "krun runtime", rcName: "krun", handler: "krun", want: true},
		{name: "crun-krun handler", rcName: "libkrun", handler: "crun-krun", want: true},
		{name: "standard runc", rcName: "runc", handler: "runc", want: false},
		{name: "crun runtime", rcName: "crun", handler: "crun", want: false},
		{name: "default runtime", rcName: "default", handler: "", want: false},
		{name: "nil runtimeclass", rc: nil, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rc := tc.rc
			if tc.rcName != "" {
				rc = &nodev1.RuntimeClass{
					ObjectMeta: metav1.ObjectMeta{Name: tc.rcName},
					Handler:    tc.handler,
				}
			}
			if got := isKnownSandboxedRuntime(rc); got != tc.want {
				t.Errorf("isKnownSandboxedRuntime() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildProbePod(t *testing.T) {
	t.Run("sandboxed pod", func(t *testing.T) {
		pod := buildProbePod("test-ns", "test-pod", "gvisor", "", "busybox")

		if pod.Name != "test-pod" || pod.Namespace != "test-ns" {
			t.Errorf("Unexpected metadata: %s/%s", pod.Namespace, pod.Name)
		}
		if pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "gvisor" {
			t.Errorf("Unexpected RuntimeClassName: %v", pod.Spec.RuntimeClassName)
		}
		if pod.Spec.NodeName != "" {
			t.Errorf("Sandboxed pod must be scheduled, not pinned; got nodeName %q", pod.Spec.NodeName)
		}
		if len(pod.Spec.Containers) != 1 {
			t.Fatalf("Expected 1 container, got %d", len(pod.Spec.Containers))
		}
		c := pod.Spec.Containers[0]
		if c.Name != sandboxProbeContainer || c.Image != "busybox" {
			t.Errorf("Unexpected container attributes: %s, %s", c.Name, c.Image)
		}
		if len(c.Args) == 0 || !strings.Contains(c.Args[0], sandboxProbeCompletedMarker) {
			t.Errorf("Container missing probe command: %v", c.Args)
		}
		if c.Resources.Requests.Cpu().IsZero() || c.Resources.Requests.Memory().IsZero() {
			t.Errorf("Container missing resource requests: %v", c.Resources)
		}
	})

	t.Run("unsandboxed control pod pinned to a node", func(t *testing.T) {
		pod := buildProbePod("test-ns", "control", "", "node-a", "busybox")
		if pod.Spec.RuntimeClassName != nil {
			t.Errorf("Control pod must not set a RuntimeClass; got %q", *pod.Spec.RuntimeClassName)
		}
		if pod.Spec.NodeName != "node-a" {
			t.Errorf("Control pod nodeName = %q, want node-a", pod.Spec.NodeName)
		}
	})
}

func TestBuildAgentSandboxCR(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: agentSandboxAPIGroup, Version: "v1beta1", Resource: "sandboxes"}
	cr := buildAgentSandboxCR("test-ns", "test-cr", gvr, "kata", "busybox")

	if cr.GetAPIVersion() != "agents.x-k8s.io/v1beta1" {
		t.Errorf("Unexpected APIVersion: %s", cr.GetAPIVersion())
	}
	if cr.GetKind() != "Sandbox" {
		t.Errorf("Unexpected Kind: %s", cr.GetKind())
	}
	if cr.GetName() != "test-cr" || cr.GetNamespace() != "test-ns" {
		t.Errorf("Unexpected metadata: %s/%s", cr.GetNamespace(), cr.GetName())
	}
	spec, _ := cr.Object["spec"].(map[string]interface{})
	tmpl, _ := spec["podTemplate"].(map[string]interface{})
	podSpec, _ := tmpl["spec"].(map[string]interface{})
	if podSpec["runtimeClassName"] != "kata" {
		t.Errorf("podTemplate.spec.runtimeClassName = %v, want kata", podSpec["runtimeClassName"])
	}
	if _, ok := tmpl["metadata"]; !ok {
		t.Errorf("podTemplate.metadata missing; the Sandbox CRD requires it")
	}

	noClass := buildAgentSandboxCR("test-ns", "test-cr", gvr, "", "busybox")
	spec, _ = noClass.Object["spec"].(map[string]interface{})
	tmpl, _ = spec["podTemplate"].(map[string]interface{})
	podSpec, _ = tmpl["spec"].(map[string]interface{})
	if _, ok := podSpec["runtimeClassName"]; ok {
		t.Errorf("podTemplate.spec.runtimeClassName must be omitted when no class is requested")
	}
}

func TestDiscoverSandboxingSolution(t *testing.T) {
	ctx := context.Background()

	withAgentSandboxAPI := func(client *fake.Clientset, versions ...string) {
		disc := client.Discovery().(*fakediscovery.FakeDiscovery)
		for _, v := range versions {
			disc.Resources = append(disc.Resources, &metav1.APIResourceList{
				GroupVersion: agentSandboxAPIGroup + "/" + v,
				APIResources: []metav1.APIResource{{Name: "sandboxes", Kind: "Sandbox", Namespaced: true}},
			})
		}
	}

	t.Run("auto discovers known RuntimeClass", func(t *testing.T) {
		client := fake.NewClientset(
			&nodev1.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: "kata"},
				Handler:    "kata-qemu",
			},
		)

		sol, err := discoverSandboxingSolution(ctx, client, sandboxTypeAuto, "", t.Logf)
		if err != nil {
			t.Fatalf("discoverSandboxingSolution failed: %v", err)
		}
		if sol.SolutionType != sandboxTypeRuntimeClass || sol.RuntimeClassName != "kata" || sol.Handler != "kata-qemu" {
			t.Errorf("Unexpected solution: %+v", sol)
		}
	})

	t.Run("explicit RuntimeClass succeeds when present", func(t *testing.T) {
		client := fake.NewClientset(
			&nodev1.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: "custom-sandbox"},
				Handler:    "custom-handler",
			},
		)

		sol, err := discoverSandboxingSolution(ctx, client, sandboxTypeRuntimeClass, "custom-sandbox", t.Logf)
		if err != nil {
			t.Fatalf("discoverSandboxingSolution failed: %v", err)
		}
		if sol.RuntimeClassName != "custom-sandbox" {
			t.Errorf("Unexpected RuntimeClassName: %s", sol.RuntimeClassName)
		}
	})

	t.Run("explicit RuntimeClass fails when missing", func(t *testing.T) {
		client := fake.NewClientset()

		_, err := discoverSandboxingSolution(ctx, client, sandboxTypeRuntimeClass, "missing-class", t.Logf)
		if err == nil {
			t.Fatal("Expected error for missing RuntimeClass, got nil")
		}
	})

	t.Run("auto returns errNoSandboxingSolution when none detected", func(t *testing.T) {
		client := fake.NewClientset(
			&nodev1.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{Name: "runc"},
				Handler:    "runc",
			},
		)

		_, err := discoverSandboxingSolution(ctx, client, sandboxTypeAuto, "", t.Logf)
		if !errors.Is(err, errNoSandboxingSolution) {
			t.Fatalf("Expected errNoSandboxingSolution, got %v", err)
		}
	})

	t.Run("auto falls back to agent-sandbox API group", func(t *testing.T) {
		client := fake.NewClientset()
		withAgentSandboxAPI(client, "v1beta1")

		sol, err := discoverSandboxingSolution(ctx, client, sandboxTypeAuto, "", t.Logf)
		if err != nil {
			t.Fatalf("discoverSandboxingSolution failed: %v", err)
		}
		if sol.SolutionType != sandboxTypeAgentSandbox || sol.AgentSandboxGVR == nil || sol.AgentSandboxGVR.Version != "v1beta1" || sol.AgentSandboxGVR.Resource != "sandboxes" {
			t.Errorf("Unexpected solution: %+v", sol)
		}
	})

	t.Run("agent-sandbox uses the preferred version", func(t *testing.T) {
		client := fake.NewClientset()
		withAgentSandboxAPI(client, "v1beta1", "v1alpha1")

		sol, err := discoverSandboxingSolution(ctx, client, sandboxTypeAgentSandbox, "", t.Logf)
		if err != nil {
			t.Fatalf("discoverSandboxingSolution failed: %v", err)
		}
		if sol.AgentSandboxGVR.Version != "v1beta1" {
			t.Errorf("Version = %q, want the preferred version v1beta1", sol.AgentSandboxGVR.Version)
		}
	})

	t.Run("agent-sandbox with an explicit RuntimeClass records its handler", func(t *testing.T) {
		client := fake.NewClientset(&nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "gvisor"}, Handler: "runsc"})
		withAgentSandboxAPI(client, "v1beta1")

		sol, err := discoverSandboxingSolution(ctx, client, sandboxTypeAgentSandbox, "gvisor", t.Logf)
		if err != nil {
			t.Fatalf("discoverSandboxingSolution failed: %v", err)
		}
		if sol.RuntimeClassName != "gvisor" || sol.Handler != "runsc" {
			t.Errorf("Unexpected solution: %+v", sol)
		}
	})

	t.Run("agent-sandbox with a missing RuntimeClass fails", func(t *testing.T) {
		client := fake.NewClientset()
		withAgentSandboxAPI(client, "v1beta1")

		if _, err := discoverSandboxingSolution(ctx, client, sandboxTypeAgentSandbox, "missing", t.Logf); err == nil {
			t.Fatal("Expected error for missing RuntimeClass, got nil")
		}
	})

	t.Run("explicit agent-sandbox fails when the API group is absent", func(t *testing.T) {
		client := fake.NewClientset()

		if _, err := discoverSandboxingSolution(ctx, client, sandboxTypeAgentSandbox, "", t.Logf); err == nil {
			t.Fatal("Expected error when agents.x-k8s.io is not served, got nil")
		}
	})

	t.Run("invalid type is rejected", func(t *testing.T) {
		client := fake.NewClientset()

		if _, err := discoverSandboxingSolution(ctx, client, "firecracker", "", t.Logf); err == nil {
			t.Fatal("Expected error for invalid -sandbox-type, got nil")
		}
	})
}

func TestSandboxProbeScriptStructure(t *testing.T) {
	script := sandboxProbeScript()

	requiredMarkers := []string{
		"SANDBOX_PROBE: SCHEDULING=PASS",
		"SANDBOX_INFO: KERNEL_RELEASE=",
		"SANDBOX_INFO: KERNEL_VERSION=",
		"SANDBOX_INFO: BOOT_ID=",
		"SANDBOX_PROBE: PID_ISOLATION=PASS",
		"SANDBOX_PROBE: KERNEL_ISOLATION=PASS",
		"SANDBOX_PROBE: FS_ISOLATION=PASS",
		"SANDBOX_PROBE: NET_ISOLATION=PASS",
		sandboxProbeCompletedMarker,
		"/proc/[0-9]*/comm",
		"/proc/sys/kernel/random/boot_id",
		"/dev/mem",
		"/dev/kmem",
		"/etc/kubernetes",
		"/proc/net/dev",
	}

	for _, marker := range requiredMarkers {
		if !strings.Contains(script, marker) {
			t.Errorf("Probe script missing required check marker: %q", marker)
		}
	}

	// The script is PID 1's cmdline inside the container, so the PID check
	// must never grep cmdline for the daemon names or it matches itself.
	if strings.Contains(script, "/proc/[0-9]*/cmdline") {
		t.Errorf("Probe script inspects /proc/*/cmdline; the daemon-name pattern would match the probe's own command line")
	}
}
