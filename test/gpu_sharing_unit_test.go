package conformance

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func nvidiaGPUSharingConfig(t *testing.T) gpuSharingVendorConfig {
	t.Helper()
	vcfg, ok := gpuSharingVendorConfigs["nvidia"]
	if !ok {
		t.Fatal("nvidia GPU sharing config must exist")
	}
	return vcfg
}

// sharedGPUNode returns a Ready node with the given allocatable resources and
// labels.
func sharedGPUNode(name string, allocatable map[string]int64, labels map[string]string) *corev1.Node {
	node := gpuNode(name, 0, true, false)
	node.Labels = labels
	node.Status.Allocatable = corev1.ResourceList{}
	for res, n := range allocatable {
		node.Status.Allocatable[corev1.ResourceName(res)] = *resource.NewQuantity(n, resource.DecimalSI)
	}
	return node
}

func TestParseGPUSharingProbeLogs(t *testing.T) {
	logs := strings.Join([]string{
		"GPU 0: NVIDIA A100-SXM4-40GB (UUID: GPU-aaaa)",
		"  MIG 1g.5gb      Device  0: (UUID: MIG-bbbb)",
		"GPU_SHARING: PHYSICAL_ID=GPU-aaaa",
		"GPU_SHARING: LOGICAL_ID=MIG-bbbb",
		"GPU_SHARING: HEALTH=PASS",
		"GPU_SHARING: COMPLETED",
	}, "\n")
	r := parseGPUSharingProbeLogs(logs)
	if !r.Completed || !r.Healthy {
		t.Errorf("completed=%t healthy=%t, want both true", r.Completed, r.Healthy)
	}
	if id, err := r.PhysicalID(); err != nil || id != "GPU-aaaa" {
		t.Errorf("PhysicalID() = %q, %v; want GPU-aaaa", id, err)
	}
	if got := r.LogicalID(); got != "MIG-bbbb" {
		t.Errorf("LogicalID() = %q, want MIG-bbbb", got)
	}

	t.Run("whole GPU falls back to the physical ID", func(t *testing.T) {
		r := parseGPUSharingProbeLogs("GPU_SHARING: PHYSICAL_ID=GPU-1\nGPU_SHARING: HEALTH=FAIL\nGPU_SHARING: ERROR=boom\n")
		if r.LogicalID() != "GPU-1" || r.Healthy || r.Completed || len(r.Errors) != 1 {
			t.Errorf("unexpected result: %+v", r)
		}
	})

	t.Run("more than one physical GPU is an error", func(t *testing.T) {
		r := parseGPUSharingProbeLogs("GPU_SHARING: PHYSICAL_ID=GPU-1\nGPU_SHARING: PHYSICAL_ID=GPU-2\n")
		if _, err := r.PhysicalID(); err == nil {
			t.Error("expected an error for two visible GPUs")
		}
	})
}

func TestNVIDIAGPUSharingProbeScript(t *testing.T) {
	vcfg := nvidiaGPUSharingConfig(t)

	tests := []struct {
		name         string
		listOutput   string
		listExit     int
		fullExit     int
		wantPhysical []string
		wantLogical  []string
		wantHealthy  bool
	}{
		{
			name:         "time-sliced whole GPU",
			listOutput:   "GPU 0: Tesla T4 (UUID: GPU-5f1c2d3e-0000-1111-2222-333344445555)",
			wantPhysical: []string{"GPU-5f1c2d3e-0000-1111-2222-333344445555"},
			wantHealthy:  true,
		},
		{
			name:         "MIG device",
			listOutput:   "GPU 0: NVIDIA A100-SXM4-40GB (UUID: GPU-aaaa)\n  MIG 1g.5gb      Device  0: (UUID: MIG-bbbb)",
			wantPhysical: []string{"GPU-aaaa"},
			wantLogical:  []string{"MIG-bbbb"},
			wantHealthy:  true,
		},
		{
			name:         "legacy MIG UUID format",
			listOutput:   "GPU 0: A100-SXM4-40GB (UUID: GPU-aaaa)\n  MIG 1g.5gb Device 0: (UUID: MIG-GPU-aaaa/7/0)",
			wantPhysical: []string{"GPU-aaaa"},
			wantLogical:  []string{"MIG-GPU-aaaa/7/0"},
			wantHealthy:  true,
		},
		{
			name:        "nvidia-smi -L fails",
			listOutput:  "Failed to initialize NVML: Unknown Error",
			listExit:    9,
			wantHealthy: false,
		},
		{
			name:         "nvidia-smi health check fails",
			listOutput:   "GPU 0: Tesla T4 (UUID: GPU-1)",
			fullExit:     15,
			wantPhysical: []string{"GPU-1"},
			wantHealthy:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			fake := "#!/bin/sh\nif [ \"$1\" = \"-L\" ]; then\ncat <<'EOF'\n" + tt.listOutput + "\nEOF\nexit " +
				strconv.Itoa(tt.listExit) + "\nfi\nexit " + strconv.Itoa(tt.fullExit) + "\n"
			if err := os.WriteFile(filepath.Join(dir, "nvidia-smi"), []byte(fake), 0o755); err != nil {
				t.Fatalf("write fake nvidia-smi: %v", err)
			}
			cmd := exec.Command("/bin/sh", "-c", vcfg.probeScript)
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"))
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("run probe: %v: %s", err, out)
			}
			r := parseGPUSharingProbeLogs(string(out))
			if !r.Completed {
				t.Fatalf("probe did not complete:\n%s", out)
			}
			if !slices.Equal(r.PhysicalIDs, tt.wantPhysical) {
				t.Errorf("physical IDs = %v, want %v\n%s", r.PhysicalIDs, tt.wantPhysical, out)
			}
			if !slices.Equal(r.LogicalIDs, tt.wantLogical) {
				t.Errorf("logical IDs = %v, want %v\n%s", r.LogicalIDs, tt.wantLogical, out)
			}
			if r.Healthy != tt.wantHealthy {
				t.Errorf("healthy = %t, want %t\n%s", r.Healthy, tt.wantHealthy, out)
			}
		})
	}

	t.Run("missing nvidia-smi fails the health check", func(t *testing.T) {
		for _, d := range nvidiaToolDirs {
			if _, err := os.Stat(filepath.Join(d, "nvidia-smi")); err == nil {
				t.Skipf("host has nvidia-smi in %s", d)
			}
		}
		cmd := exec.Command("/bin/sh", "-c", vcfg.probeScript)
		cmd.Env = append(os.Environ(), "PATH="+t.TempDir())
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("run probe: %v: %s", err, out)
		}
		r := parseGPUSharingProbeLogs(string(out))
		if !r.Completed || r.Healthy || len(r.Errors) == 0 {
			t.Errorf("want a completed, unhealthy result with an error, got %+v", r)
		}
	})
}

func TestResolveGPUSharingPlan(t *testing.T) {
	cfg := nvidiaConfig(t)
	vcfg := nvidiaGPUSharingConfig(t)
	draObjects := func() []runtime.Object {
		return []runtime.Object{
			gpuDeviceClass("gpu.nvidia.com"),
			gpuResourceSlice("node1-gpu", "gpu.nvidia.com", "node1", 1),
			gpuNode("node1", 0, true, false),
		}
	}

	tests := []struct {
		name         string
		mode         string
		resource     string
		pods         int
		allocMode    string
		objects      []runtime.Object
		wantMode     string
		wantNode     string
		wantResource string
		wantKind     string
		wantPods     int
		wantErr      string
		wantSkip     bool
	}{
		{
			name:      "auto prefers a DRA shared claim",
			mode:      gpuSharingModeAuto,
			allocMode: allocationModeAuto,
			objects:   draObjects(),
			wantMode:  gpuSharingModeDRASharedClaim,
			wantNode:  "node1",
			wantPods:  2,
		},
		{
			name:      "auto honors -allocation-mode=device-plugin",
			mode:      gpuSharingModeAuto,
			allocMode: allocationModeDevicePlugin,
			objects: append(draObjects(),
				sharedGPUNode("mig-node", map[string]int64{"nvidia.com/mig-1g.5gb": 7}, map[string]string{"nvidia.com/gpu.count": "1"})),
			wantMode:     gpuSharingModeDevicePlugin,
			wantNode:     "mig-node",
			wantResource: "nvidia.com/mig-1g.5gb",
			wantKind:     gpuSharingKindPartition,
			wantPods:     2,
		},
		{
			name:      "auto detects time-slicing from GPU Feature Discovery labels",
			mode:      gpuSharingModeAuto,
			allocMode: allocationModeAuto,
			objects: []runtime.Object{
				sharedGPUNode("ts-node", map[string]int64{"nvidia.com/gpu": 8}, map[string]string{"nvidia.com/gpu.replicas": "4", "nvidia.com/gpu.count": "2"}),
			},
			wantMode:     gpuSharingModeDevicePlugin,
			wantNode:     "ts-node",
			wantResource: "nvidia.com/gpu",
			wantKind:     gpuSharingKindSoftware,
			wantPods:     3,
		},
		{
			name:      "auto detects the MIG single strategy",
			mode:      gpuSharingModeAuto,
			allocMode: allocationModeAuto,
			objects: []runtime.Object{
				sharedGPUNode("mig-node", map[string]int64{"nvidia.com/gpu": 7}, map[string]string{"nvidia.com/mig.strategy": "single"}),
			},
			wantMode:     gpuSharingModeDevicePlugin,
			wantNode:     "mig-node",
			wantResource: "nvidia.com/gpu",
			wantKind:     gpuSharingKindPartition,
			wantPods:     7, // physicalGPUs unknown (no gpu.count label); use all allocatable
		},
		{
			name:      "auto ignores an unshared whole-GPU resource",
			mode:      gpuSharingModeAuto,
			allocMode: allocationModeAuto,
			objects:   []runtime.Object{gpuNode("node1", 8, true, false)},
			wantSkip:  true,
		},
		{
			name:      "auto with -allocation-mode=dra does not fall back to the device plugin",
			mode:      gpuSharingModeAuto,
			allocMode: allocationModeDRA,
			objects: []runtime.Object{
				sharedGPUNode("ts-node", map[string]int64{"nvidia.com/gpu": 4}, map[string]string{"nvidia.com/gpu.replicas": "4"}),
			},
			wantSkip: true,
		},
		{
			name:      "a named resource selects the device plugin and the node with most units",
			mode:      gpuSharingModeAuto,
			resource:  "nvidia.com/gpu",
			allocMode: allocationModeAuto,
			objects: append(draObjects(),
				sharedGPUNode("small", map[string]int64{"nvidia.com/gpu": 2}, nil),
				sharedGPUNode("big", map[string]int64{"nvidia.com/gpu": 4}, map[string]string{"nvidia.com/gpu.count": "1"})),
			wantMode:     gpuSharingModeDevicePlugin,
			wantNode:     "big",
			wantResource: "nvidia.com/gpu",
			wantKind:     gpuSharingKindUnknown,
			wantPods:     2,
		},
		{
			name:     "explicit -gpu-sharing-pods is used",
			mode:     gpuSharingModeDRASharedClaim,
			pods:     4,
			objects:  draObjects(),
			wantMode: gpuSharingModeDRASharedClaim,
			wantNode: "node1",
			wantPods: 4,
		},
		{
			name:    "explicit dra-shared-claim fails without DRA",
			mode:    gpuSharingModeDRASharedClaim,
			objects: []runtime.Object{gpuNode("node1", 1, true, false)},
			wantErr: "DRA is not usable",
		},
		{
			name:     "dra-shared-claim rejects -gpu-sharing-resource",
			mode:     gpuSharingModeDRASharedClaim,
			resource: "nvidia.com/gpu",
			objects:  draObjects(),
			wantErr:  "only applies to",
		},
		{
			name:    "explicit device-plugin fails without a shared resource",
			mode:    gpuSharingModeDevicePlugin,
			objects: []runtime.Object{gpuNode("node1", 1, true, false)},
			wantErr: "no usable shared resource",
		},
		{
			name:     "too many requested Pods for the node",
			mode:     gpuSharingModeDevicePlugin,
			resource: "nvidia.com/mig-1g.5gb",
			pods:     8,
			objects:  []runtime.Object{sharedGPUNode("mig-node", map[string]int64{"nvidia.com/mig-1g.5gb": 7}, nil)},
			wantErr:  "exceeds the 7 allocatable",
		},
		{
			name:    "one Pod cannot show sharing",
			mode:    gpuSharingModeDRASharedClaim,
			pods:    1,
			objects: draObjects(),
			wantErr: "at least 2 Pods",
		},
		{
			name:    "invalid mode",
			mode:    "bogus",
			wantErr: `invalid -gpu-sharing-mode "bogus"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewClientset(tt.objects...)
			plan, err := resolveGPUSharingPlan(context.Background(), client, tt.mode, tt.resource, tt.pods, tt.allocMode, cfg, vcfg, t.Logf)
			switch {
			case tt.wantSkip:
				if !errors.Is(err, errNoGPUSharingMechanism) {
					t.Fatalf("want errNoGPUSharingMechanism, got plan=%+v err=%v", plan, err)
				}
				return
			case tt.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("want error containing %q, got plan=%+v err=%v", tt.wantErr, plan, err)
				}
				return
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			}
			if plan.mode != tt.wantMode || plan.node != tt.wantNode || plan.resource != tt.wantResource || plan.kind != tt.wantKind || plan.pods != tt.wantPods {
				t.Errorf("plan = %+v; want mode=%q node=%q resource=%q kind=%q pods=%d",
					plan, tt.wantMode, tt.wantNode, tt.wantResource, tt.wantKind, tt.wantPods)
			}
		})
	}
}

func TestGPUSharingPodCount(t *testing.T) {
	dp := func(allocatable int64, physical int) gpuSharingPlan {
		return gpuSharingPlan{mode: gpuSharingModeDevicePlugin, node: "n", resource: "r", allocatable: allocatable, physicalGPUs: physical}
	}
	tests := []struct {
		name      string
		requested int
		plan      gpuSharingPlan
		want      int
		wantErr   bool
	}{
		{name: "DRA default", plan: gpuSharingPlan{mode: gpuSharingModeDRASharedClaim}, want: 2},
		{name: "unknown physical count uses all allocatable units", plan: dp(4, 0), want: 4},
		{name: "one more than the physical GPUs", plan: dp(32, 8), want: 9},
		{name: "capped at allocatable", plan: dp(4, 8), want: 4},
		{name: "allocatable below 2", plan: dp(1, 1), wantErr: true},
		{name: "explicit within allocatable", requested: 3, plan: dp(4, 0), want: 3},
		{name: "explicit above allocatable", requested: 5, plan: dp(4, 0), wantErr: true},
		{name: "negative", requested: -1, plan: dp(4, 0), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := gpuSharingPodCount(tt.requested, tt.plan)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error, got %d", got)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("got %d, %v; want %d", got, err, tt.want)
			}
		})
	}
}

func TestCheckSharedResourceExposure(t *testing.T) {
	tests := []struct {
		name    string
		plan    gpuSharingPlan
		wantErr string
	}{
		{name: "DRA has no device-plugin exposure to check", plan: gpuSharingPlan{mode: gpuSharingModeDRASharedClaim}},
		{name: "oversubscribed time-slicing", plan: gpuSharingPlan{mode: gpuSharingModeDevicePlugin, kind: gpuSharingKindSoftware, allocatable: 4, physicalGPUs: 1}},
		{name: "software sharing that does not oversubscribe", plan: gpuSharingPlan{mode: gpuSharingModeDevicePlugin, kind: gpuSharingKindSoftware, allocatable: 2, physicalGPUs: 2}, wantErr: "must oversubscribe"},
		{name: "unknown kind is held to oversubscription", plan: gpuSharingPlan{mode: gpuSharingModeDevicePlugin, kind: gpuSharingKindUnknown, allocatable: 8, physicalGPUs: 8}, wantErr: "must oversubscribe"},
		{name: "unknown physical count only needs 2 units", plan: gpuSharingPlan{mode: gpuSharingModeDevicePlugin, kind: gpuSharingKindUnknown, allocatable: 2}},
		{name: "partitions need not exceed the GPU count", plan: gpuSharingPlan{mode: gpuSharingModeDevicePlugin, kind: gpuSharingKindPartition, allocatable: 2, physicalGPUs: 2}},
		{name: "a single unit is not shared", plan: gpuSharingPlan{mode: gpuSharingModeDevicePlugin, kind: gpuSharingKindPartition, allocatable: 1}, wantErr: "at least 2 schedulable units"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkSharedResourceExposure(tt.plan)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestEvaluateGPUSharing(t *testing.T) {
	result := func(physical string, logical ...string) gpuSharingProbeResult {
		return gpuSharingProbeResult{PhysicalIDs: []string{physical}, LogicalIDs: logical, Healthy: true, Completed: true}
	}
	pods := []string{"p0", "p1", "p2"}
	dp := gpuSharingPlan{mode: gpuSharingModeDevicePlugin, resource: "nvidia.com/gpu", kind: gpuSharingKindSoftware, physicalGPUs: 2}
	mig := gpuSharingPlan{mode: gpuSharingModeDevicePlugin, resource: "nvidia.com/mig-1g.5gb", kind: gpuSharingKindPartition, physicalGPUs: 2}
	dra := gpuSharingPlan{mode: gpuSharingModeDRASharedClaim}

	tests := []struct {
		name    string
		plan    gpuSharingPlan
		pods    []string
		results map[string]gpuSharingProbeResult
		wantErr string
	}{
		{
			name:    "two of three time-sliced Pods share a GPU",
			plan:    dp,
			pods:    pods,
			results: map[string]gpuSharingProbeResult{"p0": result("GPU-a"), "p1": result("GPU-b"), "p2": result("GPU-a")},
		},
		{
			name:    "every Pod on a different GPU",
			plan:    dp,
			pods:    pods[:2],
			results: map[string]gpuSharingProbeResult{"p0": result("GPU-a"), "p1": result("GPU-b")},
			wantErr: "share a physical GPU",
		},
		{
			name:    "distinct MIG partitions of one GPU",
			plan:    mig,
			pods:    pods[:2],
			results: map[string]gpuSharingProbeResult{"p0": result("GPU-a", "MIG-1"), "p1": result("GPU-a", "MIG-2")},
		},
		{
			name:    "one MIG partition handed to two Pods",
			plan:    mig,
			pods:    pods[:2],
			results: map[string]gpuSharingProbeResult{"p0": result("GPU-a", "MIG-1"), "p1": result("GPU-a", "MIG-1")},
			wantErr: "same partition",
		},
		{
			name:    "shared claim gives every Pod the same GPU",
			plan:    dra,
			pods:    pods,
			results: map[string]gpuSharingProbeResult{"p0": result("GPU-a"), "p1": result("GPU-a"), "p2": result("GPU-a")},
		},
		{
			name:    "shared claim Pods on different GPUs",
			plan:    dra,
			pods:    pods,
			results: map[string]gpuSharingProbeResult{"p0": result("GPU-a"), "p1": result("GPU-a"), "p2": result("GPU-b")},
			wantErr: "saw different GPUs",
		},
		{
			name:    "Pod sees no GPU",
			plan:    dra,
			pods:    pods[:2],
			results: map[string]gpuSharingProbeResult{"p0": result("GPU-a"), "p1": {Completed: true}},
			wantErr: "exactly one visible physical GPU",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := evaluateGPUSharing(tt.plan, tt.pods, tt.results)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestBuildGPUSharingPod(t *testing.T) {
	cfg := nvidiaConfig(t)
	vcfg := nvidiaGPUSharingConfig(t)

	t.Run("dra-shared-claim references the shared ResourceClaim", func(t *testing.T) {
		pod := buildGPUSharingPod("ns", "p", gpuSharingPlan{mode: gpuSharingModeDRASharedClaim, node: "node1"}, cfg, vcfg)
		if len(pod.Spec.ResourceClaims) != 1 || pod.Spec.ResourceClaims[0].ResourceClaimName == nil ||
			*pod.Spec.ResourceClaims[0].ResourceClaimName != gpuSharingClaimName || pod.Spec.ResourceClaims[0].ResourceClaimTemplateName != nil {
			t.Errorf("pod must reference ResourceClaim %s directly: %+v", gpuSharingClaimName, pod.Spec.ResourceClaims)
		}
		if len(pod.Spec.Containers[0].Resources.Claims) != 1 || pod.Spec.Containers[0].Resources.Claims[0].Name != gpuSharingPodClaimName {
			t.Errorf("container must use the claim: %+v", pod.Spec.Containers[0].Resources)
		}
		if pod.Spec.Affinity != nil || pod.Spec.NodeName != "" {
			t.Error("a shared claim pins Pods through its allocation; no node pinning expected")
		}
	})

	t.Run("device-plugin requests one unit and stays on the plan's node via the scheduler", func(t *testing.T) {
		plan := gpuSharingPlan{mode: gpuSharingModeDevicePlugin, node: "node1", resource: "nvidia.com/mig-1g.5gb"}
		pod := buildGPUSharingPod("ns", "p", plan, cfg, vcfg)
		q, ok := pod.Spec.Containers[0].Resources.Limits["nvidia.com/mig-1g.5gb"]
		if !ok || q.Value() != 1 {
			t.Errorf("want a limit of 1 nvidia.com/mig-1g.5gb, got %+v", pod.Spec.Containers[0].Resources.Limits)
		}
		if pod.Spec.NodeName != "" {
			t.Error("must not bypass the scheduler with spec.nodeName")
		}
		terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
		if len(terms) != 1 || len(terms[0].MatchFields) != 1 || terms[0].MatchFields[0].Key != "metadata.name" ||
			!slices.Equal(terms[0].MatchFields[0].Values, []string{"node1"}) {
			t.Errorf("want required node affinity on metadata.name=node1, got %+v", terms)
		}
		if len(pod.Spec.ResourceClaims) != 0 {
			t.Errorf("device-plugin mode must not use ResourceClaims: %+v", pod.Spec.ResourceClaims)
		}
	})

	t.Run("probe container gets the vendor environment and tolerates the accelerator taint", func(t *testing.T) {
		pod := buildGPUSharingPod("ns", "p", gpuSharingPlan{mode: gpuSharingModeDRASharedClaim}, cfg, vcfg)
		found := false
		for _, e := range pod.Spec.Containers[0].Env {
			if e.Name == "NVIDIA_DRIVER_CAPABILITIES" && strings.Contains(e.Value, "utility") {
				found = true
			}
		}
		if !found {
			t.Errorf("NVIDIA_DRIVER_CAPABILITIES with utility not set: %+v", pod.Spec.Containers[0].Env)
		}
		if len(pod.Spec.Tolerations) != 1 || pod.Spec.Tolerations[0].Key != cfg.TaintKey {
			t.Errorf("want a toleration for %s, got %+v", cfg.TaintKey, pod.Spec.Tolerations)
		}
	})
}
