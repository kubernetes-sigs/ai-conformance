package conformance

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

const (
	gpuSharingModeAuto           = "auto"
	gpuSharingModeDRASharedClaim = "dra-shared-claim"
	gpuSharingModeDevicePlugin   = "device-plugin"

	gpuSharingProbeContainer = "prober"
	gpuSharingProbeImage     = "ubuntu:22.04"
	gpuSharingClaimName      = "shared-accelerator"
	gpuSharingPodClaimName   = "gpu"
	gpuSharingRequestName    = "shared-accelerator"
	gpuSharingTestLabel      = "gpu-sharing"

	gpuSharingPhysicalIDPrefix = "GPU_SHARING: PHYSICAL_ID="
	gpuSharingLogicalIDPrefix  = "GPU_SHARING: LOGICAL_ID="
	gpuSharingHealthPrefix     = "GPU_SHARING: HEALTH="
	gpuSharingErrorPrefix      = "GPU_SHARING: ERROR="
	gpuSharingCompletedMarker  = "GPU_SHARING: COMPLETED"
)

const (
	gpuSharingKindSoftware  = "software"
	gpuSharingKindPartition = "partition"
	gpuSharingKindUnknown   = "unknown"
)

var (
	gpuSharingMode     *string
	gpuSharingResource *string
	gpuSharingPods     *int
	gpuSharingTimeout  *time.Duration

	errNoGPUSharingMechanism = errors.New("no GPU sharing mechanism detected")
)

func init() {
	registerFlagGroup("gpu-sharing", "GPU Sharing Flags (TestGPUSharing)")
	gpuSharingMode = flag.String("gpu-sharing-mode", gpuSharingModeAuto,
		"GPU sharing mechanism to verify: 'dra-shared-claim' (several Pods reference one DRA ResourceClaim), 'device-plugin' (Pods request a shared or partitioned extended resource, e.g. time-sliced nvidia.com/gpu or nvidia.com/mig-1g.5gb), or 'auto' (default; follows -allocation-mode and prefers DRA when usable). The test is skipped when 'auto' finds no mechanism.")
	gpuSharingResource = flag.String("gpu-sharing-resource", "",
		"Extended resource that exposes shared or partitioned GPUs in 'device-plugin' mode (e.g. nvidia.com/gpu with time-slicing, or nvidia.com/mig-1g.5gb). Setting it implies 'device-plugin' when -gpu-sharing-mode=auto. If empty, it is detected from node allocatable and labels.")
	gpuSharingPods = flag.Int("gpu-sharing-pods", 0,
		"Number of concurrent Pods that request one shared GPU unit each. 0 (default) picks 2 for 'dra-shared-claim', and for 'device-plugin' one more than the node's physical GPU count when it is known (so two Pods must land on the same physical GPU), capped at the node's allocatable units.")
	gpuSharingTimeout = flag.Duration("gpu-sharing-timeout", 3*time.Minute,
		"Timeout for the GPU sharing Pods to all be Running at the same time, and for each Pod's GPU probe to complete.")
}

type gpuSharingVendorConfig struct {
	// probeScript runs in each Pod and prints the GPU_SHARING lines parsed
	// by parseGPUSharingProbeLogs: one PHYSICAL_ID per visible physical GPU,
	// a LOGICAL_ID per visible partition, HEALTH=PASS|FAIL and COMPLETED.
	probeScript string
	// env is set on the probe container (e.g. so the container runtime
	// injects the vendor's management tool).
	env []corev1.EnvVar
	// partitionResourcePrefixes are extended resource name prefixes of
	// hardware partitions (e.g. MIG profiles).
	partitionResourcePrefixes []string
	// physicalCountLabel is a node label that reports the number of physical
	// GPUs on the node, if the platform publishes one.
	physicalCountLabel string
	// wholeResourceSharing reports whether node labels say the whole-GPU
	// extended resource is shared, and how.
	wholeResourceSharing func(labels map[string]string) (kind, reason string, ok bool)
	// draSharing is the opaque vendor config applied to the DRA ResourceClaim
	// so the driver enforces sharing semantics. Without it some DRA drivers
	// allocate an exclusive device per Pod and the claim passes vacuously on
	// any DRA cluster regardless of whether GPU sharing is enabled.
	draSharing *resourcev1.OpaqueDeviceConfiguration
}

var gpuSharingVendorConfigs = map[string]gpuSharingVendorConfig{
	"nvidia": {
		probeScript: nvidiaGPUSharingProbeScript,
		// With the legacy NVIDIA container runtime, nvidia-smi is only
		// injected when the "utility" capability is requested.
		env:                       []corev1.EnvVar{{Name: "NVIDIA_DRIVER_CAPABILITIES", Value: "compute,utility"}},
		partitionResourcePrefixes: []string{"nvidia.com/mig-"},
		// Published by NVIDIA GPU Feature Discovery.
		physicalCountLabel:   "nvidia.com/gpu.count",
		wholeResourceSharing: nvidiaWholeGPUSharing,
		// draSharing is left nil: the DRA shared-claim mechanism already
		// proves GPU sharing without vendor-specific config — all Pods
		// referencing one ResourceClaim receive the same allocated device,
		// and the test verifies they observe the same physical GPU UUID.
		// Vendor time-slicing config is only needed when the driver requires
		// an explicit sharing mode and rejects claims without one.
	},
}

func nvidiaWholeGPUSharing(labels map[string]string) (string, string, bool) {
	if r, err := strconv.Atoi(labels["nvidia.com/gpu.replicas"]); err == nil && r > 1 {
		return gpuSharingKindSoftware, fmt.Sprintf("nvidia.com/gpu.replicas=%d", r), true
	}
	switch s := labels["nvidia.com/gpu.sharing-strategy"]; s {
	case "time-slicing", "mps":
		return gpuSharingKindSoftware, "nvidia.com/gpu.sharing-strategy=" + s, true
	}
	if labels["nvidia.com/mig.strategy"] == "single" {
		// With the "single" MIG strategy nvidia.com/gpu itself is a MIG
		// partition.
		return gpuSharingKindPartition, "nvidia.com/mig.strategy=single", true
	}
	return "", "", false
}

// nvidiaToolDirs are prepended to PATH by nvidiaToolEnvSetup: some platforms
// mount nvidia-smi outside the image's default PATH (e.g. GKE uses
// /usr/local/nvidia/bin).
var nvidiaToolDirs = []string{"/usr/local/nvidia/bin", "/usr/local/bin", "/usr/bin", "/bin"}

// nvidiaToolEnvSetup extends PATH and LD_LIBRARY_PATH so that nvidia-smi and
// the NVIDIA user-space libraries are reachable on platforms (e.g. GKE) that
// install the driver stack outside the standard search paths. It is shared
// between nvidiaGPUSharingProbeScript and nvidiaDriverProbeScript.
var nvidiaToolEnvSetup = `export PATH="$PATH:` + strings.Join(nvidiaToolDirs, ":") + `"
if [ -d /usr/local/nvidia/lib64 ]; then
  export LD_LIBRARY_PATH="/usr/local/nvidia/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi`

var nvidiaGPUSharingProbeScript = nvidiaToolEnvSetup + `
health=PASS
if ! command -v nvidia-smi >/dev/null 2>&1; then
  echo "` + gpuSharingErrorPrefix + `nvidia-smi not found in the container"
  health=FAIL
else
  list=$(nvidia-smi -L 2>&1)
  rc=$?
  echo "$list"
  if [ "$rc" -ne 0 ]; then
    echo "` + gpuSharingErrorPrefix + `nvidia-smi -L exited with $rc"
    health=FAIL
  fi
  echo "$list" | sed -n 's/^GPU [0-9][0-9]*:.*(UUID: \(GPU-[^)]*\)).*$/` + gpuSharingPhysicalIDPrefix + `\1/p'
  echo "$list" | sed -n 's/^[[:space:]]*MIG .*(UUID: \(MIG-[^)]*\)).*$/` + gpuSharingLogicalIDPrefix + `\1/p'
  if ! nvidia-smi >/dev/null 2>&1; then
    echo "` + gpuSharingErrorPrefix + `nvidia-smi reported an error"
    health=FAIL
  fi
fi
echo "` + gpuSharingHealthPrefix + `$health"
echo "` + gpuSharingCompletedMarker + `"`

type gpuSharingProbeResult struct {
	PhysicalIDs []string
	LogicalIDs  []string
	Healthy     bool
	Errors      []string
	Completed   bool
	RawLogs     string
}

func (r gpuSharingProbeResult) PhysicalID() (string, error) {
	if len(r.PhysicalIDs) != 1 {
		return "", fmt.Errorf("expected exactly one visible physical GPU, found %d (%v)", len(r.PhysicalIDs), r.PhysicalIDs)
	}
	return r.PhysicalIDs[0], nil
}

func (r gpuSharingProbeResult) LogicalID() string {
	if len(r.LogicalIDs) > 0 {
		return strings.Join(r.LogicalIDs, ",")
	}
	return strings.Join(r.PhysicalIDs, ",")
}

func parseGPUSharingProbeLogs(logs string) gpuSharingProbeResult {
	res := gpuSharingProbeResult{RawLogs: logs}
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, gpuSharingPhysicalIDPrefix):
			if v := strings.TrimSpace(strings.TrimPrefix(line, gpuSharingPhysicalIDPrefix)); v != "" {
				res.PhysicalIDs = append(res.PhysicalIDs, v)
			}
		case strings.HasPrefix(line, gpuSharingLogicalIDPrefix):
			if v := strings.TrimSpace(strings.TrimPrefix(line, gpuSharingLogicalIDPrefix)); v != "" {
				res.LogicalIDs = append(res.LogicalIDs, v)
			}
		case line == gpuSharingHealthPrefix+"PASS":
			res.Healthy = true
		case strings.HasPrefix(line, gpuSharingErrorPrefix):
			res.Errors = append(res.Errors, strings.TrimPrefix(line, gpuSharingErrorPrefix))
		case line == gpuSharingCompletedMarker:
			res.Completed = true
		}
	}
	return res
}

type gpuSharingPlan struct {
	mode string
	// node is where the shared resource was found. In device-plugin mode
	// every Pod is pinned to it so they compete for the same GPUs.
	node         string
	resource     string
	kind         string
	reason       string
	allocatable  int64
	physicalGPUs int // 0 if unknown
	pods         int
}

func resolveGPUSharingPlan(ctx context.Context, c kubernetes.Interface, requestedMode, requestedResource string, requestedPods int, globalAllocationMode string, cfg AcceleratorConfig, vcfg gpuSharingVendorConfig, logf func(string, ...any)) (gpuSharingPlan, error) {
	var plan gpuSharingPlan
	var err error
	switch requestedMode {
	case gpuSharingModeDRASharedClaim:
		if requestedResource != "" {
			return plan, fmt.Errorf("-gpu-sharing-resource=%q only applies to -gpu-sharing-mode=%s", requestedResource, gpuSharingModeDevicePlugin)
		}
		plan, err = draSharedClaimPlan(ctx, c, cfg, logf)
		if err != nil {
			return plan, fmt.Errorf("-gpu-sharing-mode=%s requested but DRA is not usable: %w", gpuSharingModeDRASharedClaim, err)
		}
	case gpuSharingModeDevicePlugin:
		plan, err = devicePluginSharingPlan(ctx, c, requestedResource, cfg, vcfg)
		if err != nil {
			return plan, fmt.Errorf("-gpu-sharing-mode=%s requested but no usable shared resource was found: %w", gpuSharingModeDevicePlugin, err)
		}
	case gpuSharingModeAuto:
		plan, err = autoGPUSharingPlan(ctx, c, requestedResource, globalAllocationMode, cfg, vcfg, logf)
		if err != nil {
			return plan, err
		}
	default:
		return plan, fmt.Errorf("invalid -gpu-sharing-mode %q; supported values: %q, %q, %q",
			requestedMode, gpuSharingModeAuto, gpuSharingModeDRASharedClaim, gpuSharingModeDevicePlugin)
	}

	plan.pods, err = gpuSharingPodCount(requestedPods, plan)
	if err != nil {
		return plan, err
	}
	return plan, nil
}

func autoGPUSharingPlan(ctx context.Context, c kubernetes.Interface, requestedResource, globalAllocationMode string, cfg AcceleratorConfig, vcfg gpuSharingVendorConfig, logf func(string, ...any)) (gpuSharingPlan, error) {
	if requestedResource != "" {
		plan, err := devicePluginSharingPlan(ctx, c, requestedResource, cfg, vcfg)
		if err != nil {
			return plan, fmt.Errorf("-gpu-sharing-resource=%q is set but not usable: %w", requestedResource, err)
		}
		return plan, nil
	}

	var reasons []string
	if globalAllocationMode != allocationModeDevicePlugin {
		plan, err := draSharedClaimPlan(ctx, c, cfg, logf)
		if err == nil {
			return plan, nil
		}
		reasons = append(reasons, fmt.Sprintf("DRA: %v", err))
	}
	if globalAllocationMode != allocationModeDRA {
		plan, err := devicePluginSharingPlan(ctx, c, "", cfg, vcfg)
		if err == nil {
			return plan, nil
		}
		reasons = append(reasons, fmt.Sprintf("device plugin: %v", err))
	}
	return gpuSharingPlan{}, fmt.Errorf("%w (%s)", errNoGPUSharingMechanism, strings.Join(reasons, "; "))
}

func draSharedClaimPlan(ctx context.Context, c kubernetes.Interface, cfg AcceleratorConfig, logf func(string, ...any)) (gpuSharingPlan, error) {
	node, err := checkDRAUsable(ctx, c, cfg, logf)
	if err != nil {
		return gpuSharingPlan{}, err
	}
	return gpuSharingPlan{
		mode:   gpuSharingModeDRASharedClaim,
		node:   node,
		reason: fmt.Sprintf("DRA DeviceClass %s is usable; Pods will share one ResourceClaim", cfg.DeviceClass),
	}, nil
}

func devicePluginSharingPlan(ctx context.Context, c kubernetes.Interface, resourceName string, cfg AcceleratorConfig, vcfg gpuSharingVendorConfig) (gpuSharingPlan, error) {
	usable, err := usableNodes(ctx, c)
	if err != nil {
		return gpuSharingPlan{}, err
	}
	names := make([]string, 0, len(usable))
	for name := range usable {
		names = append(names, name)
	}
	sort.Strings(names)

	newPlan := func(node corev1.Node, res, kind, reason string) gpuSharingPlan {
		q := node.Status.Allocatable[corev1.ResourceName(res)]
		return gpuSharingPlan{
			mode:         gpuSharingModeDevicePlugin,
			node:         node.Name,
			resource:     res,
			kind:         kind,
			reason:       reason,
			allocatable:  q.Value(),
			physicalGPUs: physicalGPUCount(node, vcfg),
		}
	}

	if resourceName != "" {
		// Prefer the node with the most units so concurrent Pods fit.
		var best *corev1.Node
		var bestQty int64
		for _, name := range names {
			node := usable[name]
			q, ok := node.Status.Allocatable[corev1.ResourceName(resourceName)]
			if !ok || q.Value() <= bestQty {
				continue
			}
			best, bestQty = &node, q.Value()
		}
		if best == nil {
			return gpuSharingPlan{}, fmt.Errorf("no schedulable Ready node advertises allocatable %s", resourceName)
		}
		kind, reason := classifySharedResource(*best, resourceName, cfg, vcfg)
		return newPlan(*best, resourceName, kind, reason), nil
	}

	for _, name := range names {
		node := usable[name]
		if res, ok := partitionResource(node, vcfg); ok {
			return newPlan(node, res, gpuSharingKindPartition, fmt.Sprintf("node %s advertises hardware partition resource %s", name, res)), nil
		}
		if vcfg.wholeResourceSharing == nil {
			continue
		}
		q, ok := node.Status.Allocatable[corev1.ResourceName(cfg.ExtendedResource)]
		if !ok || q.IsZero() {
			continue
		}
		if kind, reason, shared := vcfg.wholeResourceSharing(node.Labels); shared {
			return newPlan(node, cfg.ExtendedResource, kind, fmt.Sprintf("node %s labels report %s is shared (%s)", name, cfg.ExtendedResource, reason)), nil
		}
	}
	return gpuSharingPlan{}, fmt.Errorf("no schedulable Ready node advertises a hardware partition resource (prefixes %v) or a shared %s; set -gpu-sharing-resource to name the shared resource",
		vcfg.partitionResourcePrefixes, cfg.ExtendedResource)
}

func partitionResource(node corev1.Node, vcfg gpuSharingVendorConfig) (string, bool) {
	// Pick the partition resource with the most allocatable units so that
	// concurrent test Pods can land on the same physical GPU. Alphabetical
	// first would fail when a GPU has both a coarse profile (1 unit/GPU) and
	// a fine profile (7 units/GPU) and the coarse name sorts first.
	var best string
	var bestQty int64
	for name, q := range node.Status.Allocatable {
		if q.IsZero() {
			continue
		}
		for _, prefix := range vcfg.partitionResourcePrefixes {
			if strings.HasPrefix(string(name), prefix) {
				if q.Value() > bestQty {
					best, bestQty = string(name), q.Value()
				}
				break
			}
		}
	}
	return best, best != ""
}

func classifySharedResource(node corev1.Node, resourceName string, cfg AcceleratorConfig, vcfg gpuSharingVendorConfig) (string, string) {
	for _, prefix := range vcfg.partitionResourcePrefixes {
		if strings.HasPrefix(resourceName, prefix) {
			return gpuSharingKindPartition, fmt.Sprintf("-gpu-sharing-resource=%s is a hardware partition resource", resourceName)
		}
	}
	if resourceName == cfg.ExtendedResource && vcfg.wholeResourceSharing != nil {
		if kind, reason, ok := vcfg.wholeResourceSharing(node.Labels); ok {
			return kind, fmt.Sprintf("-gpu-sharing-resource=%s (%s)", resourceName, reason)
		}
	}
	return gpuSharingKindUnknown, fmt.Sprintf("-gpu-sharing-resource=%s", resourceName)
}

func physicalGPUCount(node corev1.Node, vcfg gpuSharingVendorConfig) int {
	if vcfg.physicalCountLabel == "" {
		return 0
	}
	n, err := strconv.Atoi(node.Labels[vcfg.physicalCountLabel])
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func gpuSharingPodCount(requested int, plan gpuSharingPlan) (int, error) {
	if requested < 0 || requested == 1 {
		return 0, fmt.Errorf("-gpu-sharing-pods=%d: at least 2 Pods are needed to show sharing (or 0 for the default)", requested)
	}
	if plan.mode == gpuSharingModeDRASharedClaim {
		if requested == 0 {
			return 2, nil
		}
		return requested, nil
	}

	if requested > 0 {
		if int64(requested) > plan.allocatable {
			return 0, fmt.Errorf("-gpu-sharing-pods=%d exceeds the %d allocatable %s on node %s", requested, plan.allocatable, plan.resource, plan.node)
		}
		return requested, nil
	}
	var n int64
	if plan.physicalGPUs > 0 {
		// One more than the physical GPU count so the pigeonhole principle
		// forces at least two Pods onto the same GPU.
		n = int64(plan.physicalGPUs) + 1
	} else {
		// Physical GPU count unknown: use all allocatable units. With the
		// NVIDIA device plugin's distributed policy, only 2 Pods would
		// spread across GPUs on a multi-GPU node; using the full allocatable
		// count ensures at least two Pods end up on the same physical GPU.
		n = plan.allocatable
	}
	if n > plan.allocatable {
		n = plan.allocatable
	}
	if n < 2 {
		return 0, fmt.Errorf("node %s exposes %d allocatable %s; at least 2 are needed to run concurrent workloads on one GPU", plan.node, plan.allocatable, plan.resource)
	}
	return int(n), nil
}

func checkSharedResourceExposure(plan gpuSharingPlan) error {
	if plan.mode != gpuSharingModeDevicePlugin {
		return nil
	}
	if plan.allocatable < 2 {
		return fmt.Errorf("node %s exposes %d allocatable %s; a shared GPU must be exposed as at least 2 schedulable units", plan.node, plan.allocatable, plan.resource)
	}
	if plan.kind != gpuSharingKindPartition && plan.physicalGPUs > 0 && plan.allocatable <= int64(plan.physicalGPUs) {
		return fmt.Errorf("node %s exposes %d allocatable %s for %d physical GPU(s); software sharing must oversubscribe the GPUs (allocatable > physical)",
			plan.node, plan.allocatable, plan.resource, plan.physicalGPUs)
	}
	return nil
}

// TestGPUSharing verifies the GPU Sharing conformance requirement (KAR-0003).
// Ref: https://github.com/kubernetes-sigs/ai-conformance/tree/main/kars/0003-gpu-sharing
func TestGPUSharing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if !flag.Parsed() {
		flag.Parse()
	}

	cfg, err := lookupAcceleratorConfig(*acceleratorType)
	if err != nil {
		t.Fatalf("Invalid -accelerator-type: %v", err)
	}
	vcfg, ok := gpuSharingVendorConfigs[*acceleratorType]
	if !ok {
		t.Skipf("UNKNOWN: no GPU sharing probe for accelerator type %q; manual verification is required", *acceleratorType)
	}

	clientset := getClientset(t)
	ctx := context.Background()

	plan, err := resolveGPUSharingPlan(ctx, clientset, *gpuSharingMode, *gpuSharingResource, *gpuSharingPods, *allocationMode, cfg, vcfg, t.Logf)
	if err != nil {
		if errors.Is(err, errNoGPUSharingMechanism) {
			t.Skipf("Skipping GPU sharing test: %v. Set -gpu-sharing-mode or -gpu-sharing-resource to run. Platforms that do not support GPU sharing may leave them unset to opt out.", err)
		}
		t.Fatalf("Failed to resolve the GPU sharing mechanism: %v", err)
	}
	t.Logf("GPU sharing test running with mode %s on node %s: %s (resource=%q, kind=%q, allocatable=%d, physical GPUs=%d, Pods=%d)",
		plan.mode, plan.node, plan.reason, plan.resource, plan.kind, plan.allocatable, plan.physicalGPUs, plan.pods)

	if err := checkSharedResourceExposure(plan); err != nil {
		t.Run("SharedResourceExposure", func(t *testing.T) {
			t.Fatalf("FAIL: %v", err)
		})
		return
	}

	namespace := randomNamespaceName("gpu-sharing-conformance")
	if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create namespace %s: %v", namespace, err)
	}
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: Failed to delete namespace %s: %v", namespace, err)
		}
	})

	if plan.mode == gpuSharingModeDRASharedClaim {
		if _, err := clientset.ResourceV1().ResourceClaims(namespace).Create(ctx, buildSharedResourceClaim(namespace, cfg, vcfg), metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create shared ResourceClaim: %v", err)
		}
	}

	podNames := make([]string, 0, plan.pods)
	for i := 0; i < plan.pods; i++ {
		name := fmt.Sprintf("gpu-sharing-%d", i)
		podNames = append(podNames, name)
		createTestPod(ctx, t, clientset, buildGPUSharingPod(namespace, name, plan, cfg, vcfg))
	}

	// Every Pod must hold its share at the same time; Pods that only run one
	// after another would not show sharing.
	t.Logf("Waiting for %d GPU sharing Pods to be Running at the same time...", len(podNames))
	running, err := waitForPodsRunning(ctx, clientset, namespace, podNames, *gpuSharingTimeout)
	if err != nil {
		t.Fatalf("FAIL: the GPU sharing Pods did not all run concurrently: %v\n%s", err, describePodsNotRunning(ctx, clientset, namespace, podNames))
	}

	results := make(map[string]gpuSharingProbeResult, len(podNames))
	for _, name := range podNames {
		logs, err := waitForGPUSharingProbe(ctx, clientset, namespace, name, *gpuSharingTimeout)
		if err != nil {
			t.Fatalf("GPU probe in Pod %s did not complete: %v", name, err)
		}
		results[name] = parseGPUSharingProbeLogs(logs)
		t.Logf("Pod %s on node %s sees physical GPU(s) %v, logical device %s",
			name, running[name].Spec.NodeName, results[name].PhysicalIDs, results[name].LogicalID())
	}

	t.Run("SharedResourceExposure", func(t *testing.T) {
		switch plan.mode {
		case gpuSharingModeDRASharedClaim:
			if err := verifySharedClaim(ctx, clientset, namespace, podNames); err != nil {
				t.Fatalf("FAIL: %v", err)
			}
			t.Logf("PASS: ResourceClaim %s allocated one device and is reserved for all %d Pods", gpuSharingClaimName, len(podNames))
		case gpuSharingModeDevicePlugin:
			t.Logf("PASS: node %s exposes %d allocatable %s (%s kind, %d physical GPU(s) reported)",
				plan.node, plan.allocatable, plan.resource, plan.kind, plan.physicalGPUs)
		}
	})

	t.Run("ConcurrentWorkloadsShareGPU", func(t *testing.T) {
		if err := evaluateGPUSharing(plan, podNames, results); err != nil {
			t.Fatalf("FAIL: %v", err)
		}
		t.Logf("PASS: concurrent Pods share a physical GPU")
	})

	t.Run("AcceleratorHealth", func(t *testing.T) {
		for _, name := range podNames {
			r := results[name]
			if !r.Healthy {
				t.Errorf("FAIL: GPU health check failed in Pod %s (errors: %v). Logs:\n%s", name, r.Errors, r.RawLogs)
			}
		}
	})
}

func evaluateGPUSharing(plan gpuSharingPlan, podNames []string, results map[string]gpuSharingProbeResult) error {
	byGPU := map[string][]string{}
	for _, name := range podNames {
		id, err := results[name].PhysicalID()
		if err != nil {
			return fmt.Errorf("pod %s: %v. Logs:\n%s", name, err, results[name].RawLogs)
		}
		byGPU[id] = append(byGPU[id], name)
	}

	var largest []string
	var largestID string
	ids := make([]string, 0, len(byGPU))
	for id := range byGPU {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if len(byGPU[id]) > len(largest) {
			largest, largestID = byGPU[id], id
		}
	}

	switch plan.mode {
	case gpuSharingModeDRASharedClaim:
		if len(byGPU) != 1 {
			return fmt.Errorf("pods sharing one ResourceClaim saw different GPUs: %v", byGPU)
		}
	default:
		if len(largest) < 2 {
			hint := ""
			if plan.physicalGPUs == 0 {
				hint = " The node's physical GPU count is unknown, so the device plugin may have placed each Pod on a different GPU; raise -gpu-sharing-pods above the node's physical GPU count."
			}
			return fmt.Errorf("no two of the %d concurrent Pods requesting %s share a physical GPU: %v.%s", len(podNames), plan.resource, byGPU, hint)
		}
		if plan.kind == gpuSharingKindPartition {
			seen := map[string]string{}
			for _, name := range largest {
				logical := results[name].LogicalID()
				if other, dup := seen[logical]; dup {
					return fmt.Errorf("pods %s and %s were given the same partition %s of GPU %s; each partition must be allocated to one Pod", other, name, logical, largestID)
				}
				seen[logical] = name
			}
		}
	}
	return nil
}

func buildSharedResourceClaim(ns string, cfg AcceleratorConfig, vcfg gpuSharingVendorConfig) *resourcev1.ResourceClaim {
	claim := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: gpuSharingClaimName, Namespace: ns},
		Spec: resourcev1.ResourceClaimSpec{
			Devices: resourcev1.DeviceClaim{
				Requests: []resourcev1.DeviceRequest{{
					Name: gpuSharingRequestName,
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: cfg.DeviceClass,
						Count:           1,
					},
				}},
			},
		},
	}
	if vcfg.draSharing != nil {
		claim.Spec.Devices.Config = []resourcev1.DeviceClaimConfiguration{{
			DeviceConfiguration: resourcev1.DeviceConfiguration{Opaque: vcfg.draSharing},
		}}
	}
	return claim
}

func buildGPUSharingPod(ns, name string, plan gpuSharingPlan, cfg AcceleratorConfig, vcfg gpuSharingVendorConfig) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"ai-conformance.kubernetes.io/test": gpuSharingTestLabel},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Tolerations:   []corev1.Toleration{{Key: cfg.TaintKey, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
			Containers: []corev1.Container{{
				Name:    gpuSharingProbeContainer,
				Image:   gpuSharingProbeImage,
				Command: []string{"/bin/sh", "-c"},
				// Keep the container alive after the probe so all Pods hold
				// their GPU share at the same time.
				Args: []string{vcfg.probeScript + "\nsleep 3600"},
				Env:  append([]corev1.EnvVar(nil), vcfg.env...),
			}},
		},
	}

	switch plan.mode {
	case gpuSharingModeDRASharedClaim:
		claimName := gpuSharingClaimName
		pod.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: gpuSharingPodClaimName, ResourceClaimName: &claimName}}
		pod.Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: gpuSharingPodClaimName}}
	case gpuSharingModeDevicePlugin:
		pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
			corev1.ResourceName(plan.resource): *resource.NewQuantity(1, resource.DecimalSI),
		}
		// Go through the scheduler (unlike spec.nodeName) so the shared
		// resource is accounted for, but keep every Pod on the same node.
		pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchFields: []corev1.NodeSelectorRequirement{{
						Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{plan.node},
					}},
				}},
			},
		}}
	}
	return pod
}

func verifySharedClaim(ctx context.Context, c kubernetes.Interface, ns string, podNames []string) error {
	claim, err := c.ResourceV1().ResourceClaims(ns).Get(ctx, gpuSharingClaimName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get ResourceClaim %s: %w", gpuSharingClaimName, err)
	}
	if claim.Status.Allocation == nil {
		return fmt.Errorf("ResourceClaim %s is not allocated", gpuSharingClaimName)
	}
	if n := len(claim.Status.Allocation.Devices.Results); n != 1 {
		return fmt.Errorf("ResourceClaim %s was allocated %d devices, want 1", gpuSharingClaimName, n)
	}
	reserved := map[string]bool{}
	for _, ref := range claim.Status.ReservedFor {
		if ref.Resource == "pods" {
			reserved[ref.Name] = true
		}
	}
	var missing []string
	for _, name := range podNames {
		if !reserved[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("ResourceClaim %s is not reserved for Pods %v (reservedFor: %+v)", gpuSharingClaimName, missing, claim.Status.ReservedFor)
	}
	return nil
}

func waitForGPUSharingProbe(ctx context.Context, c kubernetes.Interface, ns, podName string, timeout time.Duration) (string, error) {
	var logs string
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		raw, err := c.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{Container: gpuSharingProbeContainer}).DoRaw(ctx)
		if err != nil {
			lastAPIError = err
			return false, nil
		}
		lastAPIError = nil
		logs = string(raw)
		return logsContainExactLine(logs, gpuSharingCompletedMarker), nil
	})
	if err != nil {
		return logs, fmt.Errorf("probe did not report %q within %s; containerStatus=%s%s: %w\nPartial logs:\n%s",
			gpuSharingCompletedMarker, timeout, containerStatusJSON(ctx, c, ns, podName, gpuSharingProbeContainer), lastAPIErrorSuffix(lastAPIError), err, logs)
	}
	return logs, nil
}

func describePodsNotRunning(ctx context.Context, c kubernetes.Interface, ns string, podNames []string) string {
	var b strings.Builder
	for _, name := range podNames {
		pod, err := c.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			fmt.Fprintf(&b, "  Pod %s: <failed to get: %v>\n", name, err)
			continue
		}
		if pod.Status.Phase == corev1.PodRunning {
			continue
		}
		fmt.Fprintf(&b, "  Pod %s: phase=%s", name, pod.Status.Phase)
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodScheduled && cond.Status != corev1.ConditionTrue {
				fmt.Fprintf(&b, " unschedulable (%s: %s)", cond.Reason, cond.Message)
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}
