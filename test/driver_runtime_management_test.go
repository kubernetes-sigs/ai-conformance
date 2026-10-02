package conformance

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

const (
	// Result lines printed by an AcceleratorConfig.DriverProbeScript and
	// parsed by parseDriverRuntimeProbeLogs.
	driverRuntimeConfigOKPrefix      = "RESULT: RUNTIME_CONFIG_OK="
	driverRuntimeFunctionalPrefix    = "RESULT: DRIVER_FUNCTIONAL="
	driverRuntimeActualDriverPrefix  = "RESULT: ACTUAL_DRIVER_VERSION="
	driverRuntimeActualRuntimePrefix = "RESULT: ACTUAL_RUNTIME_VERSION="
	// driverRuntimeUnknownStatusLine is logged verbatim when -accelerator-type
	// names a variant the suite does not recognize (KAR-0001 Conformance
	// Strategy: report Unknown rather than fail).
	driverRuntimeUnknownStatusLine = "RESULT: STATUS=UNKNOWN"

	// Mechanisms through which a platform advertises accelerator driver and
	// runtime state for a node.
	platformMechanismDRA          = "dra"
	platformMechanismNodeMetadata = "node-metadata"

	// driverRuntimeLogPollTimeout bounds how long the probe container may take
	// to print its RESULT lines after the Pod reports Running: nvidia-smi can
	// take tens of seconds on a cold GPU. Keep in step with the shared
	// acceleratorLogPollTimeout introduced by #94.
	driverRuntimeLogPollTimeout = 90 * time.Second
)

var driverRuntimeImage *string

func init() {
	driverRuntimeImage = flag.String("driver-runtime-image", "ubuntu:22.04",
		"Container image without pre-installed accelerator drivers or tools, used to verify that the container runtime configuration injects them into accelerator workloads.")
}

// nvidiaDriverProbeScript is the NVIDIA AcceleratorConfig.DriverProbeScript.
// A vanilla image ships neither nvidia-smi nor the NVIDIA user-space
// libraries, so a working nvidia-smi inside the container proves that the
// container runtime configuration (CDI spec or runtime hook) injected the host
// driver stack, and a successful NVML query proves that the kernel driver and
// the injected user-space libraries are compatible.
var nvidiaDriverProbeScript = fmt.Sprintf(`if [ -d /usr/local/nvidia/lib64 ]; then
  export LD_LIBRARY_PATH="/usr/local/nvidia/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi
SMI_BIN=""
for candidate in "$(command -v nvidia-smi 2>/dev/null)" /usr/bin/nvidia-smi /usr/local/nvidia/bin/nvidia-smi; do
  if [ -n "$candidate" ] && [ -x "$candidate" ]; then
    SMI_BIN="$candidate"
    break
  fi
done
TIMEOUT_CMD=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_CMD="timeout 30"
fi

RUNTIME_OK="false"
DRIVER_OK="false"
ACTUAL_DRIVER=""
ACTUAL_RUNTIME=""

if [ -n "$SMI_BIN" ] && [ "$count" -gt 0 ]; then
  RUNTIME_OK="true"
  # nvidia-smi prints NVML failures such as "Driver/library version mismatch"
  # on stdout with a non-zero exit status, so require both a zero exit status
  # and a purely numeric version before calling the driver functional.
  QUERY_OUT="$($TIMEOUT_CMD "$SMI_BIN" --query-gpu=driver_version --format=csv,noheader,nounits 2>/dev/null)"
  QUERY_RC=$?
  QUERY_VER="$(printf '%%s\n' "$QUERY_OUT" | head -n 1 | tr -d '[:space:]')"
  if [ "$QUERY_RC" -eq 0 ] && [ -n "$QUERY_VER" ] && [ -z "$(printf '%%s' "$QUERY_VER" | tr -d '0-9.')" ]; then
    DRIVER_OK="true"
    ACTUAL_DRIVER="$QUERY_VER"
  fi
  SMI_HEADER="$($TIMEOUT_CMD "$SMI_BIN" 2>/dev/null)" || SMI_HEADER=""
  ACTUAL_RUNTIME="$(printf '%%s\n' "$SMI_HEADER" | sed -n 's/.*CUDA Version: *\([0-9][0-9.]*\).*/\1/p' | head -n 1 | tr -d '[:space:]')"
fi

if [ -z "$ACTUAL_DRIVER" ] && [ -r /proc/driver/nvidia/version ]; then
  # Diagnostics only (DRIVER_OK stays false): the kernel module version helps
  # explain a user-space failure. The first dotted number on the NVRM line is
  # the version for both proprietary and open kernel modules.
  ACTUAL_DRIVER="$(grep -o '[0-9][0-9]*\.[0-9][0-9.]*' /proc/driver/nvidia/version | head -n 1)"
fi

echo "%s$RUNTIME_OK"
echo "%s$DRIVER_OK"
echo "%s$ACTUAL_DRIVER"
echo "%s$ACTUAL_RUNTIME"`,
	driverRuntimeConfigOKPrefix,
	driverRuntimeFunctionalPrefix,
	driverRuntimeActualDriverPrefix,
	driverRuntimeActualRuntimePrefix,
)

// PlatformDriverRuntimeInfo captures the accelerator driver and container
// runtime state a platform advertises for one node.
type PlatformDriverRuntimeInfo struct {
	// Mechanism is platformMechanismDRA when DriverVersion came from DRA
	// ResourceSlice device attributes, or platformMechanismNodeMetadata when
	// Node labels/annotations were consulted (whether or not they carried a
	// version).
	Mechanism string
	NodeName  string
	// DriverVersion and RuntimeVersion are empty when the platform does not
	// advertise them. The *Key fields name the attribute, label, or
	// annotation each value was read from.
	DriverVersion     string
	DriverVersionKey  string
	RuntimeVersion    string
	RuntimeVersionKey string
	// PresenceKey is the node label marking the node as carrying the
	// accelerator (e.g. nvidia.com/gpu.present), if any.
	PresenceKey string
	// ContainerRuntimeVersion is Node.Status.NodeInfo.ContainerRuntimeVersion
	// (e.g. "containerd://2.2.0").
	ContainerRuntimeVersion string
	// RuntimeClassName is the accelerator RuntimeClass found in the cluster, if any.
	RuntimeClassName string
}

// DriverRuntimeProbeResults captures what an accelerator-requesting container
// running a vanilla image observed on its node.
type DriverRuntimeProbeResults struct {
	AcceleratorCount     int64
	HasAcceleratorCount  bool
	RuntimeConfigOK      bool
	DriverFunctional     bool
	ActualDriverVersion  string
	ActualRuntimeVersion string
	RawLogs              string
}

// TestAcceleratorDriverRuntimeManagement verifies KAR-0001 (Accelerator Driver
// & Runtime Management): the platform provides a verifiable mechanism for
// ensuring that compatible accelerator drivers and corresponding container
// runtime configurations are correctly installed on nodes with accelerators,
// using DRA for verification once the accelerator exposes version
// information there.
//
// Interpretation: "driver version" is the accelerator kernel driver version;
// "runtime version" is the accelerator runtime API version the installed
// driver supports (for NVIDIA: cudaDriverVersion in the DRA driver's
// ResourceSlices, nvidia.com/cuda.runtime-version.full from GPU Feature
// Discovery, "CUDA Version" in nvidia-smi). The container runtime
// configuration itself (CDI spec or runtime hook) is verified empirically: an
// accelerator-requesting container built from a vanilla OS image must receive
// the device nodes and the host driver libraries and tools, and those must
// work against the kernel driver.
//
//  1. VerifiableMechanismExposed resolves what the platform advertises for
//     the accelerator node: DRA ResourceSlice device attributes first, Node
//     labels/annotations otherwise, plus the node's container runtime and the
//     accelerator RuntimeClass. Missing version metadata is a WARNING, not a
//     failure; the node must report a container runtime.
//  2. DriverAndRuntimeCompatibility runs the vendor probe in a vanilla image
//     with one accelerator granted and requires the expected device count,
//     injected tools (RUNTIME_CONFIG_OK), a functional driver
//     (DRIVER_FUNCTIONAL), and, where the platform advertises versions, that
//     the observed versions match them. The check is point-in-time and covers
//     the node the probe Pod landed on.
//
// Per the KAR's Conformance Strategy, an unrecognized -accelerator-type logs
// "RESULT: STATUS=UNKNOWN" and skips so the platform can supply manual
// verification evidence.
//
// Ref: https://github.com/kubernetes-sigs/ai-conformance/tree/main/kars/0001-accelerator-driver-runtime-management
func TestAcceleratorDriverRuntimeManagement(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if !flag.Parsed() {
		flag.Parse()
	}

	cfg, err := lookupAcceleratorConfig(*acceleratorType)
	if err != nil {
		t.Log(driverRuntimeUnknownStatusLine)
		t.Skipf("accelerator type %q is not covered by the automated KAR-0001 test; manual verification is required: %v", *acceleratorType, err)
	}

	clientset := getClientset(t)
	ctx := context.Background()

	mode, acceleratorNode, err := detectAllocationMode(ctx, clientset, *allocationMode, cfg, t.Logf)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %v", err)
	}
	t.Logf("Accelerator driver & runtime management test running with allocation mode: %s (accelerator node: %s)", mode, acceleratorNode)

	namespace := randomNamespaceName("ai-conformance-driver-runtime")
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: %v. Please ensure namespace %s is terminated manually to avoid resource leaks.", err, namespace)
		}
	})
	setupTestEnvironment(ctx, t, clientset, namespace, mode, cfg)

	// platformInfo is resolved at parent level by the first subtest and
	// refined by the second against the node its probe Pod actually landed
	// on. The subtests run sequentially in declaration order (neither calls
	// t.Parallel), and the second re-resolves when run alone under -run
	// filtering.
	var platformInfo PlatformDriverRuntimeInfo
	var mechanismVerified bool

	t.Run("VerifiableMechanismExposed", func(t *testing.T) {
		info, err := inspectPlatformDriverRuntimeMechanism(ctx, clientset, acceleratorNode, mode, cfg, t.Logf)
		if err != nil {
			t.Fatalf("Platform failed the KAR-0001 verifiable mechanism check on node %s (mode=%s): %v", acceleratorNode, mode, err)
		}
		platformInfo = info
		mechanismVerified = true
		t.Logf("PASS: node %s advertises via %s: driver=%q [%s], runtime=%q [%s], presence=[%s], containerRuntime=%s, runtimeClass=%q",
			info.NodeName, info.Mechanism, info.DriverVersion, info.DriverVersionKey, info.RuntimeVersion, info.RuntimeVersionKey,
			info.PresenceKey, info.ContainerRuntimeVersion, info.RuntimeClassName)
	})

	t.Run("DriverAndRuntimeCompatibility", func(t *testing.T) {
		podName := "driver-runtime-probe-pod"
		var pod *corev1.Pod
		t.Cleanup(func() {
			if err := deletePodAndWait(ctx, clientset, namespace, podName, pod); err != nil {
				t.Errorf("Cleanup of Pod %s incomplete; subsequent accelerator tests may race its device/claim release: %v", podName, err)
			}
		})

		// nodeName stays empty so kube-scheduler places the Pod: pinning via
		// pod.Spec.NodeName bypasses the scheduler's dynamicresources plugin
		// and leaves a DRA ResourceClaim unallocated.
		container := driverRuntimeProbingContainer("prober", *driverRuntimeImage, cfg)
		pod = runTestPod(ctx, t, clientset, namespace, podName, []corev1.Container{container},
			testPodConfig{grantAccelerator: true, mode: mode, cfg: cfg})

		targetNode := acceleratorNode
		if pod.Spec.NodeName != "" {
			targetNode = pod.Spec.NodeName
		}
		if !mechanismVerified || platformInfo.NodeName != targetNode {
			info, err := inspectPlatformDriverRuntimeMechanism(ctx, clientset, targetNode, mode, cfg, t.Logf)
			if err != nil {
				t.Fatalf("Failed to resolve the platform driver/runtime mechanism on scheduled node %s: %v", targetNode, err)
			}
			platformInfo = info
		}

		probeResults, err := waitForDriverRuntimeProbeResults(ctx, clientset, namespace, podName, "prober", driverRuntimeLogPollTimeout)
		if err != nil {
			t.Fatalf("Failed to collect driver/runtime probe results from Pod %s/%s: %v\nRaw logs:\n%s",
				namespace, podName, err, probeResults.RawLogs)
		}
		if err := verifyDriverRuntimeCompatibility(platformInfo, probeResults, requestedAcceleratorCount); err != nil {
			t.Fatalf("FAIL: %v\nRaw container logs:\n%s", err, probeResults.RawLogs)
		}
		t.Logf("PASS: container runtime configuration and accelerator driver verified on node %s (actualDriver=%s, actualRuntime=%s; advertised via %s: driver=%q, runtime=%q)",
			targetNode, probeResults.ActualDriverVersion, probeResults.ActualRuntimeVersion,
			platformInfo.Mechanism, platformInfo.DriverVersion, platformInfo.RuntimeVersion)
	})
}

// inspectPlatformDriverRuntimeMechanism resolves what the platform advertises
// about the accelerator driver and container runtime on nodeName. DRA
// ResourceSlice device attributes are preferred in every allocation mode
// (KAR-0001: the platform should use DRA once the accelerator exposes version
// information there); Node labels/annotations are the fallback. Missing
// version metadata is reported through logf as a WARNING rather than failing,
// because the in-container probe still verifies the installed stack. The node
// must report a container runtime, and DRA devices on one node must agree on
// their versions.
func inspectPlatformDriverRuntimeMechanism(
	ctx context.Context,
	c kubernetes.Interface,
	nodeName string,
	mode string,
	cfg AcceleratorConfig,
	logf func(format string, args ...any),
) (PlatformDriverRuntimeInfo, error) {
	node, err := c.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return PlatformDriverRuntimeInfo{}, fmt.Errorf("failed to get accelerator node %s: %w", nodeName, err)
	}

	info := PlatformDriverRuntimeInfo{
		NodeName:                node.Name,
		ContainerRuntimeVersion: strings.TrimSpace(node.Status.NodeInfo.ContainerRuntimeVersion),
		RuntimeClassName:        detectAcceleratorRuntimeClass(ctx, c, cfg),
	}
	if info.ContainerRuntimeVersion == "" || !strings.Contains(info.ContainerRuntimeVersion, "://") {
		return PlatformDriverRuntimeInfo{}, fmt.Errorf("node %s does not report a valid container runtime in status.nodeInfo.containerRuntimeVersion (got %q)", node.Name, info.ContainerRuntimeVersion)
	}

	devices, err := nodeDRADevices(ctx, c, node, cfg)
	switch {
	case err != nil && mode == allocationModeDRA:
		return PlatformDriverRuntimeInfo{}, err
	case err == nil && len(devices) == 0 && mode == allocationModeDRA:
		// Allocation-mode detection proved that this node backs usable slices.
		return PlatformDriverRuntimeInfo{}, fmt.Errorf("no usable DRA ResourceSlice devices found for driver %s on node %s", cfg.DRADriver, node.Name)
	case err == nil && len(devices) > 0:
		versions, err := draDeviceVersions(devices, cfg)
		if err != nil {
			return PlatformDriverRuntimeInfo{}, fmt.Errorf("node %s: %w", node.Name, err)
		}
		if versions.driver != "" {
			info.Mechanism = platformMechanismDRA
			info.DriverVersion, info.DriverVersionKey = versions.driver, versions.driverKey
			info.RuntimeVersion, info.RuntimeVersionKey = versions.runtime, versions.runtimeKey
			if info.RuntimeVersion == "" {
				logf("WARNING: DRA devices of driver %s on node %s expose no runtime version attribute (checked %v); the runtime version cross-check is skipped",
					cfg.DRADriver, node.Name, cfg.DRARuntimeVersionAttributes)
			}
			return info, nil
		}
		logf("WARNING: DRA devices of driver %s on node %s expose no driver version attribute (checked %v); falling back to node metadata",
			cfg.DRADriver, node.Name, cfg.DRADriverVersionAttributes)
	}
	// Otherwise the cluster does not serve the DRA API or advertises no slices
	// for this node (expected with a device plugin): node metadata is the
	// only source.

	info.Mechanism = platformMechanismNodeMetadata
	info.DriverVersionKey, info.DriverVersion = findNodeMetadataValue(node, cfg.NodeDriverVersionLabels)
	info.RuntimeVersionKey, info.RuntimeVersion = findNodeMetadataValue(node, cfg.NodeRuntimeVersionLabels)
	info.PresenceKey, _ = findNodeMetadataValue(node, cfg.NodePresenceLabels)
	if info.DriverVersion == "" {
		logf("WARNING: node %s advertises no accelerator driver version (checked labels/annotations %v); driver and runtime compatibility is verified only by the in-container probe",
			node.Name, cfg.NodeDriverVersionLabels)
	}
	return info, nil
}

// nodeDRADevices returns the allocatable devices that complete,
// current-generation ResourceSlices of cfg.DRADriver advertise for node.
func nodeDRADevices(ctx context.Context, c kubernetes.Interface, node *corev1.Node, cfg AcceleratorConfig) ([]resourcev1.Device, error) {
	slices, err := c.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list DRA ResourceSlices: %w", err)
	}
	usable := map[string]corev1.Node{node.Name: *node}
	var devices []resourcev1.Device
	for _, slice := range completePoolSlices(slices.Items, cfg.DRADriver) {
		if _, ok := sliceEligibleNode(slice, usable, cfg); !ok {
			continue
		}
		perDevice := slice.Spec.PerDeviceNodeSelection != nil && *slice.Spec.PerDeviceNodeSelection
		for _, device := range slice.Spec.Devices {
			if deviceTaintBlocked(&device) {
				continue
			}
			if perDevice {
				if _, ok := deviceEligibleNode(&device, usable, cfg); !ok {
					continue
				}
			}
			devices = append(devices, device)
		}
	}
	return devices, nil
}

type draVersions struct {
	driver, driverKey   string
	runtime, runtimeKey string
}

// draDeviceVersions reads the driver and runtime version attributes of a
// node's devices. Either may be absent; devices that do expose a value must
// agree, since they share one node driver.
func draDeviceVersions(devices []resourcev1.Device, cfg AcceleratorConfig) (draVersions, error) {
	var v draVersions
	for _, device := range devices {
		if key, ver := findDeviceAttributeString(device.Attributes, cfg.DRADriverVersionAttributes); ver != "" {
			if v.driver == "" {
				v.driver, v.driverKey = ver, key
			} else if !versionsCompatible(v.driver, ver) {
				return draVersions{}, fmt.Errorf("inconsistent DRA driver versions: device %s reports %q, an earlier device reported %q", device.Name, ver, v.driver)
			}
		}
		if key, ver := findDeviceAttributeString(device.Attributes, cfg.DRARuntimeVersionAttributes); ver != "" {
			if v.runtime == "" {
				v.runtime, v.runtimeKey = ver, key
			} else if !versionsCompatible(v.runtime, ver) {
				return draVersions{}, fmt.Errorf("inconsistent DRA runtime versions: device %s reports %q, an earlier device reported %q", device.Name, ver, v.runtime)
			}
		}
	}
	return v, nil
}

// findDeviceAttributeString returns the first of keys present in attrs along
// with its value rendered as a string (version, string, or integer).
func findDeviceAttributeString(attrs map[resourcev1.QualifiedName]resourcev1.DeviceAttribute, keys []string) (string, string) {
	for _, k := range keys {
		attr, ok := attrs[resourcev1.QualifiedName(k)]
		if !ok {
			continue
		}
		if attr.VersionValue != nil && strings.TrimSpace(*attr.VersionValue) != "" {
			return k, strings.TrimSpace(*attr.VersionValue)
		}
		if attr.StringValue != nil && strings.TrimSpace(*attr.StringValue) != "" {
			return k, strings.TrimSpace(*attr.StringValue)
		}
		if attr.IntValue != nil {
			return k, strconv.FormatInt(*attr.IntValue, 10)
		}
	}
	return "", ""
}

// findNodeMetadataValue returns the first of keys present as a non-empty node
// label or annotation, along with its value.
func findNodeMetadataValue(node *corev1.Node, keys []string) (string, string) {
	for _, k := range keys {
		if v, ok := node.Labels[k]; ok && strings.TrimSpace(v) != "" {
			return k, strings.TrimSpace(v)
		}
		if v, ok := node.Annotations[k]; ok && strings.TrimSpace(v) != "" {
			return k, strings.TrimSpace(v)
		}
	}
	return "", ""
}

func detectAcceleratorRuntimeClass(ctx context.Context, c kubernetes.Interface, cfg AcceleratorConfig) string {
	for _, rcName := range cfg.RuntimeClassNames {
		if rc, err := c.NodeV1().RuntimeClasses().Get(ctx, rcName, metav1.GetOptions{}); err == nil && rc != nil {
			return rc.Name
		}
	}
	return ""
}

// driverRuntimeProbeCommand composes the shared device-count probe with the
// vendor's driver probe script (see AcceleratorConfig.DriverProbeScript).
func driverRuntimeProbeCommand(devicePattern, probeScript string) string {
	return acceleratorProbeCommand(devicePattern) + "\n" + probeScript
}

func driverRuntimeProbingContainer(name, image string, cfg AcceleratorConfig) corev1.Container {
	return corev1.Container{
		Name:    name,
		Image:   image,
		Command: []string{"/bin/sh", "-c"},
		Args: []string{
			driverRuntimeProbeCommand(cfg.DevicePattern, cfg.DriverProbeScript) + "\nsleep 3600",
		},
	}
}

// waitForDriverRuntimeProbeResults polls the container's logs until every
// RESULT line has been printed or timeout elapses, returning the latest parse
// either way so failures can show what was seen.
func waitForDriverRuntimeProbeResults(
	ctx context.Context,
	c kubernetes.Interface,
	namespace, podName, containerName string,
	timeout time.Duration,
) (DriverRuntimeProbeResults, error) {
	var latest DriverRuntimeProbeResults
	var lastErr error

	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		rawLogs, err := c.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{Container: containerName}).DoRaw(ctx)
		if err != nil {
			// BadRequest is returned while the container is still being created.
			if isRetryableAPIError(err) || apierrors.IsBadRequest(err) {
				lastErr = err
				return false, nil
			}
			return false, err
		}
		parsed, complete := parseDriverRuntimeProbeLogs(string(rawLogs))
		latest = parsed
		return complete, nil
	})
	if err != nil {
		return latest, fmt.Errorf("timed out waiting for complete driver/runtime probe logs%s: %w", lastAPIErrorSuffix(lastErr), err)
	}
	return latest, nil
}

func parseDriverRuntimeProbeLogs(logs string) (DriverRuntimeProbeResults, bool) {
	res := DriverRuntimeProbeResults{RawLogs: logs}
	var sawRuntimeOK, sawDriverOK, sawActualDriver, sawActualRuntime bool

	for _, rawLine := range strings.Split(logs, "\n") {
		line := strings.TrimSpace(rawLine)
		switch {
		case strings.HasPrefix(line, acceleratorCountResultPrefix):
			val := strings.TrimSpace(strings.TrimPrefix(line, acceleratorCountResultPrefix))
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				res.AcceleratorCount = n
				res.HasAcceleratorCount = true
			}
		case strings.HasPrefix(line, driverRuntimeConfigOKPrefix):
			res.RuntimeConfigOK = strings.TrimSpace(strings.TrimPrefix(line, driverRuntimeConfigOKPrefix)) == "true"
			sawRuntimeOK = true
		case strings.HasPrefix(line, driverRuntimeFunctionalPrefix):
			res.DriverFunctional = strings.TrimSpace(strings.TrimPrefix(line, driverRuntimeFunctionalPrefix)) == "true"
			sawDriverOK = true
		case strings.HasPrefix(line, driverRuntimeActualDriverPrefix):
			res.ActualDriverVersion = strings.TrimSpace(strings.TrimPrefix(line, driverRuntimeActualDriverPrefix))
			sawActualDriver = true
		case strings.HasPrefix(line, driverRuntimeActualRuntimePrefix):
			res.ActualRuntimeVersion = strings.TrimSpace(strings.TrimPrefix(line, driverRuntimeActualRuntimePrefix))
			sawActualRuntime = true
		}
	}

	complete := res.HasAcceleratorCount && sawRuntimeOK && sawDriverOK && sawActualDriver && sawActualRuntime
	return res, complete
}

// verifyDriverRuntimeCompatibility checks that the container saw exactly the
// expected accelerator devices, that the container runtime configuration
// injected the vendor tools (RuntimeConfigOK), that the driver initialized and
// reported its versions (DriverFunctional), and that the observed versions
// match whatever the platform's mechanism advertises.
func verifyDriverRuntimeCompatibility(
	platformInfo PlatformDriverRuntimeInfo,
	probe DriverRuntimeProbeResults,
	expectedCount int64,
) error {
	if !probe.HasAcceleratorCount || probe.AcceleratorCount != expectedCount {
		return fmt.Errorf("container saw %d accelerator device(s), expected %d", probe.AcceleratorCount, expectedCount)
	}
	if !probe.RuntimeConfigOK {
		return fmt.Errorf("container runtime configuration failed to inject the accelerator driver tools/libraries (e.g. nvidia-smi via CDI or a runtime hook) into the container")
	}
	if !probe.DriverFunctional || probe.ActualDriverVersion == "" {
		return fmt.Errorf("accelerator driver failed to initialize or report a valid driver version inside the container (DRIVER_FUNCTIONAL=%t, ACTUAL_DRIVER_VERSION=%q)",
			probe.DriverFunctional, probe.ActualDriverVersion)
	}
	if probe.ActualRuntimeVersion == "" {
		return fmt.Errorf("accelerator driver did not report a runtime API version inside the container (ACTUAL_RUNTIME_VERSION is empty)")
	}

	if platformInfo.DriverVersion != "" && !versionsCompatible(platformInfo.DriverVersion, probe.ActualDriverVersion) {
		return fmt.Errorf("platform %s mechanism advertises driver version %q (%s), which does not match the driver version %q observed on the node",
			platformInfo.Mechanism, platformInfo.DriverVersion, platformInfo.DriverVersionKey, probe.ActualDriverVersion)
	}
	if platformInfo.RuntimeVersion != "" && !versionsCompatible(platformInfo.RuntimeVersion, probe.ActualRuntimeVersion) {
		return fmt.Errorf("platform %s mechanism advertises runtime version %q (%s), which does not match the runtime version %q observed on the node",
			platformInfo.Mechanism, platformInfo.RuntimeVersion, platformInfo.RuntimeVersionKey, probe.ActualRuntimeVersion)
	}
	return nil
}

// versionsCompatible reports whether two version strings describe the same
// version at the precision they share: numeric segments are compared as
// integers, so "580.65.06" matches "580.65.6", and a shorter version matches a
// longer one when all shared segments agree ("12.9" and "12.9.0", "580" and
// "580.65.06", in either order). Empty strings never match; non-numeric
// strings must be equal ignoring case.
func versionsCompatible(a, b string) bool {
	aParts, ok1 := parseNumericVersionParts(a)
	bParts, ok2 := parseNumericVersionParts(b)
	if !ok1 || !ok2 {
		a, b = strings.TrimSpace(a), strings.TrimSpace(b)
		return a != "" && strings.EqualFold(a, b)
	}
	for i := 0; i < min(len(aParts), len(bParts)); i++ {
		if aParts[i] != bParts[i] {
			return false
		}
	}
	return true
}

// parseNumericVersionParts splits "v1.2.3-rc1+meta" style strings into their
// numeric segments [1 2 3]; it reports false for anything non-numeric.
func parseNumericVersionParts(v string) ([]int, bool) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	if idx := strings.IndexAny(v, "-+"); idx >= 0 {
		v = v[:idx]
	}
	if v == "" {
		return nil, false
	}
	rawParts := strings.Split(v, ".")
	parts := make([]int, 0, len(rawParts))
	for _, p := range rawParts {
		n, err := strconv.Atoi(p)
		if p == "" || err != nil || n < 0 {
			return nil, false
		}
		parts = append(parts, n)
	}
	return parts, true
}
