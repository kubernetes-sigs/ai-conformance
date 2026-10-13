package conformance

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"testing"

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
	driverRuntimeConfigOKPrefix     = "RESULT: RUNTIME_CONFIG_OK="
	driverRuntimeFunctionalPrefix   = "RESULT: DRIVER_FUNCTIONAL="
	driverRuntimeActualDriverPrefix = "RESULT: ACTUAL_DRIVER_VERSION="
	// driverRuntimeUnknownStatusLine is logged verbatim when -accelerator-type
	// names a variant the suite does not recognize (KAR-0001 Conformance
	// Strategy: report Unknown rather than fail).
	driverRuntimeUnknownStatusLine = "RESULT: STATUS=UNKNOWN"

	// Mechanisms through which a platform advertises the accelerator driver
	// installed on a node.
	platformMechanismDRA          = "dra"
	platformMechanismNodeMetadata = "node-metadata"
)

var driverRuntimeImage *string

func init() {
	registerFlagGroup("driver-runtime", "Accelerator Driver & Runtime Management Flags (TestAcceleratorDriverRuntimeManagement)")
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
  else
    echo "nvidia-smi exit status $QUERY_RC, output: $QUERY_OUT"
  fi
fi
if [ "$DRIVER_OK" != "true" ] && [ -r /proc/driver/nvidia/version ]; then
  # Diagnostics only: the kernel module version helps explain a user-space failure.
  cat /proc/driver/nvidia/version
fi

echo "%s$RUNTIME_OK"
echo "%s$DRIVER_OK"
echo "%s$ACTUAL_DRIVER"`,
	driverRuntimeConfigOKPrefix,
	driverRuntimeFunctionalPrefix,
	driverRuntimeActualDriverPrefix,
)

// PlatformDriverRuntimeInfo captures the accelerator driver version a platform
// advertises for one node and where it was read from.
type PlatformDriverRuntimeInfo struct {
	// Mechanism is platformMechanismDRA when DriverVersion came from DRA
	// ResourceSlice device attributes, or platformMechanismNodeMetadata when
	// Node labels/annotations were consulted (whether or not they carried a
	// version).
	Mechanism string
	NodeName  string
	// DriverVersion is empty when the platform does not advertise one.
	// DriverVersionKey names the attribute, label, or annotation it was read
	// from.
	DriverVersion    string
	DriverVersionKey string
}

// DriverRuntimeProbeResults captures what an accelerator-requesting container
// running a vanilla image observed on its node.
type DriverRuntimeProbeResults struct {
	AcceleratorCount    int64
	HasAcceleratorCount bool
	RuntimeConfigOK     bool
	DriverFunctional    bool
	ActualDriverVersion string
	RawLogs             string
}

// TestAcceleratorDriverRuntimeManagement verifies KAR-0001 (Accelerator Driver
// & Runtime Management): the platform provides a verifiable mechanism for
// ensuring that compatible accelerator drivers and corresponding container
// runtime configurations are correctly installed on nodes with accelerators,
// using DRA for verification once the accelerator exposes version
// information there.
//
// The test schedules an accelerator-requesting Pod built from a vanilla OS
// image and then checks the node it landed on:
//
//  1. VerifiableMechanismExposed resolves the driver version the platform
//     advertises for that node: DRA ResourceSlice device attributes first,
//     Node labels/annotations otherwise. A platform that advertises nothing
//     gets a WARNING, not a failure; the in-container probe still verifies
//     the installed stack.
//  2. DriverAndRuntimeCompatibility reads the Pod's probe output and requires
//     the expected device count, injected vendor tools (RUNTIME_CONFIG_OK,
//     which only the container runtime configuration can provide to a
//     vanilla image), a functional driver (DRIVER_FUNCTIONAL), and, where
//     the platform advertises a version, that the observed driver version
//     matches it.
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

	mode, _, err := detectAllocationMode(ctx, clientset, *allocationMode, cfg, t.Logf)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %v", err)
	}
	t.Logf("Accelerator driver & runtime management test running with allocation mode: %s", mode)

	namespace := randomNamespaceName("ai-conformance-driver-runtime")
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: %v. Please ensure namespace %s is terminated manually to avoid resource leaks.", err, namespace)
		}
	})
	setupTestEnvironment(ctx, t, clientset, namespace, mode, cfg)

	// nodeName stays empty so kube-scheduler places the Pod: pinning via
	// pod.Spec.NodeName bypasses the scheduler's dynamicresources plugin and
	// leaves a DRA ResourceClaim unallocated. Both checks below run against
	// the node the Pod landed on.
	const podName, containerName = "driver-runtime-probe-pod", "prober"
	container := driverRuntimeProbingContainer(containerName, *driverRuntimeImage, cfg)
	pod := runTestPod(ctx, t, clientset, namespace, podName, []corev1.Container{container},
		testPodConfig{grantAccelerator: true, mode: mode, cfg: cfg})
	targetNode := pod.Spec.NodeName

	var platformInfo PlatformDriverRuntimeInfo
	t.Run("VerifiableMechanismExposed", func(t *testing.T) {
		info, err := inspectPlatformDriverRuntimeMechanism(ctx, clientset, targetNode, mode, cfg, t.Logf)
		if err != nil {
			t.Fatalf("Platform failed the KAR-0001 verifiable mechanism check on node %s (mode=%s): %v", targetNode, mode, err)
		}
		platformInfo = info
		if info.DriverVersion == "" {
			t.Logf("PASS: node %s advertises no driver version (%s); the in-container probe alone verifies the installed stack", info.NodeName, info.Mechanism)
		} else {
			t.Logf("PASS: node %s advertises driver version %q via %s [%s]", info.NodeName, info.DriverVersion, info.Mechanism, info.DriverVersionKey)
		}
	})

	t.Run("DriverAndRuntimeCompatibility", func(t *testing.T) {
		probeResults, err := waitForDriverRuntimeProbeResults(ctx, clientset, namespace, podName, containerName)
		if err != nil {
			t.Fatalf("Failed to collect driver/runtime probe results from Pod %s/%s: %v\nRaw logs:\n%s",
				namespace, podName, err, probeResults.RawLogs)
		}
		if err := verifyDriverRuntimeCompatibility(platformInfo, probeResults, requestedAcceleratorCount); err != nil {
			t.Fatalf("FAIL: %v\nRaw container logs:\n%s", err, probeResults.RawLogs)
		}
		t.Logf("PASS: container runtime configuration and accelerator driver verified on node %s (observed driver=%s; advertised via %s: %q)",
			targetNode, probeResults.ActualDriverVersion, platformInfo.Mechanism, platformInfo.DriverVersion)
	})
}

// inspectPlatformDriverRuntimeMechanism resolves the accelerator driver version
// the platform advertises for nodeName. DRA ResourceSlice device attributes are
// preferred in every allocation mode (KAR-0001: the platform should use DRA
// once the accelerator exposes version information there); Node
// labels/annotations are the fallback. A platform that advertises nothing is
// reported through logf as a WARNING rather than failing, because the
// in-container probe still verifies the installed stack.
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
	info := PlatformDriverRuntimeInfo{NodeName: node.Name}

	devices, err := nodeDRADevices(ctx, c, node, cfg)
	switch {
	case err != nil && mode == allocationModeDRA:
		return PlatformDriverRuntimeInfo{}, err
	case err == nil && len(devices) == 0 && mode == allocationModeDRA:
		// The probe Pod's claim was allocated from this node, so it must back slices.
		return PlatformDriverRuntimeInfo{}, fmt.Errorf("no usable DRA ResourceSlice devices found for driver %s on node %s", cfg.DRADriver, node.Name)
	case err == nil && len(devices) > 0:
		for _, device := range devices {
			if key, ver := findDeviceAttributeString(device.Attributes, cfg.DRADriverVersionAttributes); ver != "" {
				info.Mechanism = platformMechanismDRA
				info.DriverVersionKey, info.DriverVersion = key, ver
				return info, nil
			}
		}
		logf("WARNING: DRA devices of driver %s on node %s expose no driver version attribute (checked %v); falling back to node metadata",
			cfg.DRADriver, node.Name, cfg.DRADriverVersionAttributes)
	}
	// Otherwise the cluster does not serve the DRA API or advertises no slices
	// for this node (expected with a device plugin): node metadata is the
	// only source.

	info.Mechanism = platformMechanismNodeMetadata
	info.DriverVersionKey, info.DriverVersion = findNodeMetadataValue(node, cfg.NodeDriverVersionLabels)
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

// findDeviceAttributeString returns the first of keys present in attrs along
// with its value (a version or string attribute).
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
// RESULT line has been printed or acceleratorLogPollTimeout elapses (nvidia-smi
// can take tens of seconds on a cold GPU), returning the latest parse either
// way so failures can show what was seen.
func waitForDriverRuntimeProbeResults(ctx context.Context, c kubernetes.Interface, namespace, podName, containerName string) (DriverRuntimeProbeResults, error) {
	var latest DriverRuntimeProbeResults
	var lastErr error

	err := wait.PollUntilContextTimeout(ctx, acceleratorLogPollInterval, acceleratorLogPollTimeout, true, func(ctx context.Context) (bool, error) {
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
		return latest, fmt.Errorf("timed out waiting for complete driver/runtime probe logs; containerStatus=%s%s: %w",
			containerStatusJSON(ctx, c, namespace, podName, containerName), lastAPIErrorSuffix(lastErr), err)
	}
	return latest, nil
}

func parseDriverRuntimeProbeLogs(logs string) (DriverRuntimeProbeResults, bool) {
	res := DriverRuntimeProbeResults{RawLogs: logs}
	var sawRuntimeOK, sawDriverOK, sawActualDriver bool

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
		}
	}

	complete := res.HasAcceleratorCount && sawRuntimeOK && sawDriverOK && sawActualDriver
	return res, complete
}

// verifyDriverRuntimeCompatibility checks that the container saw exactly the
// expected accelerator devices, that the container runtime configuration
// injected the vendor tools (RuntimeConfigOK), that the driver initialized and
// reported its version (DriverFunctional), and that the observed version
// matches whatever the platform's mechanism advertises.
func verifyDriverRuntimeCompatibility(platformInfo PlatformDriverRuntimeInfo, probe DriverRuntimeProbeResults, expectedCount int64) error {
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
	if platformInfo.DriverVersion != "" && !versionsCompatible(platformInfo.DriverVersion, probe.ActualDriverVersion) {
		return fmt.Errorf("platform %s mechanism advertises driver version %q (%s), which does not match the driver version %q observed on the node",
			platformInfo.Mechanism, platformInfo.DriverVersion, platformInfo.DriverVersionKey, probe.ActualDriverVersion)
	}
	return nil
}

// versionsCompatible reports whether two version strings describe the same
// version at the precision they share: numeric segments are compared as
// integers, so "580.65.06" matches "580.65.6", and a shorter version matches a
// longer one when all shared segments agree ("580.65" and "580.65.06", in
// either order). Empty strings never match; non-numeric strings must be equal
// ignoring case.
func versionsCompatible(a, b string) bool {
	aParts, ok1 := parseNumericVersionParts(a)
	bParts, ok2 := parseNumericVersionParts(b)
	if !ok1 || !ok2 {
		a, b = strings.TrimSpace(a), strings.TrimSpace(b)
		return a != "" && strings.EqualFold(a, b)
	}
	// Only the shared prefix is compared on purpose: a platform may advertise
	// fewer segments than the driver reports (e.g. "580.65" vs "580.65.06").
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
