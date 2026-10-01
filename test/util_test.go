package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
)

type AcceleratorConfig struct {
	// DeviceClass is the DRA DeviceClass that test ResourceClaims request
	// devices from.
	DeviceClass string
	// DRADriver is the driver name expected in ResourceSlice.Spec.Driver for
	// slices that back DeviceClass. In general a DeviceClass selects devices
	// via CEL selectors and its name is not required to match any driver
	// name; this per-vendor mapping makes the association explicit instead
	// of assuming name equality. (Full CEL selector evaluation is out of
	// scope for the environment probe.) For NVIDIA both happen to be
	// "gpu.nvidia.com".
	DRADriver string
	// ExtendedResource is the device-plugin extended resource name for whole
	// accelerators.
	ExtendedResource string
	TaintKey         string
	// DevicePattern is a shell glob that matches allocatable accelerator
	// device nodes while excluding auxiliary and control devices.
	DevicePattern string
}

// Allocation modes for granting accelerators to test pods. The conformance
// requirement permits mediation by "device plugin or DRA", so both mechanisms
// are supported.
const (
	allocationModeAuto                 = "auto"
	allocationModeDRA                  = "dra"
	allocationModeDevicePlugin         = "device-plugin"
	requestedAcceleratorCount    int64 = 1
	acceleratorCountResultPrefix       = "RESULT: ACCELERATOR_COUNT="
	acceleratorLogPollInterval         = 2 * time.Second
	acceleratorLogPollTimeout          = 90 * time.Second
)

var (
	kubeconfig             *string
	acceleratorType        *string
	allocationMode         *string
	gangSchedulerNamespace *string
	gangJobLabels          *string
	gangNegativeWindow     *time.Duration
	acceleratorConfigs     = map[string]AcceleratorConfig{
		"nvidia": {
			DeviceClass:      "gpu.nvidia.com",
			DRADriver:        "gpu.nvidia.com",
			ExtendedResource: "nvidia.com/gpu",
			TaintKey:         "nvidia.com/gpu",
			DevicePattern:    "/dev/nvidia[0-9]*",
		},
		// Add other vendors here
	}
	testResourceTemplateName = "accelerator-claim-template"
	testRequestName          = "single-accelerator"
)

func init() {
	kubeconfig = flag.String(clientcmd.RecommendedConfigPathFlag, "", "absolute path to the kubeconfig file")
	acceleratorType = flag.String("accelerator-type", "nvidia", "The type of accelerator to test. Supported types: 'nvidia' (default). Support for other types is being added.")
	allocationMode = flag.String("allocation-mode", allocationModeAuto,
		"How test pods request accelerators: 'dra' (ResourceClaims), 'device-plugin' (extended resources such as nvidia.com/gpu), or 'auto' (default; prefer DRA when usable, otherwise fall back to the device plugin).")
	gangSchedulerNamespace = flag.String("gang-scheduler-namespace", "",
		"Namespace pre-configured with gang scheduling resources (e.g., LocalQueue). If empty, the test will generate a random namespace.")
	gangJobLabels = flag.String("gang-job-labels", "",
		"Comma-separated key=value labels to apply to the generic gang scheduling Job (e.g. kueue.x-k8s.io/queue-name=e2e-lq).")
	gangNegativeWindow = flag.Duration("gang-negative-window", 30*time.Second,
		"Duration to observe the negative gang scheduling test job to verify no pods are partially scheduled.")
}

// buildRESTConfig loads a *rest.Config from the kubeconfig flag. Shared by
// getClientset and getDynamicClient to avoid duplicating kubeconfig loading.
func buildRESTConfig(t *testing.T) *rest.Config {
	t.Helper()
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if *kubeconfig != "" {
		loadingRules.ExplicitPath = *kubeconfig
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		t.Fatalf("Error building kubeconfig: %v", err)
	}
	return config
}

// getClientset creates a Kubernetes clientset using the kubeconfig flag.
func getClientset(t *testing.T) kubernetes.Interface {
	t.Helper()
	clientset, err := kubernetes.NewForConfig(buildRESTConfig(t))
	if err != nil {
		t.Fatalf("Error creating kubernetes client: %v", err)
	}
	return clientset
}

// getDynamicClient creates a Kubernetes dynamic client using the kubeconfig flag.
func getDynamicClient(t *testing.T) dynamic.Interface {
	t.Helper()
	dynamicClient, err := dynamic.NewForConfig(buildRESTConfig(t))
	if err != nil {
		t.Fatalf("Error creating dynamic client: %v", err)
	}
	return dynamicClient
}

func deleteNamespaceAndWait(ctx context.Context, t *testing.T, c kubernetes.Interface, namespace string) error {
	t.Helper()
	t.Logf("Cleaning up namespace %s...", namespace)
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		err := c.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
		switch {
		case err == nil, apierrors.IsNotFound(err):
			lastAPIError = nil
			return true, nil
		case isRetryableAPIError(err):
			lastAPIError = err
			return false, nil
		default:
			return false, err
		}
	})
	if err != nil {
		return fmt.Errorf("failed to request deletion of namespace %s%s: %w", namespace, lastAPIErrorSuffix(lastAPIError), err)
	}

	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := c.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			lastAPIError = nil
			return true, nil
		case err == nil:
			lastAPIError = nil
			return false, nil
		case isRetryableAPIError(err):
			lastAPIError = err
			return false, nil
		default:
			return false, err
		}
	})
	if err != nil {
		return fmt.Errorf("namespace %s was not deleted before the cleanup deadline%s: %w", namespace, lastAPIErrorSuffix(lastAPIError), err)
	}
	return nil
}

// lookupAcceleratorConfig resolves an -accelerator-type value to its config,
// rejecting unknown types up front so a flag typo reports as a flag error
// rather than a misleading cluster-environment error.
func lookupAcceleratorConfig(name string) (AcceleratorConfig, error) {
	cfg, ok := acceleratorConfigs[name]
	if !ok {
		supported := make([]string, 0, len(acceleratorConfigs))
		for k := range acceleratorConfigs {
			supported = append(supported, k)
		}
		sort.Strings(supported)
		return AcceleratorConfig{}, fmt.Errorf("unsupported accelerator type %q; supported types: %s", name, strings.Join(supported, ", "))
	}
	return cfg, nil
}

// Setup namespace and, for the DRA allocation mode, the ResourceClaimTemplate
func setupTestEnvironment(ctx context.Context, t *testing.T, c kubernetes.Interface, ns, mode string, cfg AcceleratorConfig) {
	if _, err := c.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create namespace: %v", err)
	}

	if mode != allocationModeDRA {
		return
	}

	template := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: testResourceTemplateName, Namespace: ns},
		Spec: resourcev1.ResourceClaimTemplateSpec{
			Spec: resourcev1.ResourceClaimSpec{
				Devices: resourcev1.DeviceClaim{
					Requests: []resourcev1.DeviceRequest{{
						Name: testRequestName,
						Exactly: &resourcev1.ExactDeviceRequest{
							DeviceClassName: cfg.DeviceClass,
							Count:           requestedAcceleratorCount,
						},
					}},
				},
			},
		},
	}

	if _, err := c.ResourceV1().ResourceClaimTemplates(ns).Create(ctx, template, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create resource claim template: %v", err)
	}
}

// resolvedEnvironment describes the allocation mechanism the test will
// exercise and where accelerators live.
type resolvedEnvironment struct {
	mode string
	// acceleratorNode is a Ready, schedulable node known to expose
	// accelerators via the resolved mechanism. Every success path of the
	// detection returns a concrete node, so this is always non-empty after
	// resolveAllocationMode. Resolved at parent level so subtests remain
	// self-contained under -run filtering.
	acceleratorNode string
	cfg             AcceleratorConfig
}

// resolveAllocationMode validates the flags and resolves the requested
// allocation mode against what the cluster actually exposes, failing the test
// with a clear environment error when the requested mechanism is unavailable.
func resolveAllocationMode(ctx context.Context, t *testing.T, c kubernetes.Interface) resolvedEnvironment {
	cfg, err := lookupAcceleratorConfig(*acceleratorType)
	if err != nil {
		t.Fatalf("Invalid -accelerator-type: %v", err)
	}

	mode, node, err := detectAllocationMode(ctx, c, *allocationMode, cfg, t.Logf)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %v", err)
	}
	t.Logf("Secure accelerator access test running with allocation mode: %s (accelerator node: %s)", mode, node)
	return resolvedEnvironment{mode: mode, acceleratorNode: node, cfg: cfg}
}

// detectAllocationMode resolves the requested mode against the cluster and
// returns the mode plus an accelerator node it verified. In 'auto' mode DRA
// is preferred when usable, with the device plugin as fallback; the explicit
// modes return an error when the requested mechanism is unavailable so CI
// runs stay deterministic.
func detectAllocationMode(ctx context.Context, c kubernetes.Interface, requested string, cfg AcceleratorConfig, logf func(format string, args ...any)) (string, string, error) {
	switch requested {
	case allocationModeDRA:
		node, err := checkDRAUsable(ctx, c, cfg, logf)
		if err != nil {
			return "", "", fmt.Errorf("allocation mode 'dra' requested but DRA is not usable: %w", err)
		}
		return allocationModeDRA, node, nil
	case allocationModeDevicePlugin:
		node, err := checkDevicePluginUsable(ctx, c, cfg, logf)
		if err != nil {
			return "", "", fmt.Errorf("allocation mode 'device-plugin' requested but no device-plugin capacity found: %w", err)
		}
		// Strict mode must actually prove device-plugin mediation. With the
		// DRAExtendedResource feature (K8s 1.34+), a DeviceClass can map the
		// same extended resource name to DRA, so an extended-resource request
		// could be satisfied by DRA instead of the device plugin.
		if err := checkExtendedResourceNotDRABacked(ctx, c, cfg); err != nil {
			return "", "", fmt.Errorf("allocation mode 'device-plugin' requested but it would not be deterministic: %w", err)
		}
		return allocationModeDevicePlugin, node, nil
	case allocationModeAuto:
		node, draErr := checkDRAUsable(ctx, c, cfg, logf)
		if draErr == nil {
			logf("Auto-detected allocation mode: dra")
			return allocationModeDRA, node, nil
		}
		logf("DRA not usable (%v); trying device plugin...", draErr)
		node, dpErr := checkDevicePluginUsable(ctx, c, cfg, logf)
		if dpErr == nil {
			// The fallback must be attributable too: a DRA-backed extended
			// resource would make the reported "device-plugin" mode wrong.
			if err := checkExtendedResourceNotDRABacked(ctx, c, cfg); err != nil {
				return "", "", fmt.Errorf("device-plugin fallback would not be deterministic: %w", err)
			}
			logf("Auto-detected allocation mode: device-plugin")
			return allocationModeDevicePlugin, node, nil
		}
		return "", "", fmt.Errorf("no usable accelerator allocation mechanism found. DRA: %v. Device plugin: %v. "+
			"Ensure a DRA driver advertises ResourceSlices or a device plugin advertises allocatable %s on a schedulable node",
			draErr, dpErr, cfg.ExtendedResource)
	default:
		return "", "", fmt.Errorf("invalid -allocation-mode %q; supported values: %q, %q, %q",
			requested, allocationModeAuto, allocationModeDRA, allocationModeDevicePlugin)
	}
}

// usableNodes returns the names of Ready, schedulable nodes.
func usableNodes(ctx context.Context, c kubernetes.Interface) (map[string]corev1.Node, error) {
	nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}
	usable := make(map[string]corev1.Node)
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable || !nodeIsReady(&node) {
			continue
		}
		usable[node.Name] = node
	}
	return usable, nil
}

func nodeIsReady(node *corev1.Node) bool {
	_, ready := nodeutil.GetNodeCondition(&node.Status, corev1.NodeReady)
	return ready != nil && ready.Status == corev1.ConditionTrue
}

// checkDRAUsable verifies that DRA is usable for the accelerator under test:
// the DeviceClass exists and at least one ResourceSlice from the configured
// driver advertises devices reachable from a Ready, schedulable node. It
// returns a concrete eligible node's name. DeviceClass existence alone is not
// sufficient — a stale class without backing ResourceSlices, or slices whose
// only node is down, cannot satisfy claims.
func checkDRAUsable(ctx context.Context, c kubernetes.Interface, cfg AcceleratorConfig, logf func(format string, args ...any)) (string, error) {
	if _, err := c.ResourceV1().DeviceClasses().Get(ctx, cfg.DeviceClass, metav1.GetOptions{}); err != nil {
		return "", fmt.Errorf("failed to get DeviceClass %s (is the DRA API resource.k8s.io/v1 enabled?): %w", cfg.DeviceClass, err)
	}

	slices, err := c.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list ResourceSlices: %w", err)
	}

	usable, err := usableNodes(ctx, c)
	if err != nil {
		return "", err
	}

	// Consumers must ignore ResourceSlices whose pool generation is below the
	// highest observed generation for that pool (older slices can linger
	// with stale devices during a multi-slice pool update), and must treat a
	// pool as unusable until all of its slices at that generation have been
	// observed — ResourceSliceCount exists for exactly this check, and the
	// scheduler's allocator ignores incomplete pools.
	type poolInfo struct {
		generation int64
		expected   int64
		observed   int64
		consistent bool
	}
	pools := make(map[string]*poolInfo)
	for _, slice := range slices.Items {
		if slice.Spec.Driver != cfg.DRADriver {
			continue
		}
		p, ok := pools[slice.Spec.Pool.Name]
		if !ok || slice.Spec.Pool.Generation > p.generation {
			pools[slice.Spec.Pool.Name] = &poolInfo{
				generation: slice.Spec.Pool.Generation,
				expected:   slice.Spec.Pool.ResourceSliceCount,
				observed:   1,
				consistent: true,
			}
			continue
		}
		if slice.Spec.Pool.Generation == p.generation {
			p.observed++
			if slice.Spec.Pool.ResourceSliceCount != p.expected {
				p.consistent = false
			}
		}
	}
	poolUsable := func(name string) bool {
		p := pools[name]
		return p != nil && p.consistent && p.expected > 0 && p.observed == p.expected
	}

	sawDriverSlice := false
	for _, slice := range slices.Items {
		if slice.Spec.Driver != cfg.DRADriver ||
			slice.Spec.Pool.Generation < pools[slice.Spec.Pool.Name].generation ||
			!poolUsable(slice.Spec.Pool.Name) ||
			allocatableDeviceCount(&slice) == 0 {
			continue
		}
		sawDriverSlice = true
		node, ok := sliceEligibleNode(&slice, usable, cfg)
		if !ok {
			continue
		}
		logf("Checking environment: Found ResourceSlice: %s (Node: %s, Driver: %s, Devices: %d)", slice.Name, node, slice.Spec.Driver, len(slice.Spec.Devices))
		return node, nil
	}
	if sawDriverSlice {
		return "", fmt.Errorf("ResourceSlices for driver %s exist, but none is reachable from a Ready, schedulable node", cfg.DRADriver)
	}
	return "", fmt.Errorf("no complete current-generation ResourceSlice with allocatable devices found for driver %s", cfg.DRADriver)
}

// allocatableDeviceCount counts a slice's devices that the test's
// toleration-less claim could actually be allocated: devices carrying an
// inline NoSchedule or NoExecute taint cannot satisfy it. Like DeviceClass
// CEL selectors, administrator-applied DeviceTaintRule objects (alpha
// resource.k8s.io/v1alpha3) are out of scope for this preflight.
func allocatableDeviceCount(slice *resourcev1.ResourceSlice) int {
	n := 0
	for _, device := range slice.Spec.Devices {
		if deviceTaintBlocked(&device) {
			continue
		}
		n++
	}
	return n
}

func deviceTaintBlocked(device *resourcev1.Device) bool {
	for _, taint := range device.Taints {
		if taint.Effect == resourcev1.DeviceTaintEffectNoSchedule || taint.Effect == resourcev1.DeviceTaintEffectNoExecute {
			return true
		}
	}
	return false
}

// sliceEligibleNode resolves a ResourceSlice's node topology (exactly one of
// nodeName, nodeSelector, allNodes, perDeviceNodeSelection is set) against
// the usable (Ready, schedulable) nodes and returns a concrete eligible node.
func sliceEligibleNode(slice *resourcev1.ResourceSlice, usable map[string]corev1.Node, cfg AcceleratorConfig) (string, bool) {
	switch {
	case slice.Spec.NodeName != nil:
		if _, ok := usable[*slice.Spec.NodeName]; ok {
			return *slice.Spec.NodeName, true
		}
	case slice.Spec.NodeSelector != nil:
		return firstNodeMatchingSelector(slice.Spec.NodeSelector, usable, cfg)
	case slice.Spec.AllNodes != nil && *slice.Spec.AllNodes:
		return firstUsableNode(usable, cfg)
	case slice.Spec.PerDeviceNodeSelection != nil && *slice.Spec.PerDeviceNodeSelection:
		for _, device := range slice.Spec.Devices {
			if deviceTaintBlocked(&device) {
				continue
			}
			switch {
			case device.NodeName != nil:
				if _, ok := usable[*device.NodeName]; ok {
					return *device.NodeName, true
				}
			case device.NodeSelector != nil:
				if node, ok := firstNodeMatchingSelector(device.NodeSelector, usable, cfg); ok {
					return node, true
				}
			case device.AllNodes != nil && *device.AllNodes:
				return firstUsableNode(usable, cfg)
			}
		}
	}
	return "", false
}

func firstNodeMatchingSelector(selector *corev1.NodeSelector, usable map[string]corev1.Node, cfg AcceleratorConfig) (string, bool) {
	matcher, err := nodeaffinity.NewNodeSelector(selector)
	if err != nil {
		// An unparseable selector cannot be proven to match any node.
		return "", false
	}
	matching := make(map[string]corev1.Node, len(usable))
	for name, node := range usable {
		if matcher.Match(&node) {
			matching[name] = node
		}
	}
	return firstUsableNode(matching, cfg)
}

// firstUsableNode picks a deterministic node from the candidates, preferring
// nodes that advertise the accelerator's extended resource: a broad
// (allNodes/selector) ResourceSlice can nominally cover CPU-only nodes, and
// pinning the negative isolation pod to one of those would make its pass
// vacuous when the positive pod's placement is unavailable to refine it
// (e.g. under -run subtest filtering).
func firstUsableNode(usable map[string]corev1.Node, cfg AcceleratorConfig) (string, bool) {
	if len(usable) == 0 {
		return "", false
	}
	resourceName := corev1.ResourceName(cfg.ExtendedResource)
	names := make([]string, 0, len(usable))
	for name := range usable {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		node := usable[name]
		if q, ok := node.Status.Allocatable[resourceName]; ok && !q.IsZero() {
			return name, true
		}
	}
	return names[0], true
}

// checkDevicePluginUsable verifies that a device plugin advertises allocatable
// accelerator capacity (e.g. nvidia.com/gpu) on at least one schedulable,
// Ready node, and returns that node's name. NotReady nodes retain their
// last-reported allocatable, so they must not count as usable capacity.
func checkDevicePluginUsable(ctx context.Context, c kubernetes.Interface, cfg AcceleratorConfig, logf func(format string, args ...any)) (string, error) {
	resourceName := corev1.ResourceName(cfg.ExtendedResource)

	usable, err := usableNodes(ctx, c)
	if err != nil {
		return "", err
	}

	names := make([]string, 0, len(usable))
	for name := range usable {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		node := usable[name]
		if q, ok := node.Status.Allocatable[resourceName]; ok && !q.IsZero() {
			logf("Checking environment: Found Ready node %s with allocatable %s: %s", node.Name, resourceName, q.String())
			return node.Name, nil
		}
	}
	return "", fmt.Errorf("no schedulable Ready node advertises allocatable %s", resourceName)
}

// checkExtendedResourceNotDRABacked rejects strict device-plugin mode when a
// DRA DeviceClass maps the configured extended resource name via
// spec.extendedResourceName (DRAExtendedResource feature): the scheduler may
// then satisfy the extended-resource request through DRA, so the test could
// not prove device-plugin mediation. A cluster without the DRA API cannot
// have such a mapping and passes trivially.
func checkExtendedResourceNotDRABacked(ctx context.Context, c kubernetes.Interface, cfg AcceleratorConfig) error {
	classes, err := c.ResourceV1().DeviceClasses().List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		// The DRA API is not served, so no DRA-backed extended resources can
		// exist. Every other error (Forbidden, timeout, transient API
		// failure) must fail closed — it does not prove the absence of a
		// mapping.
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to list DeviceClasses to verify %s is not DRA-backed: %w", cfg.ExtendedResource, err)
	}
	for _, class := range classes.Items {
		if class.Spec.ExtendedResourceName != nil && *class.Spec.ExtendedResourceName == cfg.ExtendedResource {
			return fmt.Errorf("DeviceClass %s maps extended resource %s to DRA (spec.extendedResourceName), so %s requests may be satisfied by DRA instead of the device plugin",
				class.Name, cfg.ExtendedResource, cfg.ExtendedResource)
		}
	}
	return nil
}

// testPodConfig controls how buildTestPod grants accelerators.
type testPodConfig struct {
	// grantAccelerator grants the FIRST container one accelerator using mode.
	grantAccelerator bool
	mode             string
	// nodeName pins the pod to a specific node (empty = let the scheduler
	// place it). Used by the negative isolation test so its request-less pod
	// probes an accelerator node instead of passing vacuously on a CPU node.
	nodeName string
	cfg      AcceleratorConfig
}

// buildTestPod constructs (without creating) a test pod. Pure function so the
// claim/extended-resource wiring is unit-testable without a cluster.
func buildTestPod(ns, name string, containers []corev1.Container, pc testPodConfig) (*corev1.Pod, error) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PodSpec{
			Containers:  containers,
			NodeName:    pc.nodeName,
			Tolerations: []corev1.Toleration{{Key: pc.cfg.TaintKey, Operator: "Exists", Effect: "NoSchedule"}}, // for scheduling on accelerator nodes with taints
		},
	}

	// Only the first container is granted the accelerator
	if pc.grantAccelerator {
		switch pc.mode {
		case allocationModeDRA:
			claimName := "claim"
			pod.Spec.ResourceClaims = []corev1.PodResourceClaim{{
				Name:                      claimName,
				ResourceClaimTemplateName: &testResourceTemplateName,
			}}
			pod.Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: claimName}}
		case allocationModeDevicePlugin:
			resourceName := corev1.ResourceName(pc.cfg.ExtendedResource)
			if pod.Spec.Containers[0].Resources.Limits == nil {
				pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{}
			}
			pod.Spec.Containers[0].Resources.Limits[resourceName] = *resource.NewQuantity(requestedAcceleratorCount, resource.DecimalSI)
		default:
			return nil, fmt.Errorf("unknown allocation mode %q", pc.mode)
		}
	}

	return pod, nil
}

func createTestPod(ctx context.Context, t *testing.T, c kubernetes.Interface, pod *corev1.Pod) {
	t.Helper()
	if _, err := c.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Pod %s: %v", pod.Name, err)
	}
}

// waitForPodsRunning waits until every named Pod reaches Running and returns
// their latest objects. This is shared by tests that coordinate multiple Pods.
func waitForPodsRunning(
	ctx context.Context,
	c kubernetes.Interface,
	namespace string,
	names []string,
	timeout time.Duration,
) (map[string]*corev1.Pod, error) {
	running := make(map[string]*corev1.Pod, len(names))
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 3*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		clear(running)
		for _, name := range names {
			pod, err := c.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				lastAPIError = err
				if isRetryableAPIError(err) {
					return false, nil
				}
				return false, err
			}
			lastAPIError = nil
			if pod.Status.Phase != corev1.PodRunning {
				return false, nil
			}
			running[name] = pod
		}
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("Pods did not all reach Running within %s%s: %w",
			timeout, lastAPIErrorSuffix(lastAPIError), err)
	}
	return running, nil
}

// runTestPod creates the pod described by pc, waits for it to reach Running,
// and returns the running pod (so callers can read its scheduled node).
func runTestPod(ctx context.Context, t *testing.T, c kubernetes.Interface, ns, name string, containers []corev1.Container, pc testPodConfig) *corev1.Pod {
	pod, err := buildTestPod(ns, name, containers, pc)
	if err != nil {
		t.Fatalf("Failed to build Pod %s: %v", name, err)
	}

	createTestPod(ctx, t, c, pod)

	t.Logf("Waiting for Pod %s to be running...", name)
	running, err := waitForPodsRunning(ctx, c, ns, []string{name}, time.Minute)
	if err != nil {
		phase := "unknown"
		if p, gerr := c.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{}); gerr == nil {
			phase = string(p.Status.Phase)
		}
		t.Fatalf("Pod %s failed to reach Running phase within 1m. Current phase: %s, Error: %v", name, phase, err)
	}
	return running[name]
}

// deletePodAndWait deletes a pod and waits until the pod AND any
// template-generated ResourceClaims are actually gone, so a subsequent
// accelerator-requesting pod does not race the previous pod's device/claim
// release (matters on single-accelerator nodes; generated claims are
// garbage-collected asynchronously after pod deletion). known is the pod as
// last observed by the caller (may be nil) — it supplies the generated claim
// names when the live pod can no longer be fetched.
func deletePodAndWait(ctx context.Context, c kubernetes.Interface, ns, name string, known *corev1.Pod) error {
	claimNames, captureErr := podGeneratedClaims(ctx, c, ns, name, known)
	releaseErr := deleteAndAwaitRelease(ctx, c, ns, name, claimNames)
	if releaseErr == nil {
		return nil
	}
	if captureErr != nil {
		captureErr = fmt.Errorf("failed to capture generated ResourceClaims of Pod %s: %w", name, captureErr)
	}
	releaseErr = fmt.Errorf("cleanup of Pod %s incomplete: %w", name, releaseErr)
	return errors.Join(captureErr, releaseErr)
}

// podGeneratedClaims returns the names of template-generated ResourceClaims
// recorded in the pod's status, preferring the live object and falling back
// to the caller's last-known copy. Whenever the status yields no claim names
// (the pod was never observed, or its status has not been patched yet), it
// scans the namespace for ResourceClaims owner-referenced by the pod — an
// orphaned or not-yet-recorded generated claim can hold the accelerator well
// after (or before) it appears in pod status.
func podGeneratedClaims(ctx context.Context, c kubernetes.Interface, ns, name string, known *corev1.Pod) ([]string, error) {
	p, err := c.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil:
	case apierrors.IsNotFound(err):
		p, err = known, nil
	default:
		// Fall back to the caller's copy, but surface the error — it does
		// not prove the pod had no claims.
		p = known
	}
	var claimNames []string
	if p != nil {
		for _, cs := range p.Status.ResourceClaimStatuses {
			if cs.ResourceClaimName != nil {
				claimNames = append(claimNames, *cs.ResourceClaimName)
			}
		}
	}
	if len(claimNames) == 0 {
		// An empty status is not proof of no claims: the resourceclaim
		// controller creates the generated ResourceClaim first and patches
		// pod.status.resourceClaimStatuses afterwards, so cleanup can race
		// that window (and a never-observed pod has no status at all).
		claims, listErr := podOwnedClaims(ctx, c, ns, name)
		if err == nil {
			err = listErr
		}
		return claims, err
	}
	return claimNames, err
}

// podOwnedClaims lists the namespace's ResourceClaims and returns those
// owner-referenced by the named Pod. The test namespace is isolated and pod
// names are unique per subtest, so a name match identifies the owner.
func podOwnedClaims(ctx context.Context, c kubernetes.Interface, ns, podName string) ([]string, error) {
	claims, err := c.ResourceV1().ResourceClaims(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// DRA API not served — no generated claims can exist.
			return nil, nil
		}
		return nil, fmt.Errorf("failed to list ResourceClaims to find claims generated for pod %s: %w", podName, err)
	}
	var claimNames []string
	for _, claim := range claims.Items {
		for _, owner := range claim.OwnerReferences {
			if owner.Kind == "Pod" && owner.Name == podName {
				claimNames = append(claimNames, claim.Name)
				break
			}
		}
	}
	return claimNames, nil
}

// deleteAndAwaitRelease deletes the pod (tolerating NotFound), waits for it
// to disappear, and then waits for every captured generated claim to be
// deleted or deallocated. Claims are processed even when the pod was already
// gone — generated claims are garbage-collected asynchronously and may
// outlive the pod.
func deleteAndAwaitRelease(ctx context.Context, c kubernetes.Interface, ns, name string, claimNames []string) error {
	var podAlreadyGone bool
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		err := c.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
		switch {
		case err == nil:
			lastAPIError = nil
			return true, nil
		case apierrors.IsNotFound(err):
			lastAPIError = nil
			podAlreadyGone = true
			return true, nil
		case isRetryableAPIError(err):
			lastAPIError = err
			return false, nil
		default:
			return false, err
		}
	})
	if err != nil {
		return fmt.Errorf("failed to delete pod%s: %w", lastAPIErrorSuffix(lastAPIError), err)
	}
	if !podAlreadyGone {
		var lastAPIError error
		err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			_, err := c.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
			switch {
			case apierrors.IsNotFound(err):
				lastAPIError = nil
				return true, nil
			case err == nil:
				lastAPIError = nil
				return false, nil
			case isRetryableAPIError(err):
				lastAPIError = err
				return false, nil
			default:
				return false, err
			}
		})
		if err != nil {
			return fmt.Errorf("pod not deleted before the cleanup deadline%s: %w", lastAPIErrorSuffix(lastAPIError), err)
		}
	}

	// Final owner scan now that the pod is gone: the resourceclaim controller
	// creates the generated claim before patching pod status, so a claim can
	// appear after the pre-delete capture — this scan catches those. A
	// racing reconciliation could in principle still create a dangling claim
	// after this scan, but such a claim can no longer be published into pod
	// status or allocated for the deleted pod, and garbage collection
	// removes it — it cannot hold the accelerator against a later subtest.
	lateClaims, err := podOwnedClaims(ctx, c, ns, name)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(claimNames))
	for _, claimName := range claimNames {
		seen[claimName] = true
	}
	for _, claimName := range lateClaims {
		if !seen[claimName] {
			claimNames = append(claimNames, claimName)
		}
	}

	for _, claimName := range claimNames {
		var lastAPIError error
		err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
			claim, err := c.ResourceV1().ResourceClaims(ns).Get(ctx, claimName, metav1.GetOptions{})
			switch {
			case apierrors.IsNotFound(err):
				lastAPIError = nil
				return true, nil
			case err != nil && isRetryableAPIError(err):
				lastAPIError = err
				return false, nil
			case err != nil:
				return false, err
			}
			lastAPIError = nil
			// A deallocated claim no longer holds devices even if the object
			// briefly outlives the pod before garbage collection.
			return claim.Status.Allocation == nil, nil
		})
		if err != nil {
			return fmt.Errorf("generated ResourceClaim %s not released before the cleanup deadline%s: %w",
				claimName, lastAPIErrorSuffix(lastAPIError), err)
		}
	}
	return nil
}

// Verify that the container sees exactly the expected number of accelerator
// device nodes by checking its logs.
func verifyAcceleratorCountInLogs(ctx context.Context, t *testing.T, c kubernetes.Interface, ns, podName, containerName string, expectedCount int64) {
	var logs string
	var lastLogErr error
	expectedText := fmt.Sprintf("%s%d", acceleratorCountResultPrefix, expectedCount)
	t.Logf("Waiting to see if Pod %s/%s logs contain '%s'...", podName, containerName, expectedText)
	// The prober emits its result line right after it starts, but on a fresh
	// cluster device/CDI setup or kubelet log lag can push that well past the
	// pod reporting Running. Poll up to a bounded timeout instead of sampling
	// a fixed window.
	err := wait.PollUntilContextTimeout(ctx, acceleratorLogPollInterval, acceleratorLogPollTimeout, true, func(ctx context.Context) (bool, error) {
		rawLogs, err := c.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{Container: containerName}).DoRaw(ctx)
		if err != nil {
			lastLogErr = err
			return false, nil
		}
		lastLogErr = nil
		logs = string(rawLogs)
		return logsContainExactLine(logs, expectedText), nil
	})
	if err == nil {
		t.Logf("PASS: %s sees exactly %d accelerator device(s).", containerName, expectedCount)
		return
	}

	// Include the container's status so an empty log is distinguishable from
	// a prober that crashed or never started.
	detail := fmt.Sprintf("not seen within %s; containerStatus=%s%s",
		acceleratorLogPollTimeout, containerStatusJSON(ctx, c, ns, podName, containerName), lastAPIErrorSuffix(lastLogErr))
	if expectedCount > 0 {
		t.Errorf("FAIL: Container %s in Pod %s should see exactly %d accelerator device(s) (%s). Logs: %s", containerName, podName, expectedCount, detail, logs)
	} else {
		t.Errorf("VIOLATION: Unauthorized Container %s in Pod %s should see no accelerator devices (%s). Logs: %s", containerName, podName, detail, logs)
	}
}

// containerStatusJSON returns the named container's status from the live pod
// (state, lastTerminationState, restartCount, ...) as JSON, or why it could
// not be read.
func containerStatusJSON(ctx context.Context, c kubernetes.Interface, ns, podName, containerName string) string {
	pod, err := c.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Sprintf("<failed to get Pod: %v>", err)
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == containerName {
			b, err := json.Marshal(cs)
			if err != nil {
				return fmt.Sprintf("<failed to marshal: %v>", err)
			}
			return string(b)
		}
	}
	return fmt.Sprintf("<container %q not in pod.status.containerStatuses; phase=%s>", containerName, pod.Status.Phase)
}

func logsContainExactLine(logs, expected string) bool {
	for _, line := range strings.Split(logs, "\n") {
		if strings.TrimSpace(line) == expected {
			return true
		}
	}
	return false
}

func acceleratorProbeCommand(devicePattern string) string {
	return fmt.Sprintf(`count=0
for device in %s; do
  [ -e "$device" ] || continue
  count=$((count + 1))
done
echo "%s$count"`, devicePattern, acceleratorCountResultPrefix)
}

// Returns a container that probes for accelerator
func acceleratorProbingContainer(name string, cfg AcceleratorConfig) corev1.Container {
	return corev1.Container{
		Name:    name,
		Image:   "ubuntu:22.04",
		Command: []string{"/bin/sh", "-c"},
		Args: []string{
			acceleratorProbeCommand(cfg.DevicePattern) + "\nsleep 3600",
		},
	}
}

func randomNamespaceName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, rand.String(5))
}

func lastAPIErrorSuffix(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("; last API error: %v", err)
}

func isRetryableAPIError(err error) bool {
	return apierrors.IsTimeout(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) ||
		apierrors.IsServiceUnavailable(err) ||
		apierrors.IsInternalError(err) ||
		utilnet.IsProbableEOF(err) ||
		utilnet.IsConnectionReset(err) ||
		utilnet.IsHTTP2ConnectionLost(err)
}
