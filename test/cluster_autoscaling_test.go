package conformance

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

const (
	autoscalerRunLabelKey       = "ai-conformance.kubernetes.io/autoscaler-run"
	baselinePodTimeout          = 5 * time.Minute
	baselineStabilityWindow     = 30 * time.Second
	baselineStabilityTimeout    = 2 * time.Minute
	autoscalerPodDeadlineBuffer = 15 * time.Minute
)

type nodeReference struct {
	name string
	uid  types.UID
}

var (
	autoscalerNodePoolLabel    *string
	autoscalerPendingTimeout   *time.Duration
	autoscalerScaleUpTimeout   *time.Duration
	autoscalerScaleDownTimeout *time.Duration
	autoscalerStabilityWindow  *time.Duration
)

func init() {
	registerFlagGroup("autoscaler", "Accelerator Cluster Autoscaling Flags (TestAcceleratorClusterAutoscaling)")
	autoscalerNodePoolLabel = flag.String("autoscaler-node-pool-label", "",
		"Node label identifying the accelerator pool to test, in key=value form. The test is skipped when unset for platforms where cluster_autoscaling is N/A.")
	autoscalerPendingTimeout = flag.Duration("autoscaler-pending-timeout", 2*time.Minute,
		"How long to wait for the scale-up trigger Pod to become Unschedulable.")
	autoscalerScaleUpTimeout = flag.Duration("autoscaler-scale-up-timeout", 20*time.Minute,
		"How long to wait for the accelerator pool to scale up and schedule the trigger Pod.")
	autoscalerScaleDownTimeout = flag.Duration("autoscaler-scale-down-timeout", 30*time.Minute,
		"How long to wait for the accelerator pool to return to its baseline size.")
	autoscalerStabilityWindow = flag.Duration("autoscaler-stability-window", 30*time.Second,
		"How long the accelerator pool must remain at baseline size before scale-down passes.")
}

// TestAcceleratorClusterAutoscaling verifies that a pending accelerator Pod
// causes the selected accelerator node pool to scale up, and that the pool
// returns to its baseline size after the Pod is deleted.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/tree/main/kars/0055-cluster-autoscaling-for-accelerators
func TestAcceleratorClusterAutoscaling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if !flag.Parsed() {
		flag.Parse()
	}
	if *autoscalerNodePoolLabel == "" {
		t.Skip("cluster autoscaling test is not configured; if the platform has no cluster autoscaler, mark cluster_autoscaling N/A, otherwise set -autoscaler-node-pool-label=<key>=<value>")
	}

	poolKey, poolValue, err := parseNodePoolLabel(*autoscalerNodePoolLabel)
	if err != nil {
		t.Fatalf("Invalid -autoscaler-node-pool-label: %v", err)
	}
	if err := validateAutoscalerDurations(
		*autoscalerPendingTimeout,
		*autoscalerScaleUpTimeout,
		*autoscalerScaleDownTimeout,
		*autoscalerStabilityWindow,
	); err != nil {
		t.Fatalf("Invalid autoscaler timeout configuration: %v", err)
	}
	podDeadlineSeconds := autoscalerPodDeadlineSeconds(
		*autoscalerPendingTimeout,
		*autoscalerScaleUpTimeout,
		*autoscalerScaleDownTimeout,
	)
	cfg, err := lookupAcceleratorConfig(*acceleratorType)
	if err != nil {
		t.Fatalf("Invalid -accelerator-type: %v", err)
	}

	ctx := context.Background()
	clientset := getClientset(t)
	mode, _, err := detectAllocationMode(ctx, clientset, *allocationMode, cfg, t.Logf)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Failed to resolve allocation mode: %v", err)
	}
	t.Logf("Accelerator autoscaling test running with allocation mode: %s", mode)

	// Capture the stable size and accelerator capacity of the target node pool.
	initialNodes, err := readyPoolNodes(ctx, clientset, poolKey, poolValue)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Failed to list accelerator pool nodes: %v", err)
	}
	if len(initialNodes) == 0 {
		t.Fatalf("ENVIRONMENT ERROR: No Ready, schedulable nodes match accelerator pool label %s=%s", poolKey, poolValue)
	}

	baselineNodes, err := waitForStablePoolSize(ctx, clientset, poolKey, poolValue, len(initialNodes), baselineStabilityWindow, baselineStabilityTimeout)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Accelerator pool did not have a stable baseline size: %v", err)
	}
	baselineCount := len(baselineNodes)
	t.Logf("Captured accelerator pool baseline: %d Ready, schedulable nodes matching %s=%s", baselineCount, poolKey, poolValue)

	if err := verifyAutoscalerPoolCapacity(ctx, clientset, baselineNodes, mode, cfg); err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %v", err)
	}
	if err := verifyPoolIsIdle(ctx, clientset, "", baselineNodes, poolKey, poolValue, mode, cfg); err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %v", err)
	}

	namespace := randomNamespaceName("accelerator-autoscaling")
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: %v. Please ensure this namespace is terminated manually to avoid resource leaks.", err)
		}
	})
	setupTestEnvironment(ctx, t, clientset, namespace, mode, cfg)

	// Keep one accelerator Pod running on every baseline node.
	runLabelValue := randomNamespaceName("run")
	createBaselinePDB(ctx, t, clientset, namespace, runLabelValue, baselineCount)
	createdPods := map[string]*corev1.Pod{}
	t.Cleanup(func() {
		names := make([]string, 0, len(createdPods))
		for name := range createdPods {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if err := deletePodAndWait(ctx, clientset, namespace, name, createdPods[name]); err != nil {
				t.Errorf("Cleanup of Pod %s incomplete; subsequent accelerator tests may race its device/claim release: %v", name, err)
			}
		}
	})

	baselinePodNames := make([]string, 0, baselineCount)
	for i := 0; i < baselineCount; i++ {
		name := fmt.Sprintf("autoscaler-baseline-%d", i)
		pod, err := buildAutoscalingPod(namespace, name, runLabelValue, poolKey, poolValue, mode, cfg, podDeadlineSeconds, true)
		if err != nil {
			t.Fatalf("Failed to build baseline Pod %s: %v", name, err)
		}
		createTestPod(ctx, t, clientset, pod)
		createdPods[name] = pod
		baselinePodNames = append(baselinePodNames, name)
	}

	runningBaseline, err := waitForPodsRunning(ctx, clientset, namespace, baselinePodNames, baselinePodTimeout)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Baseline accelerator Pods did not all reach Running: %v", err)
	}
	if err := verifyBaselinePlacement(runningBaseline, baselineNodes); err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %v", err)
	}
	for name, pod := range runningBaseline {
		createdPods[name] = pod
	}
	currentBaseline, err := readyPoolNodes(ctx, clientset, poolKey, poolValue)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Failed to recheck accelerator pool nodes: %v", err)
	}
	if !sameNodeIdentitySet(currentBaseline, baselineNodes) {
		t.Fatalf("ENVIRONMENT ERROR: Accelerator pool membership changed while establishing the baseline")
	}
	if err := verifyPoolIsIdle(ctx, clientset, namespace, currentBaseline, poolKey, poolValue, mode, cfg); err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %v", err)
	}
	t.Logf("Baseline established: %d accelerator Pods occupy %d distinct pool nodes", baselineCount, baselineCount)

	// Create one additional Pod and verify it initially cannot be scheduled.
	triggerName := "autoscaler-trigger"
	trigger, err := buildAutoscalingPod(namespace, triggerName, runLabelValue, poolKey, poolValue, mode, cfg, podDeadlineSeconds, false)
	if err != nil {
		t.Fatalf("Failed to build scale-up trigger Pod: %v", err)
	}
	createTestPod(ctx, t, clientset, trigger)
	createdPods[triggerName] = trigger

	message, err := waitForPodUnschedulable(ctx, clientset, namespace, triggerName, *autoscalerPendingTimeout)
	if err != nil {
		t.Fatalf("FAIL: Scale-up trigger Pod did not become Unschedulable: %v", err)
	}
	if mode == allocationModeDevicePlugin && !strings.Contains(message, cfg.ExtendedResource) {
		t.Fatalf("FAIL: Scale-up trigger was Unschedulable for a reason unrelated to %s: %s", cfg.ExtendedResource, message)
	}
	t.Logf("Scale-up triggered by Pending Pod %s: %s", triggerName, message)

	// Verify the pool adds a node that can run the pending accelerator Pod.
	runningTrigger, scaledNodes, err := waitForTriggerRunningAfterScaleUp(
		ctx,
		clientset,
		namespace,
		triggerName,
		poolKey,
		poolValue,
		baselineNodes,
		mode,
		cfg,
		*autoscalerScaleUpTimeout,
	)
	if err != nil {
		t.Fatalf("FAIL: Accelerator pool did not scale up successfully: %v", err)
	}
	createdPods[triggerName] = runningTrigger
	scaledNode := scaledNodes[runningTrigger.Spec.NodeName]
	scaledNodeRef := nodeReference{name: scaledNode.Name, uid: scaledNode.UID}
	if err := verifyAutoscalerPoolCapacity(ctx, clientset, scaledNodes, mode, cfg); err != nil {
		t.Fatalf("FAIL: Scaled pool did not preserve the accelerator allocation contract: %v", err)
	}
	t.Logf("PASS: Accelerator pool scaled from %d to %d nodes; trigger Pod is Running on new node %s", baselineCount, len(scaledNodes), scaledNodeRef.name)

	if err := deletePodAndWait(ctx, clientset, namespace, triggerName, runningTrigger); err != nil {
		t.Fatalf("FAIL: Trigger Pod and generated claims were not released: %v", err)
	}
	delete(createdPods, triggerName)

	// Verify the idle node is removed and the pool returns to its baseline size.
	if err := waitForScaleDown(
		ctx,
		clientset,
		poolKey,
		poolValue,
		scaledNodeRef,
		baselineNodes,
		*autoscalerStabilityWindow,
		*autoscalerScaleDownTimeout,
	); err != nil {
		t.Fatalf("FAIL: Accelerator pool did not scale down to its baseline size: %v", err)
	}
	t.Logf("PASS: Accelerator pool returned to its baseline size of %d nodes", baselineCount)
}

func parseNodePoolLabel(value string) (string, string, error) {
	if strings.Count(value, "=") != 1 {
		return "", "", fmt.Errorf("expected exactly one key=value pair, got %q", value)
	}
	key, labelValue, _ := strings.Cut(value, "=")
	if key == "" || labelValue == "" {
		return "", "", fmt.Errorf("label key and value must both be non-empty")
	}
	if errs := validation.IsQualifiedName(key); len(errs) > 0 {
		return "", "", fmt.Errorf("invalid label key %q: %s", key, strings.Join(errs, "; "))
	}
	if errs := validation.IsValidLabelValue(labelValue); len(errs) > 0 {
		return "", "", fmt.Errorf("invalid label value %q: %s", labelValue, strings.Join(errs, "; "))
	}
	return key, labelValue, nil
}

func validateAutoscalerDurations(pending, scaleUp, scaleDown, stableFor time.Duration) error {
	switch {
	case pending <= 0:
		return fmt.Errorf("-autoscaler-pending-timeout must be greater than zero")
	case scaleUp <= 0:
		return fmt.Errorf("-autoscaler-scale-up-timeout must be greater than zero")
	case scaleDown <= 0:
		return fmt.Errorf("-autoscaler-scale-down-timeout must be greater than zero")
	case stableFor < 0:
		return fmt.Errorf("-autoscaler-stability-window must not be negative")
	case stableFor >= scaleDown:
		return fmt.Errorf("-autoscaler-stability-window must be shorter than -autoscaler-scale-down-timeout")
	default:
		return nil
	}
}

func autoscalerPodDeadlineSeconds(pending, scaleUp, scaleDown time.Duration) int64 {
	lifetime := baselinePodTimeout + pending + scaleUp + scaleDown + autoscalerPodDeadlineBuffer
	return int64((lifetime + time.Second - 1) / time.Second)
}

func createBaselinePDB(
	ctx context.Context,
	t *testing.T,
	c kubernetes.Interface,
	namespace, runLabelValue string,
	baselineCount int,
) {
	t.Helper()
	minAvailable := intstr.FromInt32(int32(baselineCount))
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "autoscaler-baseline", Namespace: namespace},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{autoscalerRunLabelKey: runLabelValue},
			},
		},
	}
	if _, err := c.PolicyV1().PodDisruptionBudgets(namespace).Create(ctx, pdb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create baseline PodDisruptionBudget: %v", err)
	}
}

func readyPoolNodes(ctx context.Context, c kubernetes.Interface, key, value string) (map[string]corev1.Node, error) {
	nodes, err := labeledPoolNodes(ctx, c, key, value)
	if err != nil {
		return nil, err
	}

	ready := make(map[string]corev1.Node)
	for name, node := range nodes {
		if node.Spec.Unschedulable || !nodeIsReady(&node) {
			continue
		}
		ready[name] = node
	}
	return ready, nil
}

func labeledPoolNodes(ctx context.Context, c kubernetes.Interface, key, value string) (map[string]corev1.Node, error) {
	nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}
	matching := make(map[string]corev1.Node)
	for _, node := range nodes.Items {
		if node.Labels[key] == value {
			matching[node.Name] = node
		}
	}
	return matching, nil
}

func sameNodeIdentitySet(a, b map[string]corev1.Node) bool {
	if len(a) != len(b) {
		return false
	}
	for name, node := range a {
		other, ok := b[name]
		if !ok || node.UID != other.UID {
			return false
		}
	}
	return true
}

func waitForStablePoolSize(
	ctx context.Context,
	c kubernetes.Interface,
	key, value string,
	expected int,
	stableFor, timeout time.Duration,
) (map[string]corev1.Node, error) {
	var stableSince time.Time
	var stableNodes map[string]corev1.Node
	var lastNodes map[string]corev1.Node
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		nodes, err := readyPoolNodes(ctx, c, key, value)
		if err != nil {
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil
		lastNodes = nodes
		if len(nodes) != expected {
			stableSince = time.Time{}
			stableNodes = nil
			return false, nil
		}
		if stableSince.IsZero() || !sameNodeIdentitySet(nodes, stableNodes) {
			stableSince = time.Now()
			stableNodes = nodes
		}
		return stableFor <= 0 || time.Since(stableSince) >= stableFor, nil
	})
	if err != nil {
		return nil, fmt.Errorf("wanted %d nodes stable for %s; last observed %d%s: %w",
			expected, stableFor, len(lastNodes), lastAPIErrorSuffix(lastAPIError), err)
	}
	return lastNodes, nil
}

func verifyPoolDevicePluginCapacity(nodes map[string]corev1.Node, cfg AcceleratorConfig) error {
	resourceName := corev1.ResourceName(cfg.ExtendedResource)
	var invalid []string
	for name, node := range nodes {
		q, ok := node.Status.Allocatable[resourceName]
		if !ok || q.Value() != 1 {
			invalid = append(invalid, fmt.Sprintf("%s=%s", name, q.String()))
		}
	}
	if len(invalid) > 0 {
		sort.Strings(invalid)
		return fmt.Errorf("pool nodes must each advertise exactly one allocatable %s: %s", resourceName, strings.Join(invalid, ", "))
	}
	return nil
}

func verifyAutoscalerPoolCapacity(
	ctx context.Context,
	c kubernetes.Interface,
	nodes map[string]corev1.Node,
	mode string,
	cfg AcceleratorConfig,
) error {
	switch mode {
	case allocationModeDevicePlugin:
		if err := verifyPoolDevicePluginCapacity(nodes, cfg); err != nil {
			return err
		}
		if err := checkExtendedResourceNotDRABacked(ctx, c, cfg); err != nil {
			return fmt.Errorf("device-plugin allocation would not be deterministic: %w", err)
		}
		return nil
	case allocationModeDRA:
		if _, err := c.ResourceV1().DeviceClasses().Get(ctx, cfg.DeviceClass, metav1.GetOptions{}); err != nil {
			return fmt.Errorf("DRA DeviceClass %s is not available: %w", cfg.DeviceClass, err)
		}
		return nil
	default:
		return fmt.Errorf("unknown allocation mode %q", mode)
	}
}

func verifyPoolIsIdle(
	ctx context.Context,
	c kubernetes.Interface,
	testNamespace string,
	poolNodes map[string]corev1.Node,
	poolKey, poolValue, mode string,
	cfg AcceleratorConfig,
) error {
	pods, err := c.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: nonTerminalPodFieldSelector(),
	})
	if err != nil {
		return fmt.Errorf("failed to list Pods while checking target-pool isolation: %w", err)
	}

	var foreign []string
	if mode == allocationModeDRA {
		claims, err := c.ResourceV1().ResourceClaims(corev1.NamespaceAll).List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to list ResourceClaims while checking DRA isolation: %w", err)
		}
		for _, claim := range claims.Items {
			if claim.Namespace != testNamespace && claim.Status.Allocation != nil {
				foreign = append(foreign, fmt.Sprintf("%s/%s has an active DRA allocation", claim.Namespace, claim.Name))
			}
		}
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Namespace == testNamespace || pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if mode == allocationModeDRA && len(pod.Spec.ResourceClaims) > 0 {
			location := "pending"
			if pod.Spec.NodeName != "" {
				location = "on " + pod.Spec.NodeName
			}
			foreign = append(foreign, fmt.Sprintf("%s/%s uses DRA claims %s", pod.Namespace, pod.Name, location))
			continue
		}
		if isDaemonOrMirrorPod(pod) {
			continue
		}
		if isPlatformTransientPod(pod, mode, cfg) {
			continue
		}
		if _, inPool := poolNodes[pod.Spec.NodeName]; inPool {
			foreign = append(foreign, fmt.Sprintf("%s/%s on %s", pod.Namespace, pod.Name, pod.Spec.NodeName))
			continue
		}
		if pod.Spec.NodeName == "" {
			switch {
			case pod.Spec.NodeSelector[poolKey] == poolValue:
				foreign = append(foreign, fmt.Sprintf("%s/%s pending for the target pool", pod.Namespace, pod.Name))
			case podRequestsAccelerator(pod, mode, cfg):
				foreign = append(foreign, fmt.Sprintf("%s/%s pending for the configured accelerator", pod.Namespace, pod.Name))
			}
		}
	}
	if len(foreign) > 0 {
		sort.Strings(foreign)
		return fmt.Errorf("target accelerator pool is not isolated; found foreign workloads: %s", strings.Join(foreign, ", "))
	}
	return nil
}

func nonTerminalPodFieldSelector() string {
	return fields.AndSelectors(
		fields.OneTermNotEqualSelector("status.phase", string(corev1.PodSucceeded)),
		fields.OneTermNotEqualSelector("status.phase", string(corev1.PodFailed)),
	).String()
}

func isDaemonOrMirrorPod(pod *corev1.Pod) bool {
	if _, ok := pod.Annotations["kubernetes.io/config.mirror"]; ok {
		return true
	}
	owner := metav1.GetControllerOf(pod)
	return owner != nil && owner.Kind == "DaemonSet"
}

// isPlatformTransientPod returns true for short-lived pods that are part of
// the accelerator platform stack (e.g. driver validation pods created by the
// NVIDIA GPU Operator) and should not be treated as foreign workloads by the
// pool isolation check.
//
// These pods share the following characteristics:
//   - They run in an operator/system namespace (not a user namespace).
//   - They have RestartPolicy Never or OnFailure, indicating batch semantics.
//   - They do NOT request any accelerator resources themselves.
//
// Without this exemption, transient platform pods such as
// nvidia-cuda-validator (created by the GPU Operator's ClusterPolicy
// controller after a node joins) cause verifyPoolIsIdle to report a false
// "foreign workload" error, because they are not owned by a DaemonSet.
func isPlatformTransientPod(pod *corev1.Pod, mode string, cfg AcceleratorConfig) bool {
	if !isPlatformNamespace(pod.Namespace) {
		return false
	}
	// An unset RestartPolicy is defaulted to Always by the API server, so only
	// an explicit batch policy qualifies as transient.
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever &&
		pod.Spec.RestartPolicy != corev1.RestartPolicyOnFailure {
		return false
	}
	return !podRequestsAccelerator(pod, mode, cfg)
}

// platformNamespacePrefixes identify namespaces owned by the cluster platform
// or by an accelerator vendor's operator, rather than by users. Pods in user
// namespaces are never exempt from the pool isolation check.
var platformNamespacePrefixes = []string{
	"kube-",
	"openshift-",
	"gpu-operator",
	"nvidia-",
}

func isPlatformNamespace(namespace string) bool {
	for _, prefix := range platformNamespacePrefixes {
		if strings.HasPrefix(namespace, prefix) {
			return true
		}
	}
	return false
}

func podRequestsAccelerator(pod *corev1.Pod, mode string, cfg AcceleratorConfig) bool {
	if mode == allocationModeDRA {
		return len(pod.Spec.ResourceClaims) > 0
	}
	resourceName := corev1.ResourceName(cfg.ExtendedResource)
	requestsAccelerator := func(containers []corev1.Container) bool {
		for _, container := range containers {
			request := container.Resources.Requests[resourceName]
			limit := container.Resources.Limits[resourceName]
			if !request.IsZero() || !limit.IsZero() {
				return true
			}
		}
		return false
	}
	return requestsAccelerator(pod.Spec.InitContainers) || requestsAccelerator(pod.Spec.Containers)
}

func buildAutoscalingPod(
	namespace, name, runLabelValue, poolKey, poolValue, mode string,
	cfg AcceleratorConfig,
	activeDeadlineSeconds int64,
	requireDistinctNode bool,
) (*corev1.Pod, error) {
	pod, err := buildTestPod(
		namespace,
		name,
		[]corev1.Container{{
			Name:    "worker",
			Image:   "ubuntu:22.04",
			Command: []string{"/bin/sh", "-c"},
			Args:    []string{"sleep infinity"},
		}},
		testPodConfig{grantAccelerator: true, mode: mode, cfg: cfg},
	)
	if err != nil {
		return nil, err
	}

	pod.Spec.NodeSelector = map[string]string{poolKey: poolValue}
	pod.Spec.ActiveDeadlineSeconds = &activeDeadlineSeconds
	if requireDistinctNode {
		pod.Labels = map[string]string{autoscalerRunLabelKey: runLabelValue}
		pod.Spec.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{autoscalerRunLabelKey: runLabelValue},
					},
					TopologyKey: corev1.LabelHostname,
				}},
			},
		}
	}
	return pod, nil
}

func verifyBaselinePlacement(pods map[string]*corev1.Pod, baselineNodes map[string]corev1.Node) error {
	placements := make(map[string]string, len(pods))
	for name, pod := range pods {
		if _, ok := baselineNodes[pod.Spec.NodeName]; !ok {
			return fmt.Errorf("baseline Pod %s ran on node %s outside the captured pool", name, pod.Spec.NodeName)
		}
		if other, exists := placements[pod.Spec.NodeName]; exists {
			return fmt.Errorf("baseline Pods %s and %s ran on the same node %s; expected one Pod per pool node", other, name, pod.Spec.NodeName)
		}
		placements[pod.Spec.NodeName] = name
	}
	if len(placements) != len(baselineNodes) {
		return fmt.Errorf("baseline Pods occupied %d distinct nodes, want %d", len(placements), len(baselineNodes))
	}
	return nil
}

func waitForPodUnschedulable(
	ctx context.Context,
	c kubernetes.Interface,
	namespace, name string,
	timeout time.Duration,
) (string, error) {
	var lastPhase corev1.PodPhase
	var lastMessage string
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil
		lastPhase = pod.Status.Phase
		if unschedulable, message := podUnschedulable(pod); unschedulable {
			lastMessage = message
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return "", fmt.Errorf("Pod %s remained in phase %s without an Unschedulable condition within %s%s: %w",
			name, lastPhase, timeout, lastAPIErrorSuffix(lastAPIError), err)
	}
	return lastMessage, nil
}

func waitForTriggerRunningAfterScaleUp(
	ctx context.Context,
	c kubernetes.Interface,
	namespace, podName, poolKey, poolValue string,
	baselineNodes map[string]corev1.Node,
	mode string,
	cfg AcceleratorConfig,
	timeout time.Duration,
) (*corev1.Pod, map[string]corev1.Node, error) {
	var lastNodes map[string]corev1.Node
	var running *corev1.Pod
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		nodes, err := readyPoolNodes(ctx, c, poolKey, poolValue)
		if err != nil {
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil
		lastNodes = nodes
		for name, baseline := range baselineNodes {
			current, ok := nodes[name]
			if !ok || current.UID != baseline.UID {
				return false, fmt.Errorf("baseline node %s changed or stopped being Ready and schedulable during scale-up", name)
			}
		}
		if err := verifyPoolIsIdle(ctx, c, namespace, nodes, poolKey, poolValue, mode, cfg); err != nil {
			if isRetryableAPIError(err) {
				lastAPIError = err
				return false, nil
			}
			return false, err
		}
		if len(nodes) < len(baselineNodes)+1 {
			return false, nil
		}

		pod, err := c.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
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
		triggerNode, inPool := nodes[pod.Spec.NodeName]
		if !inPool {
			return false, fmt.Errorf("trigger Pod ran on node %s outside the selected accelerator pool", pod.Spec.NodeName)
		}
		if baseline, existed := baselineNodes[pod.Spec.NodeName]; existed && baseline.UID == triggerNode.UID {
			return false, fmt.Errorf("trigger Pod ran on baseline node %s; accelerator capacity was not exhausted as required", pod.Spec.NodeName)
		}
		running = pod
		return true, nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("last observed pool size %d, wanted at least %d and a Running trigger Pod%s: %w",
			len(lastNodes), len(baselineNodes)+1, lastAPIErrorSuffix(lastAPIError), err)
	}
	return running, lastNodes, nil
}

func waitForScaleDown(
	ctx context.Context,
	c kubernetes.Interface,
	poolKey, poolValue string,
	scaledNode nodeReference,
	baselineNodes map[string]corev1.Node,
	stableFor, timeout time.Duration,
) error {
	var stableSince time.Time
	var lastReadyCount int
	var baselineUsable bool
	var scaledNodeReady bool
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 10*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		nodes, err := labeledPoolNodes(ctx, c, poolKey, poolValue)
		if err != nil {
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil
		lastReadyCount = 0
		scaledNodeReady = false
		for _, node := range nodes {
			if !nodeIsReady(&node) {
				continue
			}
			lastReadyCount++
			if node.Name == scaledNode.name && node.UID == scaledNode.uid {
				scaledNodeReady = true
			}
		}

		baselineUsable = true
		for name, baseline := range baselineNodes {
			current, ok := nodes[name]
			if !ok || current.UID != baseline.UID || current.Spec.Unschedulable || !nodeIsReady(&current) {
				baselineUsable = false
				break
			}
		}
		if lastReadyCount != len(baselineNodes) || !baselineUsable || scaledNodeReady {
			stableSince = time.Time{}
			return false, nil
		}
		if stableSince.IsZero() {
			stableSince = time.Now()
		}
		return stableFor <= 0 || time.Since(stableSince) >= stableFor, nil
	})
	if err != nil {
		return fmt.Errorf("last observed Ready pool size %d; baseline Ready and schedulable=%t; scaled node %s/%s Ready=%t; wanted the %d baseline nodes Ready and schedulable for %s%s: %w",
			lastReadyCount, baselineUsable, scaledNode.name, scaledNode.uid, scaledNodeReady, len(baselineNodes), stableFor, lastAPIErrorSuffix(lastAPIError), err)
	}
	return nil
}
