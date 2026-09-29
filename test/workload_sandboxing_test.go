package conformance

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const (
	sandboxTypeAuto         = "auto"
	sandboxTypeRuntimeClass = "runtimeclass"
	sandboxTypeAgentSandbox = "agent-sandbox"

	agentSandboxAPIGroup = "agents.x-k8s.io"

	sandboxProbeContainer       = "prober"
	sandboxProbeCompletedMarker = "SANDBOX_PROBE: COMPLETED"
)

var (
	sandboxRuntimeClass *string
	sandboxType         *string
	sandboxImage        *string
	sandboxNamespace    *string
	sandboxTimeout      *time.Duration

	errNoSandboxingSolution = errors.New("no sandboxing solution (RuntimeClass or agent-sandbox) detected")
)

func init() {
	sandboxRuntimeClass = flag.String("sandbox-runtime-class", "",
		"Name of the RuntimeClass configured for workload sandboxing (e.g. gvisor, kata, runsc). If empty, auto-detection will be performed.")
	sandboxType = flag.String("sandbox-type", sandboxTypeAuto,
		"Type of sandboxing solution to test: 'auto' (detect RuntimeClass or agent-sandbox), 'runtimeclass', or 'agent-sandbox'.")
	sandboxImage = flag.String("sandbox-image", "busybox",
		"Container image used for executing the sandboxing isolation probe.")
	sandboxNamespace = flag.String("sandbox-namespace", "",
		"Namespace for sandboxing test execution. If empty, a temporary namespace is generated and cleaned up.")
	sandboxTimeout = flag.Duration("sandbox-timeout", 3*time.Minute,
		"Timeout for each step of the sandboxing test: the sandboxed workload (and its unsandboxed control pod) reaching Running, and the probe script completing.")
}

// sandboxingSolution represents a resolved sandboxing mechanism to test.
type sandboxingSolution struct {
	SolutionType     string // sandboxTypeRuntimeClass or sandboxTypeAgentSandbox
	RuntimeClassName string
	Handler          string
	AgentSandboxGVR  *schema.GroupVersionResource
}

// kernelIdentity is what a workload observes about the kernel it runs on. A
// sandbox with its own kernel (gVisor's sentry, a Kata guest VM) reports a
// different identity than a plain container sharing the host kernel.
type kernelIdentity struct {
	Release string // uname -r
	Version string // /proc/version
	BootID  string // /proc/sys/kernel/random/boot_id
}

// sandboxProbeResults captures parsed output from the sandbox isolation probe script.
type sandboxProbeResults struct {
	SchedulingPassed      bool
	PidIsolationPassed    bool
	KernelIsolationPassed bool
	FsIsolationPassed     bool
	NetIsolationPassed    bool
	ProbeCompleted        bool
	Kernel                kernelIdentity
	Interfaces            string
	PidCount              int
	RawLogs               string
}

// TestWorkloadSandboxing verifies the Workload Sandboxing requirement (KAR-0020).
// It verifies that the platform provides a sandboxing mechanism (e.g. standard
// Kubernetes RuntimeClass such as gVisor, Kata, or an agent-sandbox solution)
// that isolates untrusted workload execution from the host node kernel, process,
// filesystem, and network namespaces.
//
// The isolation probes alone cannot tell a sandbox from a plain runc container
// (a runc pod also has its own PID and network namespaces and no /dev/mem), so
// the test additionally runs the same probe in an unsandboxed control pod
// pinned to the node the sandboxed workload landed on and requires the two
// to observe different kernel identities.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/tree/main/kars/0020-workload-sandboxing
func TestWorkloadSandboxing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if !flag.Parsed() {
		flag.Parse()
	}

	clientset := getClientset(t)

	ctx := context.Background()
	solution, err := discoverSandboxingSolution(ctx, clientset, *sandboxType, *sandboxRuntimeClass, t.Logf)
	if err != nil {
		if errors.Is(err, errNoSandboxingSolution) {
			t.Skipf("Skipping workload sandboxing test: %v. Specify -sandbox-runtime-class or -sandbox-type to run. Platforms where workload sandboxing is not supported may leave these flags unset to opt out.", err)
		}
		t.Fatalf("Failed to resolve sandboxing solution: %v", err)
	}

	t.Logf("Running workload sandboxing test using %s (runtimeClass: %q, handler: %q)",
		solution.SolutionType, solution.RuntimeClassName, solution.Handler)

	namespace := *sandboxNamespace
	if namespace == "" {
		namespace = randomNamespaceName("sandbox-conformance")
		if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("Failed to create namespace %s: %v", namespace, err)
		}
		t.Cleanup(func() {
			if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
				t.Errorf("CLEANUP FAILURE: Failed to delete namespace %s: %v", namespace, err)
			}
		})
	}

	// Sandboxed workload.
	var sandboxedPodName string
	switch solution.SolutionType {
	case sandboxTypeRuntimeClass:
		sandboxedPodName = "sandboxed-probe-pod"
		pod := buildProbePod(namespace, sandboxedPodName, solution.RuntimeClassName, "", *sandboxImage)
		t.Cleanup(func() { deletePodInBackground(ctx, clientset, namespace, sandboxedPodName) })
		t.Logf("Creating sandboxed pod %s/%s with RuntimeClass %q...", namespace, sandboxedPodName, solution.RuntimeClassName)
		createTestPod(ctx, t, clientset, pod)
	case sandboxTypeAgentSandbox:
		dynamicClient := getDynamicClient(t)
		sandboxName := "sandboxed-probe-cr"
		t.Cleanup(func() {
			deletePolicy := metav1.DeletePropagationBackground
			_ = dynamicClient.Resource(*solution.AgentSandboxGVR).Namespace(namespace).Delete(ctx, sandboxName, metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
		})

		sandboxCR := buildAgentSandboxCR(namespace, sandboxName, *solution.AgentSandboxGVR, solution.RuntimeClassName, *sandboxImage)
		t.Logf("Creating agent-sandbox resource %s/%s...", namespace, sandboxName)
		if _, err := dynamicClient.Resource(*solution.AgentSandboxGVR).Namespace(namespace).Create(ctx, sandboxCR, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create agent-sandbox resource %s: %v", sandboxName, err)
		}

		sandboxedPodName, err = waitForAgentSandboxPod(ctx, clientset, namespace, sandboxName, *sandboxTimeout)
		if err != nil {
			t.Fatalf("Failed to find Pod created by agent-sandbox %s: %v", sandboxName, err)
		}
	default:
		t.Fatalf("BUG: unknown sandboxing solution type %q", solution.SolutionType)
	}

	sandboxedPod := waitForProbePodRunning(ctx, t, clientset, namespace, sandboxedPodName)
	sandboxNode := sandboxedPod.Spec.NodeName
	t.Logf("Sandboxed pod %s is Running on node %s", sandboxedPodName, sandboxNode)

	sandboxedLogs, err := waitForProbeCompletion(ctx, clientset, namespace, sandboxedPodName, *sandboxTimeout)
	if err != nil {
		t.Fatalf("Sandboxed workload probe did not complete: %v", err)
	}
	sandboxed := parseProbeLogs(sandboxedLogs)

	// Unsandboxed control pod on the same node. It is pinned with
	// spec.nodeName (bypassing the scheduler, so NoSchedule taints on a
	// dedicated sandbox node pool do not block it); it requests no
	// accelerators, so bypassing the scheduler is safe here.
	controlPodName := "unsandboxed-control-pod"
	controlPod := buildProbePod(namespace, controlPodName, "", sandboxNode, *sandboxImage)
	t.Cleanup(func() { deletePodInBackground(ctx, clientset, namespace, controlPodName) })
	t.Logf("Creating unsandboxed control pod %s/%s on node %s...", namespace, controlPodName, sandboxNode)
	createTestPod(ctx, t, clientset, controlPod)
	waitForProbePodRunning(ctx, t, clientset, namespace, controlPodName)

	controlLogs, err := waitForProbeCompletion(ctx, clientset, namespace, controlPodName, *sandboxTimeout)
	if err != nil {
		t.Fatalf("Control pod probe did not complete: %v", err)
	}
	control := parseProbeLogs(controlLogs)
	t.Logf("Kernel identity: sandboxed=%+v control=%+v", sandboxed.Kernel, control.Kernel)

	t.Run("SchedulingAndExecution", func(t *testing.T) {
		if !sandboxed.SchedulingPassed || !sandboxed.ProbeCompleted {
			t.Fatalf("Sandboxed workload failed to schedule or execute the probe to completion. Raw logs:\n%s", sandboxedLogs)
		}
		t.Logf("PASS: Sandboxed workload scheduled and executed on node %s (kernel release: %s)", sandboxNode, sandboxed.Kernel.Release)
	})

	t.Run("ProcessIsolation", func(t *testing.T) {
		if !sandboxed.PidIsolationPassed {
			t.Fatalf("Process isolation check failed: host processes detected or PID namespace leaked. Raw logs:\n%s", sandboxedLogs)
		}
		t.Logf("PASS: Process isolation verified (visible PIDs: %d)", sandboxed.PidCount)
	})

	t.Run("KernelAndMemoryIsolation", func(t *testing.T) {
		if !sandboxed.KernelIsolationPassed {
			t.Fatalf("Kernel/memory isolation check failed: direct host memory or kernel interface access was permitted. Raw logs:\n%s", sandboxedLogs)
		}
		differs, fields := kernelIdentitiesDiffer(sandboxed.Kernel, control.Kernel)
		if !differs {
			t.Fatalf("Sandboxed workload observes the same kernel identity as an unsandboxed pod on node %s (%+v). "+
				"The runtime handler %q does not appear to provide a kernel boundary; a sandbox such as gVisor or Kata exposes a guest kernel distinct from the host. "+
				"Sandboxed logs:\n%s\nControl logs:\n%s", sandboxNode, sandboxed.Kernel, solution.Handler, sandboxedLogs, controlLogs)
		}
		t.Logf("PASS: Kernel and memory isolation boundary verified (kernel identity differs from host in: %s)", strings.Join(fields, ", "))
	})

	t.Run("FilesystemIsolation", func(t *testing.T) {
		if !sandboxed.FsIsolationPassed {
			t.Fatalf("Filesystem isolation check failed: host filesystem paths accessible. Raw logs:\n%s", sandboxedLogs)
		}
		t.Logf("PASS: Filesystem isolation boundary verified")
	})

	t.Run("NetworkIsolation", func(t *testing.T) {
		if !sandboxed.NetIsolationPassed {
			t.Fatalf("Network isolation check failed: host network interfaces detected in sandbox. Raw logs:\n%s", sandboxedLogs)
		}
		t.Logf("PASS: Network namespace isolation verified (interfaces: %s)", sandboxed.Interfaces)
	})
}

// discoverSandboxingSolution identifies whether a sandboxed RuntimeClass or agent-sandbox is available.
func discoverSandboxingSolution(
	ctx context.Context,
	clientset kubernetes.Interface,
	requestedType string,
	requestedClass string,
	logf func(string, ...any),
) (*sandboxingSolution, error) {
	switch requestedType {
	case sandboxTypeRuntimeClass:
		return resolveRuntimeClassSolution(ctx, clientset, requestedClass, logf)
	case sandboxTypeAgentSandbox:
		return resolveAgentSandboxSolution(ctx, clientset, requestedClass, logf)
	case sandboxTypeAuto:
		if requestedClass != "" {
			return resolveRuntimeClassSolution(ctx, clientset, requestedClass, logf)
		}
		if sol, err := resolveRuntimeClassSolution(ctx, clientset, "", logf); err == nil {
			return sol, nil
		}
		if sol, err := resolveAgentSandboxSolution(ctx, clientset, "", logf); err == nil {
			return sol, nil
		}
		return nil, errNoSandboxingSolution
	default:
		return nil, fmt.Errorf("invalid -sandbox-type %q; supported types: %q, %q, %q", requestedType, sandboxTypeAuto, sandboxTypeRuntimeClass, sandboxTypeAgentSandbox)
	}
}

// resolveRuntimeClassSolution finds an appropriate RuntimeClass for sandboxing.
func resolveRuntimeClassSolution(ctx context.Context, clientset kubernetes.Interface, requestedClass string, logf func(string, ...any)) (*sandboxingSolution, error) {
	if requestedClass != "" {
		rc, err := clientset.NodeV1().RuntimeClasses().Get(ctx, requestedClass, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("specified RuntimeClass %q not found: %w", requestedClass, err)
		}
		logf("Found specified sandboxed RuntimeClass %q (handler: %s)", rc.Name, rc.Handler)
		return &sandboxingSolution{
			SolutionType:     sandboxTypeRuntimeClass,
			RuntimeClassName: rc.Name,
			Handler:          rc.Handler,
		}, nil
	}

	rcList, err := clientset.NodeV1().RuntimeClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list RuntimeClasses: %w", err)
	}

	for _, rc := range rcList.Items {
		if isKnownSandboxedRuntime(&rc) {
			logf("Discovered sandboxed RuntimeClass %q (handler: %s)", rc.Name, rc.Handler)
			return &sandboxingSolution{
				SolutionType:     sandboxTypeRuntimeClass,
				RuntimeClassName: rc.Name,
				Handler:          rc.Handler,
			}, nil
		}
	}

	return nil, errors.New("no known sandboxed RuntimeClass found")
}

// resolveAgentSandboxSolution checks for the kubernetes-sigs/agent-sandbox API
// group. requestedClass, if set, is the RuntimeClass the Sandbox's Pod runs
// with; agent-sandbox itself only orchestrates Pods and relies on the runtime
// handler for kernel isolation.
func resolveAgentSandboxSolution(ctx context.Context, clientset kubernetes.Interface, requestedClass string, logf func(string, ...any)) (*sandboxingSolution, error) {
	groups, err := clientset.Discovery().ServerGroups()
	if err != nil {
		return nil, fmt.Errorf("failed to discover server groups: %w", err)
	}

	version := ""
	for _, g := range groups.Groups {
		if g.Name != agentSandboxAPIGroup {
			continue
		}
		version = g.PreferredVersion.Version
		if version == "" && len(g.Versions) > 0 {
			version = g.Versions[0].Version
		}
		break
	}
	if version == "" {
		return nil, fmt.Errorf("API group %s not found", agentSandboxAPIGroup)
	}

	gvr := schema.GroupVersionResource{Group: agentSandboxAPIGroup, Version: version, Resource: "sandboxes"}
	logf("Discovered agent-sandbox API %s", gvr.String())

	sol := &sandboxingSolution{
		SolutionType:    sandboxTypeAgentSandbox,
		AgentSandboxGVR: &gvr,
	}
	if requestedClass == "" {
		logf("WARNING: no -sandbox-runtime-class given; the Sandbox's Pod runs under the cluster's default runtime handler, which must itself provide a kernel boundary for the KernelAndMemoryIsolation subtest to pass")
		return sol, nil
	}
	rc, err := clientset.NodeV1().RuntimeClasses().Get(ctx, requestedClass, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("specified RuntimeClass %q not found: %w", requestedClass, err)
	}
	sol.RuntimeClassName = rc.Name
	sol.Handler = rc.Handler
	return sol, nil
}

// isKnownSandboxedRuntime checks whether a RuntimeClass corresponds to known sandboxed runtimes.
func isKnownSandboxedRuntime(rc *nodev1.RuntimeClass) bool {
	if rc == nil {
		return false
	}
	name := strings.ToLower(rc.Name)
	handler := strings.ToLower(rc.Handler)

	knownKeywords := []string{
		"gvisor",
		"runsc",
		"kata",
		"sandboxed",
		"quark",
		"krun",
	}

	for _, kw := range knownKeywords {
		if strings.Contains(name, kw) || strings.Contains(handler, kw) {
			return true
		}
	}
	return false
}

// buildProbePod constructs a Pod running the isolation prober. runtimeClassName
// selects the sandbox (empty for a plain, unsandboxed pod); nodeName, if set,
// pins the pod to a node.
func buildProbePod(ns, name, runtimeClassName, nodeName, image string) *corev1.Pod {
	var rcName *string
	if runtimeClassName != "" {
		rcName = &runtimeClassName
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				"ai-conformance.kubernetes.io/test": "workload-sandboxing",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: rcName,
			NodeName:         nodeName,
			RestartPolicy:    corev1.RestartPolicyNever,
			Containers:       []corev1.Container{sandboxProbeContainerSpec(image)},
		},
	}
}

func sandboxProbeContainerSpec(image string) corev1.Container {
	return corev1.Container{
		Name:    sandboxProbeContainer,
		Image:   image,
		Command: []string{"/bin/sh", "-c"},
		Args:    []string{sandboxProbeScript() + "\nsleep 3600"},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("200m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
		},
	}
}

// buildAgentSandboxCR creates an unstructured Sandbox resource for kubernetes-sigs/agent-sandbox.
func buildAgentSandboxCR(ns, name string, gvr schema.GroupVersionResource, runtimeClassName, image string) *unstructured.Unstructured {
	podSpec := map[string]interface{}{
		"restartPolicy": "Never",
		"containers": []interface{}{
			map[string]interface{}{
				"name":    sandboxProbeContainer,
				"image":   image,
				"command": []interface{}{"/bin/sh", "-c"},
				"args":    []interface{}{sandboxProbeScript() + "\nsleep 3600"},
				"resources": map[string]interface{}{
					"requests": map[string]interface{}{
						"cpu":    "50m",
						"memory": "64Mi",
					},
					"limits": map[string]interface{}{
						"cpu":    "200m",
						"memory": "128Mi",
					},
				},
			},
		},
	}
	if runtimeClassName != "" {
		podSpec["runtimeClassName"] = runtimeClassName
	}

	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvr.Group + "/" + gvr.Version,
			"kind":       "Sandbox",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": ns,
				"labels": map[string]interface{}{
					"ai-conformance.kubernetes.io/test": "workload-sandboxing",
				},
			},
			"spec": map[string]interface{}{
				"podTemplate": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"ai-conformance.kubernetes.io/test": "workload-sandboxing",
						},
					},
					"spec": podSpec,
				},
			},
		},
	}
}

// waitForAgentSandboxPod locates the Pod owned by an agent-sandbox Sandbox.
func waitForAgentSandboxPod(ctx context.Context, c kubernetes.Interface, ns, sandboxName string, timeout time.Duration) (string, error) {
	var podName string
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		pods, err := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil
		for _, p := range pods.Items {
			for _, owner := range p.OwnerReferences {
				if owner.Kind == "Sandbox" && owner.Name == sandboxName && strings.HasPrefix(owner.APIVersion, agentSandboxAPIGroup+"/") {
					podName = p.Name
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		return "", fmt.Errorf("no Pod owned by Sandbox %s appeared within %s%s: %w", sandboxName, timeout, lastAPIErrorSuffix(lastAPIError), err)
	}
	return podName, nil
}

// waitForProbePodRunning waits for a probe pod to reach Running and returns it.
func waitForProbePodRunning(ctx context.Context, t *testing.T, c kubernetes.Interface, ns, name string) *corev1.Pod {
	t.Helper()
	t.Logf("Waiting for pod %s to reach Running phase...", name)
	running, err := waitForPodsRunning(ctx, c, ns, []string{name}, *sandboxTimeout)
	if err != nil {
		phase := "unknown"
		if p, getErr := c.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{}); getErr == nil {
			phase = string(p.Status.Phase)
		}
		t.Fatalf("Pod %s failed to reach Running phase within %v (current phase: %s): %v", name, *sandboxTimeout, phase, err)
	}
	return running[name]
}

// waitForProbeCompletion reads the prober container's logs until the
// completion marker appears. On timeout it returns the partial logs along
// with the error so the caller can surface them.
func waitForProbeCompletion(ctx context.Context, c kubernetes.Interface, ns, podName string, timeout time.Duration) (string, error) {
	var logs string
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		raw, err := c.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{Container: sandboxProbeContainer}).DoRaw(ctx)
		if err != nil {
			lastAPIError = err
			return false, nil
		}
		lastAPIError = nil
		logs = string(raw)
		return strings.Contains(logs, sandboxProbeCompletedMarker), nil
	})
	if err != nil {
		return logs, fmt.Errorf("probe in pod %s did not report %q within %s%s: %w\nPartial logs:\n%s",
			podName, sandboxProbeCompletedMarker, timeout, lastAPIErrorSuffix(lastAPIError), err, logs)
	}
	return logs, nil
}

func deletePodInBackground(ctx context.Context, c kubernetes.Interface, ns, name string) {
	deletePolicy := metav1.DeletePropagationBackground
	_ = c.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
}

// kernelIdentitiesDiffer reports whether the sandboxed workload observed a
// kernel identity distinct from the unsandboxed control pod, and which fields
// differed. A field that either side could not read is ignored, so a runtime
// that does not expose boot_id is judged on release and version alone.
func kernelIdentitiesDiffer(sandboxed, control kernelIdentity) (bool, []string) {
	var differing []string
	compare := func(field, a, b string) {
		if a == "" || b == "" {
			return
		}
		if a != b {
			differing = append(differing, field)
		}
	}
	compare("release", sandboxed.Release, control.Release)
	compare("version", sandboxed.Version, control.Version)
	compare("boot_id", sandboxed.BootID, control.BootID)
	return len(differing) > 0, differing
}

// parseProbeLogs parses the output of the sandbox probe script into structured results.
func parseProbeLogs(logs string) *sandboxProbeResults {
	res := &sandboxProbeResults{RawLogs: logs}
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "SANDBOX_PROBE: SCHEDULING=PASS":
			res.SchedulingPassed = true
		case line == "SANDBOX_PROBE: PID_ISOLATION=PASS":
			res.PidIsolationPassed = true
		case line == "SANDBOX_PROBE: KERNEL_ISOLATION=PASS":
			res.KernelIsolationPassed = true
		case line == "SANDBOX_PROBE: FS_ISOLATION=PASS":
			res.FsIsolationPassed = true
		case line == "SANDBOX_PROBE: NET_ISOLATION=PASS":
			res.NetIsolationPassed = true
		case line == sandboxProbeCompletedMarker:
			res.ProbeCompleted = true
		case strings.HasPrefix(line, "SANDBOX_INFO: KERNEL_RELEASE="):
			res.Kernel.Release = probeInfoValue(line, "SANDBOX_INFO: KERNEL_RELEASE=")
		case strings.HasPrefix(line, "SANDBOX_INFO: KERNEL_VERSION="):
			res.Kernel.Version = probeInfoValue(line, "SANDBOX_INFO: KERNEL_VERSION=")
		case strings.HasPrefix(line, "SANDBOX_INFO: BOOT_ID="):
			res.Kernel.BootID = probeInfoValue(line, "SANDBOX_INFO: BOOT_ID=")
		case strings.HasPrefix(line, "SANDBOX_INFO: INTERFACES="):
			res.Interfaces = probeInfoValue(line, "SANDBOX_INFO: INTERFACES=")
		case strings.HasPrefix(line, "SANDBOX_INFO: PID_COUNT="):
			if count, err := strconv.Atoi(probeInfoValue(line, "SANDBOX_INFO: PID_COUNT=")); err == nil {
				res.PidCount = count
			}
		}
	}
	return res
}

// probeInfoValue extracts an info value; the script prints "unknown" for
// values it could not read, which is normalized to "" (no information).
func probeInfoValue(line, prefix string) string {
	v := strings.TrimSpace(strings.TrimPrefix(line, prefix))
	if v == "unknown" {
		return ""
	}
	return v
}

// sandboxProbeScript returns the shell script executed inside the probe
// container. The same script runs in the sandboxed workload and in the
// unsandboxed control pod.
func sandboxProbeScript() string {
	return `echo "SANDBOX_PROBE: SCHEDULING=PASS"

# 0. Kernel identity: a sandbox with its own kernel reports values distinct from the host.
echo "SANDBOX_INFO: KERNEL_RELEASE=$(uname -r 2>/dev/null || echo unknown)"
echo "SANDBOX_INFO: KERNEL_VERSION=$(cat /proc/version 2>/dev/null || echo unknown)"
echo "SANDBOX_INFO: BOOT_ID=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo unknown)"

# 1. PID Isolation: check visible process count and ensure no host daemons.
# Match on comm (the executable name) rather than cmdline: this script is
# PID 1's cmdline, so grepping cmdline for the daemon names matches itself.
pid_pass=1
pid_count=0
if [ -d /proc ]; then
  pid_count=$(ls -d /proc/[0-9]* 2>/dev/null | wc -l)
  for p in /proc/[0-9]*/comm; do
    [ -f "$p" ] || continue
    read -r comm < "$p" 2>/dev/null || continue
    case "$comm" in
      kubelet|containerd|containerd-shim*|dockerd|crio|systemd-journal) pid_pass=0 ;;
    esac
  done
fi
echo "SANDBOX_INFO: PID_COUNT=$pid_count"
if [ "$pid_pass" -eq 1 ]; then
  echo "SANDBOX_PROBE: PID_ISOLATION=PASS"
else
  echo "SANDBOX_PROBE: PID_ISOLATION=FAIL"
fi

# 2. Kernel & Memory Isolation: check raw memory device access
kernel_pass=1
if head -c 1 /dev/mem 2>/dev/null; then
  kernel_pass=0
fi
if head -c 1 /dev/kmem 2>/dev/null; then
  kernel_pass=0
fi
if [ "$kernel_pass" -eq 1 ]; then
  echo "SANDBOX_PROBE: KERNEL_ISOLATION=PASS"
else
  echo "SANDBOX_PROBE: KERNEL_ISOLATION=FAIL"
fi

# 3. Filesystem Isolation: ensure host paths are not mounted
fs_pass=1
for host_path in /etc/kubernetes /var/lib/kubelet /var/log/pods; do
  if [ -d "$host_path" ]; then
    fs_pass=0
  fi
done
if [ "$fs_pass" -eq 1 ]; then
  echo "SANDBOX_PROBE: FS_ISOLATION=PASS"
else
  echo "SANDBOX_PROBE: FS_ISOLATION=FAIL"
fi

# 4. Network Isolation: ensure host interfaces/bridges are not present.
# Prefer /proc/net/dev: sandboxes with a minimal sysfs (gVisor) do not
# populate /sys/class/net.
net_pass=1
iface_list=""
if [ -r /proc/net/dev ]; then
  iface_list=$(awk -F: 'NR > 2 { gsub(/ /, "", $1); printf "%s ", $1 }' /proc/net/dev 2>/dev/null)
elif [ -d /sys/class/net ]; then
  iface_list=$(ls /sys/class/net 2>/dev/null | tr '\n' ' ')
fi
for b in $iface_list; do
  case "$b" in
    docker0|cbr0|flannel*|cni0|br-*|bond*|dummy*) net_pass=0 ;;
  esac
done
echo "SANDBOX_INFO: INTERFACES=$iface_list"
if [ "$net_pass" -eq 1 ]; then
  echo "SANDBOX_PROBE: NET_ISOLATION=PASS"
else
  echo "SANDBOX_PROBE: NET_ISOLATION=FAIL"
fi

echo "` + sandboxProbeCompletedMarker + `"
`
}

// getDynamicClient creates a dynamic Kubernetes client using the kubeconfig flag.
func getDynamicClient(t *testing.T) dynamic.Interface {
	t.Helper()
	dynamicClient, err := dynamic.NewForConfig(getRESTConfig(t))
	if err != nil {
		t.Fatalf("Error creating dynamic kubernetes client: %v", err)
	}
	return dynamicClient
}
