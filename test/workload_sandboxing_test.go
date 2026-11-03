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
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

const (
	sandboxProbeContainer       = "prober"
	sandboxProbeImage           = "busybox"
	sandboxProbeCompletedMarker = "SANDBOX_PROBE: COMPLETED"
)

var (
	sandboxRuntimeClass *string
	sandboxTimeout      *time.Duration

	errNoSandboxedRuntimeClass = errors.New("no known sandboxed RuntimeClass found")
)

func init() {
	registerFlagGroup("sandbox", "Workload Sandboxing Flags (TestWorkloadSandboxing)")
	sandboxRuntimeClass = flag.String("sandbox-runtime-class", "",
		"Name of the RuntimeClass configured for workload sandboxing (e.g. gvisor, kata, runsc). If empty, auto-detection will be performed.")
	sandboxTimeout = flag.Duration("sandbox-timeout", 3*time.Minute,
		"Timeout for each step of the sandboxing test: the sandboxed workload (and its unsandboxed control pod) reaching Running, and the probe script completing.")
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
// It verifies that the platform provides a sandboxed RuntimeClass (e.g. gVisor
// or Kata) that isolates untrusted workload execution from the host node
// kernel, process, filesystem, and network namespaces.
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
	rc, err := resolveSandboxedRuntimeClass(ctx, clientset, *sandboxRuntimeClass, t.Logf)
	if err != nil {
		if errors.Is(err, errNoSandboxedRuntimeClass) {
			t.Skipf("Skipping workload sandboxing test: %v. Specify -sandbox-runtime-class to run.", err)
		}
		t.Fatalf("Failed to resolve sandboxed RuntimeClass: %v", err)
	}
	t.Logf("Running workload sandboxing test with RuntimeClass %q (handler: %q)", rc.Name, rc.Handler)

	namespace := randomNamespaceName("sandbox-conformance")
	if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create namespace %s: %v", namespace, err)
	}
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: Failed to delete namespace %s: %v", namespace, err)
		}
	})

	// Sandboxed workload. It goes through the scheduler so the RuntimeClass's
	// scheduling constraints (nodeSelector, tolerations) apply.
	sandboxedPodName := "sandboxed-probe-pod"
	t.Logf("Creating sandboxed pod %s/%s with RuntimeClass %q...", namespace, sandboxedPodName, rc.Name)
	createTestPod(ctx, t, clientset, buildProbePod(namespace, sandboxedPodName, rc.Name, ""))
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
	t.Logf("Creating unsandboxed control pod %s/%s on node %s...", namespace, controlPodName, sandboxNode)
	createTestPod(ctx, t, clientset, buildProbePod(namespace, controlPodName, "", sandboxNode))
	waitForProbePodRunning(ctx, t, clientset, namespace, controlPodName)

	controlLogs, err := waitForProbeCompletion(ctx, clientset, namespace, controlPodName, *sandboxTimeout)
	if err != nil {
		t.Fatalf("Control pod probe did not complete: %v", err)
	}
	control := parseProbeLogs(controlLogs)
	t.Logf("Kernel identity: sandboxed=%+v control=%+v", sandboxed.Kernel, control.Kernel)

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
				"Sandboxed logs:\n%s\nControl logs:\n%s", sandboxNode, sandboxed.Kernel, rc.Handler, sandboxedLogs, controlLogs)
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

// resolveSandboxedRuntimeClass returns the RuntimeClass to test: the requested
// one if given, otherwise the first RuntimeClass that looks like a known
// sandboxed runtime. It returns errNoSandboxedRuntimeClass when nothing was
// requested and nothing was detected.
func resolveSandboxedRuntimeClass(ctx context.Context, clientset kubernetes.Interface, requestedClass string, logf func(string, ...any)) (*nodev1.RuntimeClass, error) {
	if requestedClass != "" {
		rc, err := clientset.NodeV1().RuntimeClasses().Get(ctx, requestedClass, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("specified RuntimeClass %q not found: %w", requestedClass, err)
		}
		logf("Found specified sandboxed RuntimeClass %q (handler: %s)", rc.Name, rc.Handler)
		return rc, nil
	}

	rcList, err := clientset.NodeV1().RuntimeClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list RuntimeClasses: %w", err)
	}
	for i := range rcList.Items {
		rc := &rcList.Items[i]
		if isKnownSandboxedRuntime(rc) {
			logf("Discovered sandboxed RuntimeClass %q (handler: %s)", rc.Name, rc.Handler)
			return rc, nil
		}
	}
	return nil, errNoSandboxedRuntimeClass
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
func buildProbePod(ns, name, runtimeClassName, nodeName string) *corev1.Pod {
	var rcName *string
	if runtimeClassName != "" {
		rcName = &runtimeClassName
	}
	var tolerations []corev1.Toleration
	if nodeName != "" {
		// A pinned pod bypasses the scheduler and so NoSchedule taints, but a
		// NoExecute taint on a dedicated sandbox node pool would still evict
		// it. Tolerate everything; this is a short-lived control pod.
		tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
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
			Tolerations:      tolerations,
			RestartPolicy:    corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    sandboxProbeContainer,
				Image:   sandboxProbeImage,
				Command: []string{"/bin/sh", "-c"},
				// Keep the container alive after the probe so the pod stays
				// Running while the logs are collected.
				Args: []string{sandboxProbeScript() + "\nsleep 3600"},
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
			}},
		},
	}
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
	return `# 0. Kernel identity: a sandbox with its own kernel reports values distinct from the host.
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
