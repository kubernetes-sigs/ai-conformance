## Prerequisites
To run these AI Conformance tests, you must have:

- Golang: Installed on your local machine.
- Kubeconfig: A valid kubeconfig file with cluster-admin permissions for the target cluster.
- Accelerator Node Pool: The cluster must have nodes with accelerators exposed through the Kubernetes resource management framework — either a DRA driver (ResourceClaims against a DeviceClass such as `gpu.nvidia.com`) or a device plugin (extended resources such as `nvidia.com/gpu`). Make sure your nodes allow testing pods to be scheduled on them (e.g. no taints that prevent scheduling).
- Cluster Autoscaling Test: `TestAcceleratorClusterAutoscaling` additionally requires a running cluster autoscaler and an isolated accelerator pool with minimum size `N >= 1`, maximum size at least `N+1`, effective capacity for exactly one requested accelerator per baseline node, scale-down enabled, sufficient cloud quota/stock, and one stable node label inherited by new pool nodes. The pool must contain no non-DaemonSet workloads or other Pending Pods explicitly selecting the pool. Device-plugin mode permits unrelated running accelerator workloads outside the pool but rejects other Pending Pods requesting the configured extended resource. DRA mode requires no other active Pods with ResourceClaims or allocated ResourceClaims outside the test namespace while the test runs because DRA devices may use shared topology.
- Workload Sandboxing Test: `TestWorkloadSandboxing` requires a sandboxed `RuntimeClass` (e.g. gVisor or Kata Containers) or the [`agent-sandbox`](https://github.com/kubernetes-sigs/agent-sandbox) API, and is skipped if neither is detected. The node that runs the sandboxed workload must also admit a plain, unsandboxed pod pinned to it (the test's control pod).
- Network Access: The test machine must be able to reach the Kubernetes API server.

## Running the Tests

Run the tests using the standard go test command. You should run tests with the same release tag as your cluster version, to ensure compatibility with the Kubernetes AI conformance program.

By default, the test looks for your kubeconfig at `~/.kube/config`. You can override this by setting the `KUBECONFIG` environment variable or using the `-kubeconfig` flag.

```bash
export KUBECONFIG=/path/to/my/config # Optional
go test -v ./test [-run <TestName>] [flags...]
```

Run `go test ./test -args -help` for details about each flag.

The allocation-mode detection, pod-construction, and device-probe logic also has hermetic unit tests that need no cluster (Kubernetes API tests use a fake clientset):

```bash
go test -v -short ./test
```

### Test Cases Covered

| Test Name | Requirement Covered | Requirement Level |
|-|-|-|
| `TestSecureAcceleratorAccess` | Secure Accelerator Access | MUST |
| `TestGangScheduling` | Gang Scheduling | MUST |
| `TestAcceleratorClusterAutoscaling` | Effective Cluster Autoscaling for Accelerators | MUST |
| `TestWorkloadSandboxing` | Workload Sandboxing for Untrusted Code | SHOULD |

### Workload Sandboxing

The workload sandboxing test (`TestWorkloadSandboxing`) verifies that the platform provides a sandboxing mechanism (such as standard Kubernetes `RuntimeClass` resources like `gvisor`, `kata`, or controllers like [`kubernetes-sigs/agent-sandbox`](https://github.com/kubernetes-sigs/agent-sandbox)) that isolates untrusted workload execution from the host node kernel, process, memory, filesystem, and network namespaces.

The test defaults to auto-detection (`-sandbox-type=auto`):
1. If `-sandbox-runtime-class` is provided, it verifies and uses that `RuntimeClass`.
2. Otherwise, it detects available sandboxed `RuntimeClass` objects (e.g., `gvisor`, `runsc`, `kata`, `sandboxed`, `quark`, `krun`) or the `agents.x-k8s.io` API group.
3. If no sandboxing mechanism is detected and no flag is set, the test is skipped with guidance on configuring flags or opting out if the capability is N/A.

To run with an explicit RuntimeClass:
```bash
go test -v ./test \
  -run TestWorkloadSandboxing \
  -sandbox-runtime-class=gvisor
```

To run with agent-sandbox (`-sandbox-runtime-class` selects the RuntimeClass the Sandbox's Pod runs with; agent-sandbox orchestrates Pods and relies on the runtime handler for kernel isolation):
```bash
go test -v ./test \
  -run TestWorkloadSandboxing \
  -sandbox-type=agent-sandbox \
  -sandbox-runtime-class=gvisor
```

The test runs a probe script in the sandboxed workload and then runs the same script in an unsandboxed control pod pinned to the node the sandboxed workload landed on. The subtests verify observable isolation boundaries:
- `SchedulingAndExecution`: The sandboxed workload schedules, and the probe runs to completion inside the sandbox runtime.
- `ProcessIsolation`: Host processes (e.g., `kubelet`, `containerd`) are not visible and the PID namespace is strictly container-scoped.
- `KernelAndMemoryIsolation`: Host physical/kernel memory access (`/dev/mem`, `/dev/kmem`) is restricted, and the sandboxed workload observes a kernel identity (kernel release, `/proc/version`, boot ID) distinct from the control pod on the same node. A plain container shares the host kernel identity; a user-space kernel (gVisor) or microVM guest kernel (Kata) does not.
- `FilesystemIsolation`: Host filesystem paths are not mounted into the sandbox.
- `NetworkIsolation`: The sandboxed workload is restricted to its own network namespace, with no host bridge or CNI interfaces visible.

### Accelerator Cluster Autoscaling

The autoscaling test observes a preconfigured accelerator pool. The test requires an explicit `-autoscaler-node-pool-label` flag and is **skipped by default** if the flag is unset. 

If your platform provides cluster autoscaling, you must set this flag and run the test. If cluster autoscaling is not supported (N/A), you can leave the flag unset to skip the test.

Scale-up and scale-down can take significantly longer than Go's default test timeout, so it is recommended to use `-timeout 75m` or a larger value. The test also includes configurable observation windows (`-autoscaler-scale-up-timeout`, `-autoscaler-scale-down-timeout`, etc.) since node provisioning times vary heavily by cloud provider. 

Run `go test ./test -args -help` for details on all supported flags.

## Vendor Customization & Neutrality

The tests are designed to be vendor-neutral where possible, but hardware-level probing often requires vendor-specific configuration. If your platform uses different hardware/software not covered by the tests, please file an issue to request support for your hardware/software. In the meantime, you will need to certify manually.

### Opting Out (N/A Requirements)

If a specific requirement is not applicable to your platform (i.e., you answered "N/A" in the conformance checklist), you may skip or "opt-out" of that specific sub-test. To skip tests during execution, you can use test-specific flags (if available) or use the `go test -run <regex>` flag to select only the applicable tests. When submitting your results, ensure that you provide a clear explanation for why the requirement is considered N/A.

## Automation, CI & Certification Submissions

For CI environments and certification submissions, you can output the results in both human-readable and machine-readable formats. Use the following commands to generate the required artifacts for your `KubernetesAIConformance-1.NN.yaml` evidence:

### Generating Test Artifacts

```bash
# 1. Capture full output log (e2e.log)
go test -v ./test | tee e2e.log

# 2. Generate machine-readable JSON (results.json)
go test -v ./test -json > results.json

# 3. Generate JUnit XML report (junit.xml)
# Option A: Using gotestsum (Recommended - runs via go run)
go run gotest.tools/gotestsum@latest --junitfile junit.xml -- ./test
# Option B: Using go-junit-report
go test -v ./test 2>&1 | go run github.com/jstemmer/go-junit-report/v2@latest > junit.xml
```

### Using Test Results for Certification (Hybrid Approach)

Starting with v1.37, it is **recommended** that any requirement covered by an automated test is verified using this test suite. When submitting your conformance results, you can use the generated `e2e.log` and `results.json` (or `junit.xml`) as evidence for these automated test cases. Link them in your `KubernetesAIConformance-1.NN.yaml` file under the respective requirement's `evidence` field.


