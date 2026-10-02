## Prerequisites
To run these AI Conformance tests, you must have:

- Golang: Installed on your local machine.
- Kubeconfig: A valid kubeconfig file with cluster-admin permissions for the target cluster.
- Accelerator Node Pool: The cluster must have nodes with accelerators exposed through the Kubernetes resource management framework — either a DRA driver (ResourceClaims against a DeviceClass such as `gpu.nvidia.com`) or a device plugin (extended resources such as `nvidia.com/gpu`). Make sure your nodes allow testing pods to be scheduled on them (e.g. no taints that prevent scheduling).
- Cluster Autoscaling Test: `TestAcceleratorClusterAutoscaling` additionally requires a running cluster autoscaler and an isolated accelerator pool with minimum size `N >= 1`, maximum size at least `N+1`, effective capacity for exactly one requested accelerator per baseline node, scale-down enabled, sufficient cloud quota/stock, and one stable node label inherited by new pool nodes. The pool must contain no non-DaemonSet workloads or other Pending Pods explicitly selecting the pool. Device-plugin mode permits unrelated running accelerator workloads outside the pool but rejects other Pending Pods requesting the configured extended resource. DRA mode requires no other active Pods with ResourceClaims or allocated ResourceClaims outside the test namespace while the test runs because DRA devices may use shared topology.
- Driver & Runtime Test: `TestAcceleratorDriverRuntimeManagement` additionally pulls a vanilla container image (`ubuntu:22.04` by default; override with `-driver-runtime-image` for air-gapped registries) and expects the platform's container runtime configuration to inject the accelerator vendor's tools (`nvidia-smi` for NVIDIA) into accelerator workloads.
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
| `TestAcceleratorDriverRuntimeManagement` | Accelerator Driver & Runtime Management | SHOULD |
| `TestSecureAcceleratorAccess` | Secure Accelerator Access | MUST |
| `TestGangScheduling` | Gang Scheduling | MUST |
| `TestAcceleratorClusterAutoscaling` | Effective Cluster Autoscaling for Accelerators | MUST |

### Accelerator Driver & Runtime Management

`TestAcceleratorDriverRuntimeManagement` verifies KAR-0001 (`driver_runtime_management`) on the accelerator node resolved by allocation-mode detection and on the node its probe Pod lands on:

1. `VerifiableMechanismExposed`: resolves what the platform advertises about the accelerator driver on the node. DRA `ResourceSlice` device attributes are preferred in every allocation mode (for NVIDIA: `driverVersion` and `cudaDriverVersion`); Node labels or annotations are the fallback (for NVIDIA: the GPU Feature Discovery labels `nvidia.com/cuda.driver-version.full` and `nvidia.com/cuda.runtime-version.full`). It also records the node's `status.nodeInfo.containerRuntimeVersion`, the accelerator presence label, and the accelerator `RuntimeClass`. Missing version metadata is logged as a `WARNING` and does not fail the subtest; a node that reports no container runtime does.
2. `DriverAndRuntimeCompatibility`: runs an accelerator-requesting Pod built from a vanilla image (`ubuntu:22.04` by default; `-driver-runtime-image` overrides it) and requires that the container sees exactly the requested accelerator devices, that the container runtime configuration (CDI spec or runtime hook) injected the vendor tools (`RUNTIME_CONFIG_OK`), that the driver initializes and reports a version (`DRIVER_FUNCTIONAL`), and that the observed driver and runtime versions match whatever the platform advertises. Versions are compared at the precision they share (`580.65.6` matches `580.65.06`, `12.9.0` matches `12.9`).

"Runtime version" here is the accelerator runtime API version supported by the installed driver (the CUDA driver API version for NVIDIA); the container runtime configuration itself is verified by the injection check. The check is point-in-time: it confirms that the platform's mechanism reflects what is installed on the node during the run.

If `-accelerator-type` names a variant the suite does not recognize, the test logs `RESULT: STATUS=UNKNOWN` and skips instead of failing (KAR-0001 Conformance Strategy). Provide manual verification evidence for the requirement in that case.

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


