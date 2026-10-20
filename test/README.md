## Prerequisites
To run these AI Conformance tests, you must have:

- Golang: Installed on your local machine.
- Kubeconfig: A valid kubeconfig file with cluster-admin permissions for the target cluster.
- Accelerator Node Pool: The cluster must have nodes with accelerators exposed through the Kubernetes resource management framework — either a DRA driver (ResourceClaims against a DeviceClass such as `gpu.nvidia.com`) or a device plugin (extended resources such as `nvidia.com/gpu`). Make sure your nodes allow testing pods to be scheduled on them (e.g. no taints that prevent scheduling).
- Cluster Autoscaling Test: `TestAcceleratorClusterAutoscaling` additionally requires a running cluster autoscaler and an isolated accelerator pool with minimum size `N >= 1`, maximum size at least `N+1`, effective capacity for exactly one requested accelerator per baseline node, scale-down enabled, sufficient cloud quota/stock, and one stable node label inherited by new pool nodes. The pool must contain no non-DaemonSet workloads or other Pending Pods explicitly selecting the pool. Device-plugin mode permits unrelated running accelerator workloads outside the pool but rejects other Pending Pods requesting the configured extended resource. DRA mode requires no other active Pods with ResourceClaims or allocated ResourceClaims outside the test namespace while the test runs because DRA devices may use shared topology.
- AI Service Metrics Test: `TestAIServiceMetrics` needs no accelerator. It requires a monitoring system with a Prometheus-compatible query API, the ability to pull the public `python:3.13-alpine` image, and room to schedule one small Pod (50m CPU, 64Mi memory request).
- Workload Sandboxing Test: `TestWorkloadSandboxing` requires a sandboxed `RuntimeClass` (e.g. gVisor or Kata Containers) and is skipped if none is detected. The node that runs the sandboxed workload must also admit a plain, unsandboxed pod pinned to it (the test's control pod).
- HPA Autoscaling Test: `TestHPAAutoscaling` additionally requires capacity to allocate `-hpa-max-replicas` accelerators (default 2, at least 2) to the test at the same time, one per Pod, plus 50m CPU and 64Mi memory per Pod, a custom metrics pipeline configured as described in [HPA Autoscaling for AI Workloads](#hpa-autoscaling-for-ai-workloads), and the ability to pull `docker.io/library/python:3.13-alpine` (or a mirror of it set with `-hpa-workload-image`).
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
| `TestRobustCRDControllerOperation` | Robust CRD and Controller Operation | MUST |
| `TestAIServiceMetrics` | AI Job & Inference Service Metrics | MUST |
| `TestWorkloadSandboxing` | Workload Sandboxing for Untrusted Code | SHOULD |
| `TestHPAAutoscaling` | Effective HPA Autoscaling for AI Workloads | MUST |

### Workload Sandboxing

The workload sandboxing test (`TestWorkloadSandboxing`) verifies that the platform provides a sandboxed `RuntimeClass` (e.g. `gvisor`, `kata`) that isolates untrusted workload execution from the host node kernel, process, memory, filesystem, and network namespaces.

By default the test looks for a `RuntimeClass` whose name or handler matches a known sandboxed runtime (`gvisor`, `runsc`, `kata`, `sandboxed`, `quark`, `krun`). To use a specific `RuntimeClass` (for example one with a platform-specific name, or the one your [`agent-sandbox`](https://github.com/kubernetes-sigs/agent-sandbox) Sandboxes run with), pass it explicitly:
```bash
go test -v ./test \
  -run TestWorkloadSandboxing \
  -sandbox-runtime-class=gvisor
```

If no sandboxed `RuntimeClass` is detected and none is specified, the test is skipped with guidance on configuring the flag or opting out if the capability is N/A.

The test runs a probe script in the sandboxed pod and then runs the same script in an unsandboxed control pod pinned to the node the sandboxed pod landed on. The subtests verify observable isolation boundaries:
- `ProcessIsolation`: Host processes (e.g., `kubelet`, `containerd`) are not visible and the PID namespace is strictly container-scoped.
- `KernelAndMemoryIsolation`: Host physical/kernel memory access (`/dev/mem`, `/dev/kmem`) is restricted, and the sandboxed pod observes a kernel identity (kernel release, `/proc/version`, boot ID) distinct from the control pod on the same node. A plain container shares the host kernel identity; a user-space kernel (gVisor) or microVM guest kernel (Kata) does not.
- `FilesystemIsolation`: Host filesystem paths are not mounted into the sandbox.
- `NetworkIsolation`: The sandboxed pod is restricted to its own network namespace, with no host bridge or CNI interfaces visible.

### Accelerator Cluster Autoscaling

The autoscaling test observes a preconfigured accelerator pool. The test requires an explicit `-autoscaler-node-pool-label` flag and is **skipped by default** if the flag is unset. 

If your platform provides cluster autoscaling, you must set this flag and run the test. If cluster autoscaling is not supported (N/A), you can leave the flag unset to skip the test.

Scale-up and scale-down can take significantly longer than Go's default test timeout, so it is recommended to use `-timeout 75m` or a larger value. The test also includes configurable observation windows (`-autoscaler-scale-up-timeout`, `-autoscaler-scale-down-timeout`, etc.) since node provisioning times vary heavily by cloud provider. 

Run `go test ./test -args -help` for details on all supported flags.

### HPA Autoscaling for AI Workloads

`TestHPAAutoscaling` (KAR-0057) verifies that the HorizontalPodAutoscaler scales accelerator-backed Pods on a custom per-Pod metric. It is **skipped by default** and runs when `-hpa-custom-metric-name` is set. If your platform supports the HorizontalPodAutoscaler, configure the pipeline below and run the test; if it does not (N/A), leave the flag unset. The project's own E2E job (`test/e2e/run-e2e-gcp.sh`) provisions one GPU and no custom metrics pipeline, so it skips this test.

#### What the test does

The test creates, in a new namespace, a ConfigMap holding the metric value and a one-replica Deployment whose Pod requests one accelerator (through DRA or a device plugin, as resolved by `-allocation-mode`). The container starts only if it sees exactly one accelerator device, then serves the ConfigMap value as a Prometheus gauge and reports Ready only while that value is a valid number. The gauge stands in for an inference server's per-replica request queue depth. The test sets it directly rather than generating load, so it checks that the HPA acts on such a signal for accelerator Pods; it is not an inference benchmark.

1. **Baseline**: with the metric at 1 (target 10), wait for the Pod to be Ready on an accelerator and for the custom metrics API to report 1 for it. Only then create an `autoscaling/v2` HPA (`minReplicas: 1`, `maxReplicas: -hpa-max-replicas`, `Pods` metric with an `AverageValue` target of 10) and require it to reconcile on the metric with the Deployment at one replica, then, as a negative control, keep deciding on one replica (and the Deployment to stay at one) for one minute. The negative control checks the HPA's decision, not the Pod, so a Pod evicted or restarted meanwhile does not fail it.
2. **Scale-up**: raise the metric to 100. Require the HPA to scale the Deployment to `maxReplicas`, with new Pod UIDs, every Pod Running and Ready with its accelerator, and the custom metrics API reporting 100 for every Pod, including the new ones.
3. **Scale-down**: lower the metric to 2. Require the HPA to return to one replica, one Running and Ready Pod on an accelerator, every Pod the scale-down deleted to be gone (not still Terminating) and, in DRA mode, no ResourceClaim to remain allocated except the remaining Pod's. Pod lifecycle events outside autoscaling are allowed and logged: a Pod its node ended without deleting it (e.g. evicted under node pressure, left in phase Failed); a Pod still terminating after a disruption, which Kubernetes marks with the `DisruptionTarget` condition (eviction API, taint-based eviction, preemption, Pod garbage collection), together with its ResourceClaim; and, if one of the scaled-up Pods was lost in one of these ways, a replacement Pod as the remaining replica. The ReplicaSet deletes the replicas it removes without that condition, so they must always be gone. Clusters older than Kubernetes 1.31 may not set the condition; there a disrupted Pod must also finish terminating. The HPA's scale-down stabilization window is set from `-hpa-scale-down-stabilization`; the time from lowering the metric to the scale-down is logged, not asserted, because how soon a correct HPA may scale down also depends on its controller's sync period.
4. **Cleanup**: delete the HPA, then the Deployment, wait until every workload Pod object is gone and, in DRA mode, every ResourceClaim is deallocated, and delete the namespace.

In every phase the HPA's status must show `ScalingActive=True` and record exactly the phase's metric value as the metric's current average, not just a value on the expected side of the target, so a status left over from another phase cannot satisfy the check. The HPA records the plain mean of its Pods' samples, so once every Pod reports the phase's value it records exactly that value.

What "with its accelerator" and the removal checks prove depends on the allocation mode:

| | DRA | Device plugin |
|-|-|-|
| Each replica holds its own accelerator allocation | The Pod's ResourceClaim was generated for that Pod, is reserved only for it, and has exactly one device from the configured driver, allocated for the Pod's node. No device is allocated to two of the Pods, except as separate shares of a device the driver allows to be allocated more than once (`allowMultipleAllocations`), which the test accepts. | The Pod requests one unit of the extended resource (e.g. `nvidia.com/gpu`), its node advertises it, and no node runs more of the test's Pods than it has allocatable units. Which physical device the device plugin assigned is not visible through the API. |
| Every container sees exactly one device | The container exits, and the Pod never becomes Ready, unless exactly one path matches the accelerator's device-node pattern (`/dev/nvidia[0-9]*` for NVIDIA). This shows the device was exposed to the container; it does not exercise the device. | Same. |
| Removed replicas give their accelerators back | Yes: no ResourceClaim remains allocated for a removed Pod. | Only that the removed Pod objects are gone. The test does not observe the device plugin returning the device to its pool. |

Each phase uses a distinct metric value, and no Pod can expose a value before the test writes it, so a sample with the current phase's value was produced after the test changed it; samples from an earlier phase are rejected. The test does not interpret the timestamps adapters attach to samples, because adapters report different things (prometheus-adapter reports the query time).

Results are reported as follows. `ENVIRONMENT ERROR` means a prerequisite is missing, so the test could not judge the HPA: the custom metrics API is not served or none of its versions is available, the workload metric never reaches it (for the baseline Pod or for the scaled-up Pods), the HPA itself reports it cannot read the metric (`ScalingActive=False` with reason `FailedGetPodsMetric`), Pods are Unschedulable (e.g. too few accelerators), cannot pull or start their container, are ended by their node (phase Failed, e.g. rejected at admission because a device could not be allocated, or evicted), or their container exits (for example because its startup check did not see exactly one accelerator device, or the workload image cannot run the test's script; the container's last output is included), the test cannot create or read its resources, the API server stopped answering for more than a minute at the end of a wait or during the negative control (so the HPA's behavior could not be observed), or the `go test -timeout` ran out. Pod problems while scaling up count as `ENVIRONMENT ERROR` only while the HPA is still deciding on `maxReplicas` for the raised metric. `FAIL` means the prerequisites were in place but the HPA did not behave as required: it did not reconcile on the metric, scaled while the metric was below target, did not scale up to `maxReplicas` or back down, or the replicas it created or removed did not meet the checks above; the HPA's conditions are included. A failed cleanup is reported as `CLEANUP FAILURE`.

#### Custom metrics integration contract

The test does not install a metrics adapter: `custom.metrics.k8s.io` can be served by only one adapter, so installing one would replace the platform's pipeline. The platform must instead provide the following. Before deploying anything, the test checks that `custom.metrics.k8s.io` is served and available, and reports `ENVIRONMENT ERROR` otherwise.

| Stage | Requirement |
|-|-|
| Discovery | Select Pods labeled `ai-conformance.kubernetes.io/hpa-metric-source: "true"` in any namespace. Use `-hpa-pod-annotations` to add annotations your discovery needs (e.g. `prometheus.io/scrape=true`). |
| Scrape | Scrape the container port named `metrics` (8080) at `/metrics`, preferably every 15s. The response is Prometheus text format (`text/plain; version=0.0.4`) containing the gauge `ai_conformance_hpa_queue_depth`. |
| Relabeling | Attach the Pod's namespace and name to each sample (as `namespace` and `pod` labels in the examples below) so the adapter can map it to the Pod. |
| Adapter mapping | Serve the series as a metric of resource `pods` in `custom.metrics.k8s.io`, one value per Pod, filtered by the label selector in the request. Pass the served name as `-hpa-custom-metric-name`. Either `v1beta1` or `v1beta2` works: the test reads the version the HPA controller reads, which is the group's preferred version if it is one of those two, or else the first of them the group lists. (The prometheus-adapter Helm chart registers `v1beta1` only.) |
| HPA access | The HPA controller must be allowed to read `custom.metrics.k8s.io`. Kubernetes' default `system:controller:horizontal-pod-autoscaler` ClusterRole grants this; keep it if your platform customizes controller RBAC. |

The examples below show one way to meet the contract with the Prometheus Operator and prometheus-adapter. They are platform configuration applied by the cluster operator, not resources the test creates.

With the Prometheus Operator (e.g. kube-prometheus-stack, whose Prometheus selects PodMonitors labeled `release: <release name>` by default), a PodMonitor provides discovery and scraping, and the operator adds `namespace` and `pod` labels:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: ai-conformance-hpa
  namespace: monitoring
  labels:
    release: kube-prometheus-stack
spec:
  namespaceSelector:
    any: true
  selector:
    matchLabels:
      ai-conformance.kubernetes.io/hpa-metric-source: "true"
  podMetricsEndpoints:
  - port: metrics
    path: /metrics
    interval: 15s
```

Without the operator, an equivalent Prometheus scrape job is:

```yaml
- job_name: ai-conformance-hpa
  scrape_interval: 15s
  kubernetes_sd_configs:
  - role: pod
  relabel_configs:
  - source_labels: [__meta_kubernetes_pod_label_ai_conformance_kubernetes_io_hpa_metric_source]
    regex: "true"
    action: keep
  - source_labels: [__meta_kubernetes_pod_container_port_name]
    regex: metrics
    action: keep
  - source_labels: [__meta_kubernetes_namespace]
    target_label: namespace
  - source_labels: [__meta_kubernetes_pod_name]
    target_label: pod
```

The adapter mapping as prometheus-adapter Helm values. Point `prometheus.url` at your Prometheus (the value below is kube-prometheus-stack's Service); `max` keeps one value per Pod if a Pod is scraped more than once:

```yaml
prometheus:
  url: http://prometheus-operated.monitoring.svc
  port: 9090
rules:
  custom:
  - seriesQuery: 'ai_conformance_hpa_queue_depth{namespace!="",pod!=""}'
    resources:
      overrides:
        namespace: {resource: "namespace"}
        pod: {resource: "pod"}
    name:
      matches: "^ai_conformance_hpa_queue_depth$"
      as: "ai_conformance_hpa_queue_depth"
    metricsQuery: 'max(<<.Series>>{<<.LabelMatchers>>}) by (<<.GroupBy>>)'
```

To check the pipeline, run any Pod labeled `ai-conformance.kubernetes.io/hpa-metric-source=true` that serves the gauge, then query the API the HPA uses, with the version your adapter registers:

```bash
kubectl get apiservices | grep custom.metrics.k8s.io   # e.g. v1beta1.custom.metrics.k8s.io
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/<namespace>/pods/*/ai_conformance_hpa_queue_depth"
```

#### Permissions

The test runs with the cluster-admin kubeconfig listed in the prerequisites. It uses: namespaces (create, delete); ConfigMaps, Deployments, HorizontalPodAutoscalers (`autoscaling/v2`), and in DRA mode ResourceClaimTemplates, in its own namespace (create, update, delete); Pods and ResourceClaims (get, list); Nodes, DeviceClasses and ResourceSlices (get, list) for accelerator detection; and `custom.metrics.k8s.io` (get) plus API discovery. It creates no cluster-scoped resources.

#### Running the test

```bash
go test -v ./test -run TestHPAAutoscaling -timeout 75m -args \
  -hpa-custom-metric-name=ai_conformance_hpa_queue_depth
```

Add `-hpa-pod-annotations=prometheus.io/scrape=true` (or whatever your discovery needs) if your pipeline discovers Pods by annotation, `-allocation-mode=dra` or `-allocation-mode=device-plugin` to require a specific mechanism, and `-hpa-workload-image` to pull the workload image from a mirror.

Each wait is bounded by a flag: `-hpa-scale-up-timeout` (default 10m) for the baseline Pod becoming Ready and, separately, for the scale-up, which `-hpa-metric-timeout` extends so the new Pods can become Ready and report the raised metric; `-hpa-metric-timeout` (default 5m) also for each time a metric change must reach the custom metrics API, and for the HPA's first reconcile; and `-hpa-scale-down-timeout` (default 10m, which must exceed `-hpa-scale-down-stabilization`, default 1m) after the lowered metric is reported. The negative control lasts one minute, or up to two if the API server does not answer at its end. With the defaults the test therefore runs for at most about 52 minutes, then cleanup takes at most 7 (5 for the workload, 2 for the namespace). Use `-timeout 75m`, and raise it by as much as you raise these flags. The test stops 10 minutes before the `go test` deadline so that its cleanup still runs (a `go test` timeout would otherwise end the process without cleanup, leaving the Pods and their accelerators allocated), and it fails at once if `-timeout` leaves less than that, as Go's 10-minute default does.

### AI Job & Inference Service Metrics

The metrics test deploys a small workload that exposes Prometheus metrics, sends it a fixed number of requests, and verifies that the platform's monitoring system collects the workload's metrics and that the collected request count increases by exactly the number of requests sent.

The test is **skipped by default**. Point it at the monitoring system's Prometheus-compatible query API with exactly one of:

- `-service-metrics-prometheus-service=<namespace>/<name>:<port>`: an in-cluster Service, reached through the API server service proxy (no port-forward needed).
- `-service-metrics-query-url=<url>`: any reachable query endpoint, including any path prefix (e.g. `https://mimir.example/prometheus`), optionally with `-service-metrics-query-bearer-token-file`, `-service-metrics-query-headers` (e.g. a tenant header like `X-Scope-OrgID=<tenant>`) and `-service-metrics-query-ca-file` for a private CA.

By default the test creates a `monitoring.coreos.com/v1` `ServiceMonitor`. Use `-service-metrics-scrape-labels` if your Prometheus only selects labeled ServiceMonitors (for kube-prometheus-stack: `release=<helm release name>`). If your platform discovers scrape targets another way, pass a templated manifest with `-service-metrics-scrape-manifest`; run `go test ./test -args -help` for the available template fields. In a custom manifest, set the scrape interval to `{{.ScrapeInterval}}`. With a longer interval or ingestion delay, raise `-service-metrics-collection-timeout`, which bounds each of the two waits (before and after traffic).

Example with kube-prometheus-stack:

```bash
go test -v ./test -run TestAIServiceMetrics -args \
  -service-metrics-prometheus-service=monitoring/prometheus-operated:9090 \
  -service-metrics-scrape-labels=release=kube-prometheus-stack
```

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


