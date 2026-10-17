# KAR-0057: Effective HPA Autoscaling for AI Workloads

## Description

If the platform supports the HorizontalPodAutoscaler, it must function correctly for pods utilizing accelerators. This includes the ability to scale these Pods based on custom metrics relevant to AI/ML workloads.

## Motivation

AI workloads often need to scale horizontally based on demand. For example, GPU-backed inference services may need to scale up when request latency increases or queue depth grows. The HorizontalPodAutoscaler (HPA) is the standard Kubernetes mechanism for this, but it must work correctly with pods that consume accelerator resources and scale based on custom metrics (e.g. accelerator utilization, request throughput) rather than just CPU and memory.

Without HPA support for accelerator-backed pods and custom metrics, operators must manually scale these workloads or build custom autoscaling solutions, which adds complexity and reduces reliability.

## Graduation Criteria

**SHOULD**
- [x] Describe how users can test it for self-attestation with scripts, documentation, etc
- [ ] Starting v1.37, new SHOULDs must include proposed automated tests in the automated tests section below

**MUST**
- [ ] Starting v1.37, new MUSTs must include automated tests that have been added to the AI conformance test suite
- [x] Demonstrate at least two real-world usage of SHOULD before graduating to MUST
- [x] Kubernetes core APIs must be GA

## Test Plan

### How We Might Test It

A custom metrics pipeline is configured to expose accelerator-related custom metrics to the HPA. Create a Deployment with each Pod requesting an accelerator and exposing a custom metric. Create a HorizontalPodAutoscaler targeting the Deployment. Introduce load to the sample application, causing the average custom metric value to significantly exceed the target, triggering a scale up. Then remove the load to trigger a scale down.

### Automated Tests

The test verifies that the HorizontalPodAutoscaler running on the platform can scale accelerator-backed pods based on a custom metric.

1. **Setup**: Use the platform's custom metrics pipeline, configured to expose a controllable test metric. Create a Deployment whose pods each request an accelerator and emit the custom metric, and an HPA targeting the Deployment with a defined target value for that metric.

2. **Scale-up case**: Drive the custom metric above the target. Verify that the HPA increases the replica count and that new pods reach `Running` on accelerator-equipped nodes.

3. **Scale-down case**: Lower the custom metric below the target. Verify that the HPA decreases the replica count after the stabilization window.

4. **Cleanup**: Delete the HPA, the Deployment and any other resources the test created.

The proposed `TestHPAAutoscaling` (see [test/hpa_autoscaling_test.go](../../test/hpa_autoscaling_test.go)) implements these steps as follows. The test does not deploy a custom metrics adapter, because doing so would replace the platform's registration of the custom metrics API; it relies on the platform's pipeline meeting a documented integration contract (see [test/README.md](../../test/README.md)). Each Pod requests one accelerator and exposes a synthetic per-replica queue depth gauge, standing in for an inference server's scaling signal, whose value the test controls through a ConfigMap; the test checks the HPA's behavior, not inference performance. The test verifies that the HPA holds one replica while the metric is below target, scales up to `maxReplicas` with every new Pod Running and Ready on an allocated accelerator when the metric is raised, and scales back down once the metric is lowered, with the removed Pods deleted and, under DRA, their ResourceClaims deallocated.

## Implementation History

2026-03-12: KAR created
2026-10-16: Automated conformance test proposed in test/hpa_autoscaling_test.go (pending review)

## Related KARs

<!--
List KARS that are related. This is in case of additional requirements that come up after a KAR has already graduated to "implemented"
-->
