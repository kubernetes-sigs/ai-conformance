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

1. **Setup**: Deploy a custom metrics adapter exposing a configurable test metric. Create a Deployment whose pods each request an accelerator and emit the custom metric, and an HPA targeting the Deployment with a defined target value for that metric.

2. **Scale-up case**: Drive the custom metric above the target. Verify that the HPA increases the replica count and that new pods reach `Running` on accelerator-equipped nodes.

3. **Scale-down case**: Lower the custom metric below the target. Verify that the HPA decreases the replica count after the stabilization window.

4. **Cleanup**: Delete the HPA, Deployment, and custom metrics adapter.

## Implementation History

2026-03-12: KAR created

## Related KARs

<!--
List KARS that are related. This is in case of additional requirements that come up after a KAR has already graduated to "implemented"
-->
