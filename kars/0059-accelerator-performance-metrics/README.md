# KAR-0059: Accelerator Performance Metrics

## Description

For supported accelerator types, the platform must allow for the installation and successful operation of at least one accelerator metrics solution that exposes fine-grained performance metrics via a standardized, machine-readable metrics endpoint. This must include a core set of metrics for per-accelerator utilization and memory usage. Additionally, other relevant metrics such as temperature, power draw, and interconnect bandwidth should be exposed if the underlying hardware or virtualization layer makes them available. The list of metrics should align with emerging standards, such as OpenTelemetry metrics, to ensure interoperability. The platform may provide a managed solution, but this is not required for conformance.

## Motivation

Observability into accelerator health and performance is fundamental for operating AI/ML workloads reliably. Without fine-grained metrics, operators cannot detect underutilized GPUs, thermal throttling, memory pressure, or failing hardware. These scenarios may directly impact training throughput and inference latency.

Standardizing the metrics format and ensuring at least one solution is operational on the platform allows consistent tooling across different vendors and accelerator types, reducing the operational burden of running AI workloads at scale.

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

Given a node with a supported accelerator type, identify the Prometheus-compatible metrics endpoint for the accelerators on the node and scrape metrics from the endpoint. Parse the scraped metrics to find metrics for each supported accelerator on the node, including: accelerator utilization, memory usage, temperature, power usage, etc. The test can evolve to check for specific metric names once those are standardized.

### Automated Tests

The test verifies that an accelerator metrics solution running on the platform exposes per-accelerator performance metrics via a standardized, machine-readable endpoint.

1. **Setup**: Ensure at least one supported accelerator type is available on a node. Identify the metrics endpoint exposed by the platform's accelerator metrics solution.

2. **Metrics presence case**: Scrape the metrics endpoint and verify that the response includes per-accelerator metrics for utilization and memory usage. Where the underlying hardware supports it, additional metrics for temperature, power draw, and interconnect bandwidth should also be present.

3. **Workload-driven case**: Schedule a workload that exercises the accelerator (e.g., a short compute task). Scrape the endpoint before and during the workload, and verify that the utilization metric reflects the change.

4. **Cleanup**: Delete the test workload.

## Implementation History

2026-03-12: KAR created

## Related KARs

<!--
List KARS that are related. This is in case of additional requirements that come up after a KAR has already graduated to "implemented"
-->
