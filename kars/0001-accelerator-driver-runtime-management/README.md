# KAR-0001: Accelerator Driver & Runtime Management

## Description

Provide a verifiable mechanism for ensuring that compatible accelerator drivers and corresponding container runtime configurations are correctly installed and maintained on nodes with accelerators. Once the accelerator supports exposing driver and runtime version information as part of DRA, then the platform should use the DRA mechanism for verification.

**Conformance Strategy:** verification will initially prioritize common accelerator types that cover the majority of platforms. For platforms using specialized or less common variants, conformance may initially require manual verification until the automated test suite is expanded by community contributions.

## Motivation

AI/ML workloads often have strict dependencies on specific versions of accelerator drivers and container runtimes. Incompatibility between these components is a common source of errors, leading to wasted resources and developer frustration. 

This requirement ensures that a Kubernetes platform provides a reliable way to verify that the correct accelerator drivers and container runtimes are installed and configured on nodes with accelerators.

By providing a verifiable mechanism to ensure compatibility, platforms can significantly improve the reliability of AI/ML workloads, simplify troubleshooting, and guarantee portability across different environments. This helps accelerate the adoption of AI/ML on Kubernetes by providing a more stable and predictable platform.

## Graduation Criteria

**SHOULD**
- [x] Describe how users can test it for self-attestation with scripts, documentation, etc
- [x] Starting v1.37, new SHOULDs must include proposed automated tests in the automated tests section below

**MUST**
- [x] Starting v1.37, new MUSTs must include automated tests that have been added to the AI conformance test suite
- [ ] Demonstrate at least two real-world usage of SHOULD before graduating to MUST
- [ ] Kubernetes core APIs must be GA

## Test Plan

### How We Might Test It

We should be able to use the verifiable mechanism provided by the platform to ensure that the compatible accelerator driver and container runtime are installed and configured correctly on nodes with accelerators. This might be achieved by using labels and annotations on the nodes, or through DRA attributes.

### Automated Tests

The automated conformance test (`TestAcceleratorDriverRuntimeManagement` in `test/driver_runtime_management_test.go`) verifies the mechanism above:
1. Resolves the accelerator driver version the platform advertises for an accelerator node, preferring DRA `ResourceSlice` device attributes and falling back to Node labels or annotations.
1. Runs an accelerator-requesting Pod built from a vanilla OS image and checks, from inside the container, that the container runtime configuration injected the vendor's driver tools and libraries, that the driver initializes, and that the driver version it reports matches the version the platform advertises.
1. If `-accelerator-type` names a variant the test does not recognize, it outputs an "Unknown" status and skips rather than failing, signaling that manual verification is required.

## Implementation History

2025-11-19: KAR created

2026-09-22: `TestAcceleratorDriverRuntimeManagement` added to the AI conformance test suite

## Related KARs

<!--
List KARS that are related. This is in case of additional requirements that come up after a KAR has already graduated to “implemented”
-->
