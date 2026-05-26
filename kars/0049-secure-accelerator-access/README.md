# KAR-0049: Secure Accelerator Access

## Description

Ensure that access to accelerators from within containers is properly isolated and mediated by the Kubernetes resource management framework (device plugin or DRA) and container runtime, preventing unauthorized access or interference between workloads.

## Motivation

AI/ML workloads on Kubernetes depend on accelerators like GPUs and TPUs for training and inference. In multi-tenant clusters, only containers that request accelerator resources should be allowed to use accelerator devices. In addition, they should not be able to discover or interfere with devices assigned to other workloads.

Without proper isolation, a container could discover GPU devices it was never allocated and read memory from another tenant's inference workload, exposing sensitive model weights or input data. This requirement ensures that Kubernetes device allocation and container runtime layers work together to prevent these kinds of security and stability risks.

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

1. Deploy a Pod to a node with available accelerators, without requesting accelerator resources in the Pod spec. Execute a command in the Pod to probe for accelerator devices, and the command should fail or report that no accelerator devices are found.

2. Create two Pods, each allocated an accelerator resource. Execute a command in one Pod to attempt to access the other Pod's accelerator, and the access should be denied.

### Automated Tests

Automated tests for this requirement are being developed in [#45](https://github.com/kubernetes-sigs/ai-conformance/pull/45), tracked by the test plan in [#27](https://github.com/kubernetes-sigs/ai-conformance/issues/27).

## Implementation History

2026-03-12: KAR created

## Related KARs

<!--
List KARS that are related. This is in case of additional requirements that come up after a KAR has already graduated to "implemented"
-->
