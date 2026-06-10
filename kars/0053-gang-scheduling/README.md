# KAR-0053: Gang Scheduling

## Description

The platform must allow for the installation and successful operation of at least one gang scheduling solution that ensures all-or-nothing scheduling for distributed AI workloads (e.g. Kueue, Volcano, etc.) To be conformant, the vendor must demonstrate that their platform can successfully run at least one such solution.

## Motivation

Distributed AI workloads such as multi-node training jobs require all of their component pods to be scheduled simultaneously. If only some pods in a group are scheduled while others remain pending, the running pods waste expensive accelerator resources while waiting for the rest. Gang scheduling ensures that either all pods in a job are placed at once, or none are, preventing resource deadlocks and wasted capacity.

This is especially critical for large-scale training jobs that may require dozens or hundreds of coordinated pods across multiple nodes with accelerators.

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

Install a gang scheduling solution (e.g. Kueue or Volcano) on the cluster. Submit a distributed AI workload that requires multiple pods to be co-scheduled. Verify that all pods are scheduled simultaneously and the workload completes successfully. Then submit a workload that requests more resources than available and verify that no partial scheduling occurs — all pods should remain pending until sufficient resources are available.

### Automated Tests

The test verifies that a gang scheduling solution running on the platform enforces all-or-nothing scheduling for a multi-pod workload.

1. **Positive case**: Submit a workload requiring N co-scheduled pods to a cluster with sufficient resources. Verify that all N pods reach `Running` and the workload completes successfully.

2. **Negative case**: Submit a workload requiring N co-scheduled pods to a cluster with capacity for fewer than N. Verify that no pods reach `Running` within a configurable wait period, confirming no partial scheduling occurs.

3. **Cleanup**: Delete the test workload and verify resources are released.

## Implementation History

2026-03-12: KAR created

## Related KARs

<!--
List KARS that are related. This is in case of additional requirements that come up after a KAR has already graduated to "implemented"
-->
