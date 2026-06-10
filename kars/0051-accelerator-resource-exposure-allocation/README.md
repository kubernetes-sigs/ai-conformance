# KAR-0051: Accelerator Resource Exposure & Allocation

## Description

Support Dynamic Resource Allocation (DRA) APIs to enable more flexible and fine-grained resource requests beyond simple counts.

## Motivation

Traditional Kubernetes device plugins expose accelerators as simple integer counts (e.g. `nvidia.com/gpu: 1`), which limits the ability to express fine-grained requirements such as specific GPU models, memory sizes, or interconnect topologies. Dynamic Resource Allocation (DRA) provides a structured API for requesting and allocating accelerator resources with richer semantics, enabling workloads to specify exactly what they need and platforms to make smarter allocation decisions.

Supporting DRA is essential for AI/ML workloads that often have specific hardware requirements beyond just "a GPU" — for example, requesting a GPU with a minimum amount of VRAM, or a specific accelerator model compatible with a particular framework version.

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

Verify that all the resource.k8s.io/v1 DRA API resources are enabled.

### Automated Tests

This requirement is covered by the upstream Kubernetes conformance test suite starting in v1.35, which verifies that kube-apiserver supports create/update/list/patch/delete operations for all `resource.k8s.io/v1` DRA API types:

- `[sig-node] [DRA] CRUD Tests resource.k8s.io/v1 DeviceClass [Conformance]`
- `[sig-node] [DRA] CRUD Tests resource.k8s.io/v1 ResourceClaim [Conformance]`
- `[sig-node] [DRA] CRUD Tests resource.k8s.io/v1 ResourceClaimTemplate [Conformance]`
- `[sig-node] [DRA] CRUD Tests resource.k8s.io/v1 ResourceSlice [Conformance]`

See [test/conformance/testdata/conformance.yaml](https://github.com/kubernetes/kubernetes/blob/release-1.35/test/conformance/testdata/conformance.yaml#L2935-L2959) and the implementation in [test/e2e/dra/dra.go](https://github.com/kubernetes/kubernetes/blob/release-1.35/test/e2e/dra/dra.go).

## Implementation History

2026-03-12: KAR created

## Related KARs

<!--
List KARS that are related. This is in case of additional requirements that come up after a KAR has already graduated to "implemented"
-->
