# KAR-0003: GPU Sharing

## Description

For accelerators that support static GPU sharing, provide well-defined mechanisms for at least one GPU sharing strategy to improve utilization for workloads that do not require a full dedicated GPU. If hardware-level partitioning is supported, then these fractional GPU resources should be exposed as schedulable resources. If software-based sharing (e.g. time-slicing) is supported, then oversubscription of GPUs should be allowed. 

Forward-looking: Once the accelerator supports static GPU sharing as part of DRA, the platform should expose the DRA mechanism to allow users to leverage static GPU sharing. Once the accelerator supports dynamic GPU sharing as part of DRA and the partitionable devices feature is GA, the platform should expose the DRA mechanism to allow users to leverage dynamic GPU sharing.

## Motivation

AI/ML workloads on Kubernetes need GPU sharing because it dramatically improves GPU utilization. Most inference and fine-tuning jobs don’t need an entire modern GPU. Partitioning lets multiple workloads run concurrently without wasting compute. It also reduces cost by allowing teams to pack more jobs onto the same hardware, and enables resource isolation so different users or namespaces can safely share the same GPU without interfering with each other. GPU sharing also provides scheduling flexibility, letting Kubernetes treat GPUs more like fine-grained, composable resources rather than monolithic devices.

## Graduation Criteria

**SHOULD**
- [x] Describe how users can test it for self-attestation with scripts, documentation, etc
- [x] Starting v1.37, new SHOULDs must include proposed automated tests in the automated tests section below

**MUST**
- [ ] Starting v1.37, new MUSTs must include automated tests that have been added to the AI conformance test suite
- [ ] Demonstrate at least two real-world usage of SHOULD before graduating to MUST
- [ ] Kubernetes core APIs must be GA

## Test Plan

### How We Might Test It

We should be able to use the verifiable mechanism provided by the platform to ensure that a single physical GPU can be exposed as multiple allocatable GPU resources and scheduled by Kubernetes. Create functional tests that verify multiple workloads requesting multiple logical GPUs can be scheduled and are running on the same physical GPU.

### Automated Tests

An automated test could involve the following steps:
1. Get node information to verify node's `Allocatable` field includes statically configured shared GPUs
1. Deploying one pod requesting one or more shared logical GPUs e.g. 
```yaml
resources:
  limits:
    "nvidia.com/gpu": 1
```
or 
```yaml
resources:
  limits:
    "nvidia.com/mig-1g.5gb": 1
```
1. The pods should succesfully be running on a node with the statically configured logical GPUs and the same physical GPU.
1. The pods would execute a script that inspects the accelerator health and no errors returned.

The automated tests will default to checking common accelerators (~80% of platforms). If the test encounters an accelerator variant it does not recognize, it will output an "Unknown" status rather than failing, signaling that manual verification is required. We expect the test suite to grow over time to support automated verification for all platforms.

## Implementation History

2025-11-19: KAR created
2026-10-21: Add automated conformance test TestGPUSharing

## Related KARs

<!--
List KARS that are related. This is in case of additional requirements that come up after a KAR has already graduated to “implemented”
-->