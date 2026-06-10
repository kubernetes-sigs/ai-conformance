# KAR-0063: Robust CRD and Controller Operation

## Description

The platform must prove that at least one complex AI operator with a CRD (e.g., Ray, Kubeflow) can be installed and functions reliably. This includes verifying that the operator's pods run correctly, its webhooks are operational, and its custom resources can be reconciled.

## Motivation

AI/ML workloads on Kubernetes are typically managed by operators that extend the platform with custom resources, such as RayCluster, TrainJob, or InferenceService. These operators rely on CRDs, admission webhooks, and controller reconciliation loops to function correctly. If any of these components fail silently or are incompatible with the platform, AI workloads cannot be deployed or managed reliably.

This requirement ensures the platform can support the full lifecycle of at least one representative AI operator, providing confidence that the Kubernetes API extensions ecosystem works correctly for AI use cases.

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

Deploy a representative AI operator, verify all Pods of the operator and its webhook are Running and its CRDs are registered with the API server. Verify that invalid attempts (e.g. an invalid spec) are rejected by its admission webhook. Verify that a valid instance of the custom resource can be reconciled.

### Automated Tests

The test verifies that a representative AI operator with a CRD can be installed, that its admission webhook rejects invalid input, and that its controller reconciles valid custom resources.

1. **Setup**: Install a representative AI operator (e.g., one that manages a workload such as RayCluster, TrainJob, or InferenceService). Wait for the operator's Pods to be Running and its CRDs to be registered with the API server.

2. **Webhook validation case**: Submit a custom resource with an intentionally invalid spec and verify the admission webhook rejects it with an error.

3. **Reconciliation case**: Submit a valid instance of the custom resource and verify the operator reconciles it (e.g., creates the expected child resources and the custom resource reaches its expected Ready/Succeeded status within a reasonable timeout).

4. **Cleanup**: Delete the custom resources and uninstall the operator.

## Implementation History

2026-03-12: KAR created

## Related KARs

<!--
List KARS that are related. This is in case of additional requirements that come up after a KAR has already graduated to "implemented"
-->
