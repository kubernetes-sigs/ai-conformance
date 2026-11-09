# KAR-0011: Advanced Inference Ingress

## Description

Support an implementation of the [Gateway API Inference Extension (GAIE)](https://github.com/kubernetes-sigs/gateway-api-inference-extension), which can route requests to models hosted on Kubernetes. The implementation supports serving LLMs and making advanced routing decisions (e.g., K/V cache-aware routing) based on metrics and capabilities advertised by the underlying model serving platform.

## Motivation

As AI/ML inference workloads become more complex, standard LoadBalancer or Ingress resources are insufficient. Model serving platforms need a unified interface to express routing requirements with request-level intelligence without relying on proprietary, vendor-specific networking implementations.

Adopting the GAIE prevents ecosystem fragmentation by providing a single standard for frameworks (like KServe, vLLM) to target.

## Graduation Criteria

**SHOULD**
- [x] Describe how users can test it for self-attestation with scripts, documentation, etc
- [ ] Starting v1.37, new SHOULDs must include proposed automated tests in the automated tests section below

**MUST**
- [ ] Starting v1.37, new MUSTs must include automated tests that have been added to the AI conformance test suite
- [ ] Demonstrate at least two real-world usage of SHOULD before graduating to MUST
- [ ] Kubernetes core APIs must be GA

## Test Plan

<!--
**Note:** *Not required until targeted at a release.*
The goal is to ensure that we don't accept requirements with inadequate ways to test them.
Starting v1.37, new SHOULDs must include proposed automated tests and new MUSTs must include automated tests that have been added to the AI conformance test suite.
For SHOULDs, users can run automated test or self-attestation following manual steps described in How We Might Test It section below.
-->

### How We Might Test It

GAIE's [official conformance suite](https://github.com/kubernetes-sigs/gateway-api-inference-extension/tree/main/conformance) can be used to verify the implementation.

### Automated Tests

The test will be written in go, and the logic looks like the following:

```bash
# Run GAIE official conformance suite
git clone https://github.com/kubernetes-sigs/gateway-api-inference-extension.git  
cd gateway-api-inference-extension/conformance
kubectl get gatewayclass # get the gatewayclass name
go test . -args -gateway-class <gatewayclass_name>
```

## Implementation History

2026-02-19: KAR created

## Related KARs

- [KAR-0111: Gateway API for AI Inference](/kars/0111-gateway-api-inference/README.md)
  (`networking.ai_inference`, `MUST`: core Kubernetes Gateway API traffic
  management for inference services).
