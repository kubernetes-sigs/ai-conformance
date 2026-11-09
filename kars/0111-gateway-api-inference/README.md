# KAR-0111: Gateway API for AI Inference

## Description

Support the Kubernetes Gateway API with an implementation for advanced traffic
management for inference services, which enables capabilities like weighted
traffic splitting, header-based routing (for OpenAI protocol headers), and
optional integration with service meshes.

## Motivation

Production AI/ML inference services require traffic management capabilities
beyond basic Layer-4 load balancing or legacy `Ingress` resources, such as
gradual canary rollouts across model versions via weighted traffic splitting and
request routing based on HTTP headers (for example, OpenAI protocol or
organization headers).

Standardizing on the Kubernetes `Gateway` API (`gateway.networking.k8s.io/v1`)
with an active Gateway controller gives inference workloads a portable,
vendor-neutral traffic management API across conformant platforms, without
relying on controller-specific `Ingress` annotations. Model-aware LLM routing
extensions (such as KV-cache-aware routing via Gateway API Inference Extension)
are tracked separately in
[KAR-0011](/kars/0011-advanced-ingress-inference/README.md).

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

Verify that the `gateway.networking.k8s.io/v1` API resources (`GatewayClass`,
`Gateway`, `HTTPRoute`) are served on the cluster and backed by an active
Gateway controller implementation, for example by running the upstream
[Gateway API conformance suite](https://gateway-api.sigs.k8s.io/concepts/conformance/)
(`HTTPRouteWeight` and `HTTPRouteHeaderMatching`), or by deploying a `Gateway`
and `HTTPRoute` fronting two inference backend Services and verifying header
matching and weighted traffic splitting. Integration with a service mesh is
optional and not required for conformance.

### Automated Tests

The automated conformance test verifies the observable outcomes in the
Description:

1. Verifies that `gateway.networking.k8s.io/v1` (`GatewayClass`, `Gateway`,
   `HTTPRoute`) is enabled and detects an accepted `GatewayClass` (or uses a
   configured class, skipping when none is available).
2. Deploys two HTTP inference backend Services behind a `Gateway` and
   `HTTPRoute` configured with header-based routing (e.g., OpenAI protocol
   headers) and weighted `backendRefs`, waiting for the `Gateway` and
   `HTTPRoute` to report `Accepted` / `Programmed`.
3. Sends HTTP traffic through the `Gateway` and verifies that requests carrying
   the target header route to the matching backend while general traffic splits
   across backends according to the configured weights.

## Implementation History

- 2025-10-22: Included as a `MUST` requirement (`networking.ai_inference`) in
  initial v1.33 and v1.34 AI Conformance manifests.
- 2026-10-28: Backfilled KAR created.

## Related KARs

- [KAR-0011: Advanced Inference Ingress](/kars/0011-advanced-ingress-inference/README.md)
  (`networking.advanced_inference_ingress`, `SHOULD`: Gateway API Inference
  Extension).
