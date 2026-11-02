# Kubernetes AI Conformance

Proposals and discussions for [Kubernetes AI Conformance](https://github.com/kubernetes/community/tree/master/sig-architecture#ai-conformance) subproject owned by [SIG Architecture](https://github.com/kubernetes/community/tree/master/sig-architecture). You can find the conformance program in [cncf/k8s-ai-conformance](https://github.com/cncf/k8s-ai-conformance).

## Defining AI Conformance Requirements

Discussions for AI conformance requirements are now tracked using the
[AI Conformance Requirements](https://github.com/orgs/kubernetes-sigs/projects/114)
GitHub Project, shifting from the original
[Google Doc](https://docs.google.com/document/d/1hXoSdh9FEs13Yde8DivCYjjXyxa7j4J8erjZPEGWuzc/edit?tab=t.0).

Each requirement is tracked as a GitHub issue with the following workflow and status label:
- **Implementable**: Has been accepted by the AI Conformance program for a Kubernetes release.
- **Implemented**: Has been part of one or more Kubernetes releases in the [cncf/k8s-ai-conformance](https://github.com/cncf/k8s-ai-conformance) repository and has graduated to MUST. Implementation is complete. Further changes should be made via new KARs.

To participate, please comment on the relevant GitHub issues and pull requests.
Unresolved items will be discussed during the
[recurring AI Conformance meetings](https://github.com/kubernetes/community/tree/master/sig-architecture#meetings).

### Process Details

This process adopts the Kubernetes Enhancement Proposal (KEP) process as the basis for managing the lifecycle of Kubernetes AI conformance Requirements (KARs), including review, discussion, and approval.

- **Requirement Proposal**: Propose each new Kubernetes AI conformance Requirement (KAR) as a GitHub issue. Create a PR for the KAR targeting a specific Kubernetes release. Progress for each requirement will be tracked using the GitHub issue and the KAR. Each requirement will be reviewed individually.
- **Graduation Criteria**: Requirements will graduate from "SHOULD" to "MUST" stage in alignment with the process used for KEPs in each Kubernetes release cycle.
- **Timeline Alignment**: The lifecycle for requirements will follow the Kubernetes release schedule. For example, [this timeline](https://github.com/kubernetes/sig-release/blob/master/releases/release-1.35/README.md#timeline) will be adopted starting Kubernetes v1.36.
  - **KEP/KAR Freeze**: Locks in the set of KARs to be considered for updates for a given Kubernetes release. No new requirements after KEP freeze.
  - **Discussion and Refinement before Code Freeze**: After KEP/KAR freeze, all discussions, text refinement, any changes (including associated tests) for accepted requirements will happen as part of the PR review for KAR updates. Before the code freeze deadline for the given Kubernetes release, a PR with all the AI conformance requirements for that Kubernetes release in the form of conformance-versions/KubernetesAIConformance-1.NN.yaml and all changes for all KARs for that Kubernetes release must be reviewed, approved, and merged. 
  - **Post Code Freeze**: A PR with a copy of conformance-versions/KubernetesAIConformance-1.NN.yaml must be reviewed and merged in [cncf/k8s-ai-conformance](https://github.com/cncf/k8s-ai-conformance) to ensure transparency and clarity for the entire community. In the event a kubernetes feature does not reach GA and impacts the graduation of a KAR, we will need to reassess that KAR to rollback and update conformance-versions/KubernetesAIConformance-1.NN.yaml accordingly.
- **Reviewers**: everyone in ai-conformance
- **Approvers**: ai-conformance-requirement-approvers group. For automated tests, SIG Testing and SIG Arch leads will be tagged for approval.
- **Stage**: All requirements need to start with SHOULD and eventually graduate to MUST
- **Requirement removal**: Criteria to remove a requirement is TBD


## AI Conformance Tests

Each KAR documents how to verify its requirement in its Test Plan section.
Starting v1.37, new SHOULDs must include proposed automated tests, and new MUSTs must include automated tests that have been added to the AI conformance test suite in [`test/`](test/).

All MUST requirements now have automated tests. See [`test/README.md`](test/README.md) for how to run them and which test covers each requirement.

## Community, discussion, contribution, and support

Learn how to engage with the Kubernetes community on the [community page](http://kubernetes.io/community/).

You can reach the maintainers of this project at:

- [Slack channel](https://kubernetes.slack.com/messages/k8s-ai-conformance)
- [Mailing List](https://groups.google.com/a/kubernetes.io/g/sig-architecture)

### Code of conduct

Participation in the Kubernetes community is governed by the [Kubernetes Code of Conduct](code-of-conduct.md).
