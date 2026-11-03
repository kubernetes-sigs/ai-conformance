# Conformance Versions

Kubernetes AI Conformance follows Kubernetes Releases. Therefore the corresponding Version is always towards the Kubernetes Version e.g `KubernetesAIConformance-1.33.yaml` correlates to `Kubernetes v1.33`

In this folder are the Conformance requirements for each Kubernetes Version supported by the Kubernetes AI Conformance Program. 

The files in this folder are the final version and will be transferred to the [cncf/k8s-ai-conformance](https://github.com/cncf/k8s-ai-conformance) repository. 

## Hybrid Verification Submission (v1.37+)

Starting with v1.37, we support a hybrid verification approach that combines automated test results with manual attestation. It is **recommended that all requirements with an automated test are verified by running the test suite**.

1.  **Automated Tests**: For requirements with automated tests (all MUST requirements; see the "Test Cases Covered" table in [`test/README.md`](../test/README.md)), it is recommended to run the test suite and include the output files (`e2e.log`, `junit.xml`) in your submission PR.
2.  **Not Applicable (N/A) Requirements**: If a requirement is not applicable to your platform, the corresponding automated test should be skipped (either automatically by the test suite or by opting out). You should provide a brief explanation in the `notes` or `evidence` field of the checklist YAML detailing why the requirement is considered N/A.
3.  **Manual Attestation**: For requirements without automated tests, continue to provide documentation or reference URLs in the `evidence` field of the checklist YAML.

Reference your automated test results in the checklist YAML using the `file://` scheme or relative paths to your submitted test artifacts.