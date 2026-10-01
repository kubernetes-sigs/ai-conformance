package conformance

import (
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestVerifyCRDEstablished(t *testing.T) {
	tests := []struct {
		name    string
		crd     *unstructured.Unstructured
		wantErr bool
	}{
		{
			name: "established CRD",
			crd: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"type":   "Established",
								"status": "True",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "not established CRD",
			crd: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"type":   "Established",
								"status": "False",
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no conditions",
			crd: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"status": map[string]interface{}{},
				},
			},
			wantErr: true,
		},
		{
			name: "no status",
			crd: &unstructured.Unstructured{
				Object: map[string]interface{}{},
			},
			wantErr: true,
		},
		{
			name: "multiple conditions with Established",
			crd: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"status": map[string]interface{}{
						"conditions": []interface{}{
							map[string]interface{}{
								"type":   "NamesAccepted",
								"status": "True",
							},
							map[string]interface{}{
								"type":   "Established",
								"status": "True",
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifyCRDEstablished(tt.crd)
			if (err != nil) != tt.wantErr {
				t.Errorf("verifyCRDEstablished() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBuildInvalidTrainJob(t *testing.T) {
	job := buildInvalidTrainJob("test-ns", "bad-job")

	if job.GetKind() != "TrainJob" {
		t.Errorf("expected kind TrainJob, got %s", job.GetKind())
	}
	if job.GetAPIVersion() != "trainer.kubeflow.org/v1alpha1" {
		t.Errorf("expected apiVersion trainer.kubeflow.org/v1alpha1, got %s", job.GetAPIVersion())
	}
	if job.GetName() != "bad-job" {
		t.Errorf("expected name bad-job, got %s", job.GetName())
	}
	if job.GetNamespace() != "test-ns" {
		t.Errorf("expected namespace test-ns, got %s", job.GetNamespace())
	}

	runtimeName, _, _ := unstructured.NestedString(job.Object, "spec", "runtimeRef", "name")
	if runtimeName != "does-not-exist-runtime" {
		t.Errorf("expected runtimeRef.name 'does-not-exist-runtime', got %q", runtimeName)
	}
}

func TestBuildValidTrainJob(t *testing.T) {
	job := buildValidTrainJob("test-ns", "good-job", "torch-distributed")

	if job.GetKind() != "TrainJob" {
		t.Errorf("expected kind TrainJob, got %s", job.GetKind())
	}
	if job.GetAPIVersion() != "trainer.kubeflow.org/v1alpha1" {
		t.Errorf("expected apiVersion trainer.kubeflow.org/v1alpha1, got %s", job.GetAPIVersion())
	}
	if job.GetName() != "good-job" {
		t.Errorf("expected name good-job, got %s", job.GetName())
	}
	if job.GetNamespace() != "test-ns" {
		t.Errorf("expected namespace test-ns, got %s", job.GetNamespace())
	}

	runtimeName, _, _ := unstructured.NestedString(job.Object, "spec", "runtimeRef", "name")
	if runtimeName != "torch-distributed" {
		t.Errorf("expected runtimeRef.name 'torch-distributed', got %q", runtimeName)
	}

	runtimeKind, _, _ := unstructured.NestedString(job.Object, "spec", "runtimeRef", "kind")
	if runtimeKind != "ClusterTrainingRuntime" {
		t.Errorf("expected runtimeRef.kind 'ClusterTrainingRuntime', got %q", runtimeKind)
	}
}

func TestIsAdmissionDenied(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "non-status error",
			err:  fmt.Errorf("some random error"),
			want: false,
		},
		{
			name: "webhook denial status error",
			err: &apierrors.StatusError{
				ErrStatus: metav1.Status{
					Code:    403,
					Message: `admission webhook "validator.trainer.kubeflow.org" denied the request: invalid spec`,
				},
			},
			want: true,
		},
		{
			name: "not found status error",
			err:  apierrors.NewNotFound(schema.GroupResource{Resource: "trainjobs"}, "test-job"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isAdmissionDenied(tt.err)
			if got != tt.want {
				t.Errorf("isAdmissionDenied() = %v, want %v", got, tt.want)
			}
		})
	}
}
