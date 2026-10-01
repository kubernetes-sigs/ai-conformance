package conformance

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// operatorDef bundles operator-specific configuration for a curated AI operator.
// Adding a new operator requires only a new entry in supportedOperators.
type operatorDef struct {
	namespace     string
	labelSelector string
	webhookName   string
	crdName       string
	crGVR         schema.GroupVersionResource
	// runtimeGVR is nil when the operator has no separate runtime resource.
	runtimeGVR     *schema.GroupVersionResource
	defaultRuntime string
	// successConditions are condition types that indicate the controller has
	// successfully reconciled a CR. Checked positively to avoid matching
	// non-ready states like Suspended=True.
	successConditions []string
	buildInvalidCR    func(namespace, name string) *unstructured.Unstructured
	buildValidCR      func(namespace, name, runtime string) *unstructured.Unstructured
}

// supportedOperators is the curated set of AI operators this test can exercise.
// Each entry bundles the GVRs, webhook name, CRD name, and CR builders for that
// operator, so the test cannot be trivially re-pointed at an arbitrary CRD.
var supportedOperators = map[string]operatorDef{
	"kubeflow-trainer": {
		namespace:     "kubeflow-system",
		labelSelector: "app.kubernetes.io/name=kubeflow-trainer",
		webhookName:   "validator.trainer.kubeflow.org",
		crdName:       "trainjobs.trainer.kubeflow.org",
		crGVR: schema.GroupVersionResource{
			Group:    "trainer.kubeflow.org",
			Version:  "v1alpha1",
			Resource: "trainjobs",
		},
		runtimeGVR: &schema.GroupVersionResource{
			Group:    "trainer.kubeflow.org",
			Version:  "v1alpha1",
			Resource: "clustertrainingruntimes",
		},
		defaultRuntime:    "torch-distributed",
		successConditions: []string{"Complete"},
		buildInvalidCR:    buildInvalidTrainJob,
		buildValidCR:      buildValidTrainJob,
	},
}

var (
	operatorName             *string
	operatorRuntimeName      *string
	operatorReadyTimeout     *time.Duration
	operatorReconcileTimeout *time.Duration
)

func init() {
	operatorName = flag.String("operator", "kubeflow-trainer",
		"Name of the AI operator to test. Supported: kubeflow-trainer.")
	operatorRuntimeName = flag.String("operator-runtime-name", "",
		"Override the operator's default runtime name for the reconciliation test.")
	operatorReadyTimeout = flag.Duration("operator-ready-timeout", 5*time.Minute,
		"Timeout for operator pods to become Ready.")
	operatorReconcileTimeout = flag.Duration("operator-reconcile-timeout", 3*time.Minute,
		"Timeout for the controller to reconcile a valid custom resource.")
}

var crdGVR = schema.GroupVersionResource{
	Group:    "apiextensions.k8s.io",
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

// TestRobustCRDControllerOperation verifies the Robust CRD and Controller Operation requirement.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/blob/main/kars/0063-robust-crd-controller-operation/README.md
func TestRobustCRDControllerOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if !flag.Parsed() {
		flag.Parse()
	}

	op, ok := supportedOperators[*operatorName]
	if !ok {
		names := make([]string, 0, len(supportedOperators))
		for k := range supportedOperators {
			names = append(names, k)
		}
		sort.Strings(names)
		t.Fatalf("Unsupported -operator %q; supported operators: %s", *operatorName, strings.Join(names, ", "))
	}

	runtimeName := op.defaultRuntime
	if *operatorRuntimeName != "" {
		runtimeName = *operatorRuntimeName
	}

	clientset := getClientset(t)
	dynamicClient := getDynamicClient(t)

	ctx := context.Background()

	namespace := randomNamespaceName("crd-controller")
	if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create test namespace: %v", err)
	}
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: %v. Please ensure this namespace is terminated manually to avoid resource leaks.", err)
		}
	})

	t.Run("OperatorPodsRunning", func(t *testing.T) {
		testOperatorPodsRunning(ctx, t, clientset, op)
	})

	t.Run("CRDRegistered", func(t *testing.T) {
		testCRDRegistered(ctx, t, dynamicClient, op)
	})

	t.Run("WebhookOperational", func(t *testing.T) {
		testWebhookOperational(ctx, t, clientset, op)
	})

	t.Run("WebhookRejectsInvalidCR", func(t *testing.T) {
		testWebhookRejectsInvalidCR(ctx, t, dynamicClient, namespace, op)
	})

	t.Run("ControllerReconcilesCR", func(t *testing.T) {
		testControllerReconcilesCR(ctx, t, dynamicClient, namespace, op, runtimeName)
	})
}

func testOperatorPodsRunning(ctx context.Context, t *testing.T, clientset kubernetes.Interface, op operatorDef) {
	t.Helper()
	t.Logf("Verifying operator pods are Ready in namespace %s (selector: %s)...", op.namespace, op.labelSelector)

	var lastPodCount int
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, *operatorReadyTimeout, true, func(ctx context.Context) (bool, error) {
		pods, err := clientset.CoreV1().Pods(op.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: op.labelSelector,
		})
		if err != nil {
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil

		if len(pods.Items) == 0 {
			lastPodCount = 0
			return false, nil
		}

		allReady := true
		for _, pod := range pods.Items {
			if !podIsReady(&pod) {
				allReady = false
				t.Logf("  Pod %s not ready (phase: %s)", pod.Name, pod.Status.Phase)
			}
		}
		lastPodCount = len(pods.Items)
		return allReady, nil
	})
	if err != nil {
		t.Fatalf("FAIL: Operator pods did not become Ready (found %d pods)%s: %v",
			lastPodCount, lastAPIErrorSuffix(lastAPIError), err)
	}
	t.Logf("PASS: %d operator pod(s) are Ready in namespace %s", lastPodCount, op.namespace)
}

func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func testCRDRegistered(ctx context.Context, t *testing.T, dynamicClient dynamic.Interface, op operatorDef) {
	t.Helper()
	t.Logf("Verifying CRD %s is registered and Established...", op.crdName)

	crd, err := dynamicClient.Resource(crdGVR).Get(ctx, op.crdName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("FAIL: CRD %s is not registered with the API server: %v", op.crdName, err)
	}

	if err := verifyCRDEstablished(crd); err != nil {
		t.Fatalf("FAIL: CRD %s is registered but not Established: %v", op.crdName, err)
	}

	t.Logf("PASS: CRD %s is registered and Established", op.crdName)
}

func verifyCRDEstablished(crd *unstructured.Unstructured) error {
	conditions, found, err := unstructured.NestedSlice(crd.Object, "status", "conditions")
	if err != nil {
		return fmt.Errorf("failed to read status.conditions: %w", err)
	}
	if !found || len(conditions) == 0 {
		return fmt.Errorf("CRD has no status conditions")
	}

	for _, c := range conditions {
		condition, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(condition, "type")
		condStatus, _, _ := unstructured.NestedString(condition, "status")
		if condType == "Established" && condStatus == "True" {
			return nil
		}
	}
	return fmt.Errorf("Established=True condition not found")
}

func testWebhookOperational(ctx context.Context, t *testing.T, clientset kubernetes.Interface, op operatorDef) {
	t.Helper()
	t.Logf("Verifying ValidatingWebhookConfiguration %s exists...", op.webhookName)

	webhook, err := clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(
		ctx, op.webhookName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("FAIL: ValidatingWebhookConfiguration %s not found: %v", op.webhookName, err)
	}

	if len(webhook.Webhooks) == 0 {
		t.Fatalf("FAIL: ValidatingWebhookConfiguration %s has no webhooks", op.webhookName)
	}

	for _, wh := range webhook.Webhooks {
		if len(wh.Rules) == 0 {
			t.Errorf("FAIL: Webhook %s has no rules configured", wh.Name)
		}
		if len(wh.ClientConfig.CABundle) == 0 {
			t.Errorf("FAIL: Webhook %s has no caBundle set", wh.Name)
		}
		t.Logf("  Webhook: %s (rules: %d, caBundle: %d bytes)", wh.Name, len(wh.Rules), len(wh.ClientConfig.CABundle))
	}
	if t.Failed() {
		return
	}
	t.Logf("PASS: ValidatingWebhookConfiguration %s exists with %d webhook(s)", op.webhookName, len(webhook.Webhooks))
}

func testWebhookRejectsInvalidCR(ctx context.Context, t *testing.T, dynamicClient dynamic.Interface, namespace string, op operatorDef) {
	t.Helper()
	t.Logf("Submitting invalid CR to verify webhook rejection...")

	invalidCR := op.buildInvalidCR(namespace, "invalid-cr")

	_, err := dynamicClient.Resource(op.crGVR).Namespace(namespace).Create(ctx, invalidCR, metav1.CreateOptions{})
	if err == nil {
		t.Cleanup(func() {
			_ = dynamicClient.Resource(op.crGVR).Namespace(namespace).Delete(ctx, "invalid-cr", metav1.DeleteOptions{})
		})
		t.Fatalf("FAIL: Invalid CR was accepted by the API server; expected admission webhook to reject it")
	}

	if isAdmissionDenied(err) {
		t.Logf("PASS: Webhook correctly rejected invalid CR: %v", err)
	} else {
		t.Fatalf("FAIL: CR creation failed with an unexpected error (expected admission rejection): %v", err)
	}
}

func testControllerReconcilesCR(ctx context.Context, t *testing.T, dynamicClient dynamic.Interface, namespace string, op operatorDef, runtimeName string) {
	t.Helper()

	if op.runtimeGVR != nil {
		t.Logf("Verifying runtime %s exists...", runtimeName)
		if _, err := dynamicClient.Resource(*op.runtimeGVR).Get(ctx, runtimeName, metav1.GetOptions{}); err != nil {
			t.Fatalf("FAIL: Runtime %s not found (ensure operator was installed with default runtimes): %v",
				runtimeName, err)
		}
	}

	crName := "valid-cr"
	validCR := op.buildValidCR(namespace, crName, runtimeName)

	t.Logf("Creating valid CR %s...", crName)
	if _, err := dynamicClient.Resource(op.crGVR).Namespace(namespace).Create(ctx, validCR, metav1.CreateOptions{}); err != nil {
		t.Fatalf("FAIL: Valid CR was rejected: %v", err)
	}
	t.Cleanup(func() {
		t.Logf("Cleaning up CR %s...", crName)
		err := dynamicClient.Resource(op.crGVR).Namespace(namespace).Delete(ctx, crName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Logf("Failed to clean up CR %s: %v", crName, err)
		}
	})

	t.Logf("Waiting for controller to reconcile CR %s...", crName)
	if err := waitForCRReconciled(ctx, t, dynamicClient, namespace, crName, op); err != nil {
		t.Fatalf("FAIL: Controller did not reconcile CR %s: %v", crName, err)
	}

	t.Logf("PASS: Controller reconciled CR %s", crName)
}

func buildInvalidTrainJob(namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "trainer.kubeflow.org/v1alpha1",
			"kind":       "TrainJob",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"runtimeRef": map[string]interface{}{
					"name":     "does-not-exist-runtime",
					"apiGroup": "trainer.kubeflow.org",
					"kind":     "ClusterTrainingRuntime",
				},
			},
		},
	}
}

func buildValidTrainJob(namespace, name, runtimeName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "trainer.kubeflow.org/v1alpha1",
			"kind":       "TrainJob",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"runtimeRef": map[string]interface{}{
					"name":     runtimeName,
					"apiGroup": "trainer.kubeflow.org",
					"kind":     "ClusterTrainingRuntime",
				},
			},
		},
	}
}

func waitForCRReconciled(ctx context.Context, t *testing.T, dynamicClient dynamic.Interface, namespace, name string, op operatorDef) error {
	t.Helper()

	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, *operatorReconcileTimeout, true, func(ctx context.Context) (bool, error) {
		cr, err := dynamicClient.Resource(op.crGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil

		conditions, found, err := unstructured.NestedSlice(cr.Object, "status", "conditions")
		if err != nil || !found || len(conditions) == 0 {
			return false, nil
		}

		for _, c := range conditions {
			condition, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			condType, _, _ := unstructured.NestedString(condition, "type")
			condStatus, _, _ := unstructured.NestedString(condition, "status")
			condReason, _, _ := unstructured.NestedString(condition, "reason")
			t.Logf("  CR condition: type=%s, status=%s, reason=%s", condType, condStatus, condReason)

			if condStatus == "True" && condType == "Failed" {
				return false, fmt.Errorf("CR %s reached a terminal failed state", name)
			}
		}

		for _, c := range conditions {
			condition, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			condType, _, _ := unstructured.NestedString(condition, "type")
			condStatus, _, _ := unstructured.NestedString(condition, "status")
			for _, successType := range op.successConditions {
				if condType == successType && condStatus == "True" {
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("CR %s did not reach a success condition %v within %s%s: %w",
			name, op.successConditions, *operatorReconcileTimeout, lastAPIErrorSuffix(lastAPIError), err)
	}
	return nil
}

func isAdmissionDenied(err error) bool {
	if err == nil {
		return false
	}
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	msg := strings.ToLower(statusErr.ErrStatus.Message)
	return strings.Contains(msg, "denied the request") ||
		strings.Contains(msg, "admission webhook")
}
