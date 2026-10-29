package conformance

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const (
	gaieBackendName     = "gaie-backend"
	gaieBackendLabel    = "ai-conformance.kubernetes.io/gaie-backend"
	gaieBackendPort     = int32(8080)
	gaieListenerPort    = int64(80)
	gaieGatewayName     = "gaie-test-gateway"
	gaiePoolName        = "gaie-test-pool"
	gaieModelObjName    = "gaie-test-model"
	gaieRouteName       = "gaie-test-route"
	gaieProberName      = "gaie-route-prober"
	gaieProbeImage      = "python:3.13-alpine"
	gaieRouteOKMarker   = "GAIE_TEST: PASS"
	gaieRouteDoneMarker = "GAIE_TEST: DONE"

	gaieInferenceGroup = "inference.networking.x-k8s.io"
	gaieGatewayGroup   = "gateway.networking.k8s.io"
)

var (
	inferencePoolGVR  = schema.GroupVersionResource{Group: gaieInferenceGroup, Version: "v1alpha2", Resource: "inferencepools"}
	inferenceModelGVR = schema.GroupVersionResource{Group: gaieInferenceGroup, Version: "v1alpha2", Resource: "inferencemodels"}
	gaieGatewayGVR    = schema.GroupVersionResource{Group: gaieGatewayGroup, Version: "v1", Resource: "gateways"}
	gaieHTTPRouteGVR  = schema.GroupVersionResource{Group: gaieGatewayGroup, Version: "v1", Resource: "httproutes"}

	errGAIENotInstalled = errors.New("gateway API inference extension CRDs not installed")
)

var (
	gaieGatewayClass *string
	gaieModelName    *string
	gaieTimeout      *time.Duration
)

func init() {
	registerFlagGroup("gaie", "Advanced Inference Ingress Flags (TestAdvancedIngressInference)")
	gaieGatewayClass = flag.String("gaie-gateway-class", "",
		"GatewayClass name that supports the Gateway API Inference Extension (GAIE). "+
			"The test is skipped when unset. Platforms that do not support GAIE may leave it unset to opt out.")
	gaieModelName = flag.String("gaie-model-name", "conformance-test-model",
		"Model name registered in InferenceModel for routing verification.")
	gaieTimeout = flag.Duration("gaie-timeout", 5*time.Minute,
		"Timeout for Gateway provisioning and inference request routing to the backend.")
}

// gaieBackendScript is an OpenAI-compatible HTTP stub that the test routes
// inference requests through the gateway to, verifying end-to-end GAIE routing.
var gaieBackendScript = `
import http.server, json, os, socket, socketserver

PORT = int(os.environ.get("PORT", "8080"))
MODEL = os.environ.get("MODEL_NAME", "conformance-test-model")

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, code, body, ct="application/json"):
        d = body if isinstance(body, bytes) else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(d)))
        self.end_headers()
        self.wfile.write(d)
    def do_GET(self):
        if self.path == "/healthz":
            self.send(200, b"ok", "text/plain")
        elif self.path == "/v1/models":
            self.send(200, json.dumps({"object": "list", "data": [{"id": MODEL, "object": "model"}]}))
        else:
            self.send(404, b"not found", "text/plain")
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        self.rfile.read(n)
        if "/completions" in self.path:
            self.send(200, json.dumps({
                "id": "cmpl-conformance",
                "object": "chat.completion",
                "model": MODEL,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "GAIE_ROUTING_OK"}, "finish_reason": "stop"}],
            }))
        else:
            self.send(404, b"not found", "text/plain")

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

Server(("", PORT), Handler).serve_forever()
`

// gaieProbeScript is run inside the probe pod; it polls the gateway until
// the inference backend responds with the expected routing marker.
var gaieProbeScript = `
import urllib.request, json, os, sys, time

gw = os.environ["GATEWAY_ADDRESS"]
port = os.environ.get("GATEWAY_PORT", "80")
model = os.environ.get("MODEL_NAME", "conformance-test-model")
url = "http://{}:{}/v1/chat/completions".format(gw, port)
body = json.dumps({"model": model, "messages": [{"role": "user", "content": "ping"}]}).encode()
headers = {"Content-Type": "application/json"}

for attempt in range(18):
    try:
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read())
            content = resp["choices"][0]["message"]["content"]
            print("GAIE_RESPONSE:", content, flush=True)
            if content == "GAIE_ROUTING_OK":
                print("GAIE_TEST: PASS", flush=True)
            else:
                print("GAIE_TEST: unexpected content:", content, flush=True)
                print("GAIE_TEST: FAIL", flush=True)
            print("GAIE_TEST: DONE", flush=True)
            sys.exit(0 if content == "GAIE_ROUTING_OK" else 1)
    except Exception as e:
        print("attempt {}: {}".format(attempt, e), flush=True)
        time.sleep(10)

print("GAIE_TEST: ERROR: all routing attempts failed", flush=True)
print("GAIE_TEST: DONE", flush=True)
sys.exit(1)
`

// TestAdvancedIngressInference verifies the Advanced Inference Ingress requirement (KAR-0011).
// It checks that the platform supports an implementation of the Gateway API Inference Extension
// (GAIE) that can route requests to inference backends.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/tree/main/kars/0011-advanced-ingress-inference
func TestAdvancedIngressInference(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if !flag.Parsed() {
		flag.Parse()
	}
	if *gaieGatewayClass == "" {
		t.Skip("Skipping Advanced Inference Ingress test: -gaie-gateway-class not set. " +
			"Set it to the name of a GatewayClass that supports GAIE. " +
			"Platforms that do not support GAIE may leave it unset to opt out.")
	}

	clientset := getClientset(t)
	dyn := getDynamicClient(t)
	ctx := context.Background()

	if err := checkGAIEInstalled(clientset); err != nil {
		t.Skipf("Skipping Advanced Inference Ingress test: %v. "+
			"Install the Gateway API Inference Extension to run this test.", err)
	}
	t.Logf("GAIE CRDs verified: %s and %s APIs are available", gaieInferenceGroup, gaieGatewayGroup)

	namespace := randomNamespaceName("gaie-conformance")
	if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create namespace %s: %v", namespace, err)
	}
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: Failed to delete namespace %s: %v", namespace, err)
		}
	})

	// Deploy the mock OpenAI-compatible inference backend.
	backendPod, backendSvc := buildGAIEBackend(namespace, *gaieModelName)
	createTestPod(ctx, t, clientset, backendPod)
	if _, err := clientset.CoreV1().Services(namespace).Create(ctx, backendSvc, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create backend Service: %v", err)
	}

	// Create GAIE resources: InferencePool, InferenceModel, Gateway, HTTPRoute.
	pool := buildInferencePool(namespace, gaiePoolName, gaieBackendLabel, gaieBackendName, gaieBackendPort)
	if _, err := dyn.Resource(inferencePoolGVR).Namespace(namespace).Create(ctx, pool, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create InferencePool: %v", err)
	}

	model := buildInferenceModel(namespace, gaieModelObjName, *gaieModelName, gaiePoolName)
	if _, err := dyn.Resource(inferenceModelGVR).Namespace(namespace).Create(ctx, model, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create InferenceModel: %v", err)
	}

	gw := buildGAIEGateway(namespace, gaieGatewayName, *gaieGatewayClass, gaieListenerPort)
	if _, err := dyn.Resource(gaieGatewayGVR).Namespace(namespace).Create(ctx, gw, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Gateway: %v", err)
	}

	route := buildGAIEHTTPRoute(namespace, gaieRouteName, gaieGatewayName, gaiePoolName, gaieBackendPort)
	if _, err := dyn.Resource(gaieHTTPRouteGVR).Namespace(namespace).Create(ctx, route, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create HTTPRoute: %v", err)
	}

	// Wait for the backend pod separately so Gateway readiness can be checked in parallel.
	if _, err := waitForPodsRunning(ctx, clientset, namespace, []string{gaieBackendName}, *gaieTimeout); err != nil {
		t.Fatalf("Backend pod did not reach Running: %v", err)
	}
	t.Logf("Backend pod %s/%s is Running", namespace, gaieBackendName)

	t.Run("InferenceExtensionInstalled", func(t *testing.T) {
		t.Logf("PASS: %s (InferencePool, InferenceModel) and %s (Gateway, HTTPRoute) APIs are present",
			gaieInferenceGroup, gaieGatewayGroup)
	})

	var gatewayAddress string
	t.Run("GatewayReadiness", func(t *testing.T) {
		addr, err := waitForGatewayReady(ctx, dyn, namespace, gaieGatewayName, *gaieTimeout)
		if err != nil {
			t.Fatalf("FAIL: Gateway %s/%s did not become ready within %s: %v",
				namespace, gaieGatewayName, *gaieTimeout, err)
		}
		gatewayAddress = addr
		t.Logf("PASS: Gateway %s/%s is ready at %s", namespace, gaieGatewayName, addr)
	})
	if t.Failed() || gatewayAddress == "" {
		return
	}

	t.Run("InferenceModelRouting", func(t *testing.T) {
		logs, err := runGAIEProbe(ctx, t, clientset, namespace, gatewayAddress, *gaieModelName, *gaieTimeout)
		if err != nil {
			t.Fatalf("FAIL: inference routing probe failed: %v\nLogs:\n%s", err, logs)
		}
		if !logsContainExactLine(logs, gaieRouteOKMarker) {
			t.Fatalf("FAIL: gateway did not route inference request to backend. Expected %q in logs:\n%s",
				gaieRouteOKMarker, logs)
		}
		t.Logf("PASS: inference request routed through gateway to backend model %s", *gaieModelName)
	})
}

// checkGAIEInstalled verifies that both the Gateway API and the Inference
// Extension CRD groups are registered in the cluster.
func checkGAIEInstalled(c kubernetes.Interface) error {
	for _, gv := range []string{
		gaieInferenceGroup + "/v1alpha2",
		gaieGatewayGroup + "/v1",
	} {
		if _, err := c.Discovery().ServerResourcesForGroupVersion(gv); err != nil {
			return fmt.Errorf("%w: %s is not served (%v)", errGAIENotInstalled, gv, err)
		}
	}
	return nil
}

// buildGAIEBackend returns the mock inference backend Pod and its Service.
func buildGAIEBackend(ns, modelName string) (*corev1.Pod, *corev1.Service) {
	labels := map[string]string{gaieBackendLabel: gaieBackendName}
	nonRoot := true
	uid := int64(65534)
	noEscalation := false
	port := intstr.FromInt32(gaieBackendPort)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gaieBackendName,
			Namespace: ns,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   &nonRoot,
				RunAsUser:      &uid,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:    "backend",
				Image:   gaieProbeImage,
				Command: []string{"python3", "-u", "-c", gaieBackendScript},
				Env: []corev1.EnvVar{
					{Name: "PORT", Value: fmt.Sprintf("%d", gaieBackendPort)},
					{Name: "MODEL_NAME", Value: modelName},
				},
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: gaieBackendPort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: port},
					},
					PeriodSeconds: 2,
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("50m"),
						corev1.ResourceMemory: resource.MustParse("64Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &noEscalation,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
		},
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: gaieBackendName, Namespace: ns, Labels: labels},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name: "http", Port: gaieBackendPort, TargetPort: port,
			}},
		},
	}
	return pod, svc
}

// buildInferencePool builds an InferencePool that selects backend Pods by label.
func buildInferencePool(ns, name, labelKey, labelVal string, port int32) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gaieInferenceGroup + "/v1alpha2",
		"kind":       "InferencePool",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"targetPortNumber": int64(port),
			"selector":         map[string]any{labelKey: labelVal},
		},
	}}
}

// buildInferenceModel builds an InferenceModel that maps modelName to poolName.
func buildInferenceModel(ns, name, modelName, poolName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gaieInferenceGroup + "/v1alpha2",
		"kind":       "InferenceModel",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"modelName":   modelName,
			"criticality": "Standard",
			"poolRef":     map[string]any{"name": poolName},
		},
	}}
}

// buildGAIEGateway builds a Gateway that listens on listenPort using gatewayClass.
func buildGAIEGateway(ns, name, gatewayClass string, listenPort int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gaieGatewayGroup + "/v1",
		"kind":       "Gateway",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"gatewayClassName": gatewayClass,
			"listeners": []any{
				map[string]any{
					"name":     "http",
					"protocol": "HTTP",
					"port":     listenPort,
					"allowedRoutes": map[string]any{
						"namespaces": map[string]any{"from": "Same"},
					},
				},
			},
		},
	}}
}

// buildGAIEHTTPRoute builds an HTTPRoute that routes to an InferencePool backend.
func buildGAIEHTTPRoute(ns, name, gatewayName, poolName string, backendPort int32) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gaieGatewayGroup + "/v1",
		"kind":       "HTTPRoute",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"parentRefs": []any{
				map[string]any{"name": gatewayName, "namespace": ns},
			},
			"rules": []any{
				map[string]any{
					"backendRefs": []any{
						map[string]any{
							"group": gaieInferenceGroup,
							"kind":  "InferencePool",
							"name":  poolName,
							"port":  int64(backendPort),
						},
					},
				},
			},
		},
	}}
}

// waitForGatewayReady polls until the Gateway has a Programmed=True condition
// and at least one address in its status.
func waitForGatewayReady(ctx context.Context, dyn dynamic.Interface, ns, name string, timeout time.Duration) (string, error) {
	var lastAddr string
	var lastConditions string
	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		gw, err := dyn.Resource(gaieGatewayGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}

		// Require Programmed=True before reading addresses.
		conditions, _, _ := unstructured.NestedSlice(gw.Object, "status", "conditions")
		var condParts []string
		programmed := false
		for _, c := range conditions {
			cond, _ := c.(map[string]any)
			typ, _ := cond["type"].(string)
			status, _ := cond["status"].(string)
			condParts = append(condParts, typ+"="+status)
			if typ == "Programmed" && status == "True" {
				programmed = true
			}
		}
		lastConditions = strings.Join(condParts, ", ")
		if !programmed {
			return false, nil
		}

		addrs, _, _ := unstructured.NestedSlice(gw.Object, "status", "addresses")
		for _, a := range addrs {
			addr, _ := a.(map[string]any)
			if v, ok := addr["value"].(string); ok && v != "" {
				lastAddr = v
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return "", fmt.Errorf("Gateway %s/%s not ready after %s (conditions: %s): %w",
			ns, name, timeout, lastConditions, err)
	}
	return lastAddr, nil
}

// runGAIEProbe creates a probe Pod that sends an inference request through the
// gateway and waits for it to complete, returning its logs.
func runGAIEProbe(ctx context.Context, t *testing.T, c kubernetes.Interface, ns, gatewayAddr, modelName string, timeout time.Duration) (string, error) {
	t.Helper()
	nonRoot := true
	uid := int64(65534)
	noEscalation := false

	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: gaieProberName, Namespace: ns},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   &nonRoot,
				RunAsUser:      &uid,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:    "prober",
				Image:   gaieProbeImage,
				Command: []string{"python3", "-u", "-c", gaieProbeScript},
				Env: []corev1.EnvVar{
					{Name: "GATEWAY_ADDRESS", Value: gatewayAddr},
					{Name: "GATEWAY_PORT", Value: fmt.Sprintf("%d", gaieListenerPort)},
					{Name: "MODEL_NAME", Value: modelName},
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("10m"),
						corev1.ResourceMemory: resource.MustParse("32Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("64Mi"),
					},
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &noEscalation,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
		},
	}
	createTestPod(ctx, t, c, probe)

	var logs string
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 3*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(ns).Get(ctx, gaieProberName, metav1.GetOptions{})
		if err != nil {
			if isRetryableAPIError(err) {
				lastAPIError = err
				return false, nil
			}
			return false, err
		}
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return false, nil
		}
		raw, err := c.CoreV1().Pods(ns).GetLogs(gaieProberName, &corev1.PodLogOptions{Container: "prober"}).DoRaw(ctx)
		if err != nil {
			lastAPIError = err
			return false, nil
		}
		logs = string(raw)
		return logsContainExactLine(logs, gaieRouteDoneMarker), nil
	})
	if err != nil {
		return logs, fmt.Errorf("probe pod %s/%s did not complete within %s%s: %w",
			ns, gaieProberName, timeout, lastAPIErrorSuffix(lastAPIError), err)
	}
	return logs, nil
}
