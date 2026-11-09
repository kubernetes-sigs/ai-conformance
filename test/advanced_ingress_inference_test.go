package conformance

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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
	gaieBackend0Name    = "gaie-backend-0"
	gaieBackend1Name    = "gaie-backend-1"
	gaieBackendLabel    = "ai-conformance.kubernetes.io/gaie-backend"
	gaieBackendPort     = int32(8080)
	gaieListenerPort    = 80
	gaieEPPName         = "gaie-epp"
	gaieEPPPort         = int32(9002)
	gaieGatewayName     = "gaie-test-gateway"
	gaiePoolName        = "gaie-test-pool"
	gaieRouteName       = "gaie-test-route"
	gaieProberName      = "gaie-route-prober"
	gaieRouteOKMarker   = "GAIE_TEST: PASS"
	gaieDeleteTimeout   = 5 * time.Minute
	gaieDefaultEPPImage = "registry.k8s.io/gateway-api-inference-extension/lwepp:v1.6.2"

	gaieInferenceGroup = "inference.networking.x-k8s.io"
	gaieGatewayGroup   = "gateway.networking.k8s.io"
)

var (
	inferencePoolGVR = schema.GroupVersionResource{Group: gaieInferenceGroup, Version: "v1", Resource: "inferencepools"}
	gaieGatewayGVR   = schema.GroupVersionResource{Group: gaieGatewayGroup, Version: "v1", Resource: "gateways"}
	gaieHTTPRouteGVR = schema.GroupVersionResource{Group: gaieGatewayGroup, Version: "v1", Resource: "httproutes"}

	errGAIENotInstalled = errors.New("gateway API inference extension CRDs not installed")
)

var (
	gaieGatewayClass *string
	gaieEPPImage     *string
	gaieTimeout      *time.Duration
)

func init() {
	registerFlagGroup("gaie", "Advanced Inference Ingress Flags (TestAdvancedInferenceIngress)")
	gaieGatewayClass = flag.String("gaie-gateway-class", "",
		"GatewayClass name that supports the Gateway API Inference Extension (GAIE). "+
			"The test is skipped when unset. Platforms that do not support GAIE may leave it unset to opt out.")
	gaieEPPImage = flag.String("gaie-epp-image", gaieDefaultEPPImage,
		"Container image for the lightweight Endpoint Picker (EPP) used to verify endpoint-picker routing.")
	gaieTimeout = flag.Duration("gaie-timeout", 5*time.Minute,
		"Timeout for Gateway provisioning and inference request routing.")
}

// gaieBackendScript is an OpenAI-compatible HTTP stub.
// Each backend Pod returns its BACKEND_NAME so the prober can verify that
// endpoint-picker routing reached the correct Pod. The server binds
// dual-stack so both IPv4-only and IPv6-only clusters work.
const gaieBackendScript = `
import http.server, json, os, socket

PORT = int(os.environ.get("PORT", "8080"))
BACKEND_NAME = os.environ.get("BACKEND_NAME", "unknown")

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def reply(self, code, body, ct="application/json"):
        d = body if isinstance(body, bytes) else body.encode()
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", str(len(d)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(d)
    def do_GET(self):
        if self.path == "/healthz":
            self.reply(200, b"ok", "text/plain")
        elif self.path == "/v1/models":
            self.reply(200, json.dumps({"object":"list","data":[{"id":"test-model","object":"model"}]}))
        else:
            self.reply(404, b"not found", "text/plain")
    def do_POST(self):
        if "/completions" in self.path:
            self.reply(200, json.dumps({
                "id": "cmpl-conformance", "object": "chat.completion", "model": "test-model",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": BACKEND_NAME}, "finish_reason": "stop"}],
            }))
        else:
            self.reply(404, b"not found", "text/plain")

class Server(http.server.ThreadingHTTPServer):
    address_family = socket.AF_INET6 if socket.has_dualstack_ipv6() else socket.AF_INET
    request_queue_size = 128
    def server_bind(self):
        if self.address_family == socket.AF_INET6:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()

Server(("::" if Server.address_family == socket.AF_INET6 else "", PORT), Handler).serve_forever()
`

// gaieProbeScript is run inside the probe pod. It waits for the gateway to be
// reachable, then verifies that requests carrying test-epp-endpoint-selection
// reach the designated backend Pod, exercising the EPP routing contract.
const gaieProbeScript = `
import urllib.request, json, os, sys, time

URL = os.environ["GATEWAY_URL"]
BACKEND_0 = os.environ["BACKEND_0_NAME"]
BACKEND_1 = os.environ["BACKEND_1_NAME"]
IP_0 = os.environ["BACKEND_0_IP"]
IP_1 = os.environ["BACKEND_1_IP"]
HDR = "test-epp-endpoint-selection"
BODY = json.dumps({"model":"test-model","messages":[{"role":"user","content":"ping"}]}).encode()

def call(target_ip=""):
    h = {"Content-Type": "application/json"}
    if target_ip:
        h[HDR] = target_ip
    req = urllib.request.Request(URL, data=BODY, headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["choices"][0]["message"]["content"]

# Wait for the gateway to be reachable before testing endpoint selection.
for i in range(36):
    try:
        call()
        break
    except Exception as e:
        print("warmup %d: %s" % (i, e), flush=True)
        time.sleep(5)
else:
    print("GAIE_TEST: ERROR: gateway not reachable after warmup", flush=True)
    sys.exit(1)

def verify(ip, expected, label):
    for i in range(10):
        try:
            got = call(ip)
            if got == expected:
                print("GAIE_TEST: %s reached %s" % (label, expected), flush=True)
                return True
            print("attempt %d: %s got %s, want %s" % (i, label, got, expected), flush=True)
        except Exception as e:
            print("attempt %d: %s error: %s" % (i, label, e), flush=True)
        time.sleep(5)
    return False

ok0 = verify(IP_0, BACKEND_0, "selection-to-backend-0")
ok1 = verify(IP_1, BACKEND_1, "selection-to-backend-1")
if ok0 and ok1:
    print("GAIE_TEST: PASS", flush=True)
else:
    if not ok0:
        print("GAIE_TEST: FAIL: requests pinned to %s did not reach %s" % (IP_0, BACKEND_0), flush=True)
    if not ok1:
        print("GAIE_TEST: FAIL: requests pinned to %s did not reach %s" % (IP_1, BACKEND_1), flush=True)
print("GAIE_TEST: DONE", flush=True)
sys.exit(0 if ok0 and ok1 else 1)
`

// TestAdvancedInferenceIngress verifies the Advanced Inference Ingress
// requirement (KAR-0011): a platform-deployed GAIE implementation routes
// inference requests to backends through endpoint-picker logic.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/tree/main/kars/0011-advanced-ingress-inference
func TestAdvancedInferenceIngress(t *testing.T) {
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

	ctx := context.Background()
	clientset := getClientset(t)
	dyn := getDynamicClient(t)

	// When the operator explicitly sets -gaie-gateway-class, missing CRDs indicate
	// a misconfigured cluster rather than an opt-out, so fail hard.
	if err := checkGAIEInstalled(clientset); err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %v. Install the Gateway API Inference Extension CRDs to run this test.", err)
	}
	t.Logf("GAIE CRDs verified: %s/v1 and %s/v1 APIs are available", gaieInferenceGroup, gaieGatewayGroup)

	namespace := randomNamespaceName("gaie-conformance")
	if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Failed to create namespace %s: %v", namespace, err)
	}
	t.Cleanup(func() {
		// Delete Gateway API and GAIE resources explicitly first: cloud Gateway
		// controllers may hold finalizers that block namespace termination.
		if err := deleteGAIEResourcesAndWait(ctx, dyn, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: %v", err)
		}
		if err := deleteNamespaceAndWaitTimeout(ctx, t, clientset, namespace, gaieDeleteTimeout); err != nil {
			t.Errorf("CLEANUP FAILURE: %v. Ensure namespace %s is manually terminated to avoid resource leaks.", err, namespace)
		}
	})

	// Deploy two inference backend Pods. Each returns its own name so the
	// prober can distinguish which Pod served a given request.
	for _, name := range []string{gaieBackend0Name, gaieBackend1Name} {
		pod := buildGAIEBackendPod(namespace, name)
		createTestPod(ctx, t, clientset, pod)
	}

	// Deploy the lightweight EPP that exercises the endpoint-picker contract.
	sa, role, rb, eppPod, eppSvc := buildGAIEEPP(namespace, gaiePoolName, *gaieEPPImage)
	if _, err := clientset.CoreV1().ServiceAccounts(namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create EPP ServiceAccount: %v", err)
	}
	if _, err := clientset.RbacV1().Roles(namespace).Create(ctx, role, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create EPP Role: %v", err)
	}
	if _, err := clientset.RbacV1().RoleBindings(namespace).Create(ctx, rb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create EPP RoleBinding: %v", err)
	}
	createTestPod(ctx, t, clientset, eppPod)
	if _, err := clientset.CoreV1().Services(namespace).Create(ctx, eppSvc, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create EPP Service: %v", err)
	}

	// Create GAIE resources.
	pool := buildInferencePool(namespace, gaiePoolName, gaieBackendLabel, gaieEPPName, gaieEPPPort, gaieBackendPort)
	if _, err := dyn.Resource(inferencePoolGVR).Namespace(namespace).Create(ctx, pool, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create InferencePool: %v", err)
	}
	gw := buildGAIEGateway(namespace, gaieGatewayName, *gaieGatewayClass, gaieListenerPort)
	if _, err := dyn.Resource(gaieGatewayGVR).Namespace(namespace).Create(ctx, gw, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Gateway: %v", err)
	}
	route := buildGAIEHTTPRoute(namespace, gaieRouteName, gaieGatewayName, gaiePoolName, gaieBackendPort)
	if _, err := dyn.Resource(gaieHTTPRouteGVR).Namespace(namespace).Create(ctx, route, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create HTTPRoute: %v", err)
	}

	// Wait for both backends and the EPP to reach Running before proceeding.
	backends, err := waitForPodsRunning(ctx, clientset, namespace,
		[]string{gaieBackend0Name, gaieBackend1Name, gaieEPPName}, *gaieTimeout)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: inference or EPP pods did not start: %v", err)
	}
	backend0IP := backends[gaieBackend0Name].Status.PodIP
	backend1IP := backends[gaieBackend1Name].Status.PodIP
	t.Logf("Backend pods running: %s=%s %s=%s", gaieBackend0Name, backend0IP, gaieBackend1Name, backend1IP)

	t.Run("InferenceExtensionInstalled", func(t *testing.T) {
		t.Logf("PASS: %s/v1 (InferencePool) and %s/v1 (Gateway, HTTPRoute) APIs are present",
			gaieInferenceGroup, gaieGatewayGroup)
	})

	var gatewayAddress string
	t.Run("GatewayReadiness", func(t *testing.T) {
		addr, err := waitForGAIEGatewayProgrammed(ctx, dyn, namespace, gaieGatewayName, *gaieGatewayClass, *gaieTimeout)
		if err != nil {
			t.Fatalf("FAIL: %v", err)
		}
		if err := waitForInferencePoolAccepted(ctx, dyn, namespace, gaiePoolName, *gaieTimeout); err != nil {
			t.Fatalf("FAIL: %v", err)
		}
		if err := waitForGAIEHTTPRouteAccepted(ctx, dyn, namespace, gaieRouteName, gaieGatewayName, *gaieTimeout); err != nil {
			t.Fatalf("FAIL: %v", err)
		}
		gatewayAddress = addr
		t.Logf("PASS: Gateway %s/%s is Programmed at %s; InferencePool and HTTPRoute are Accepted", namespace, gaieGatewayName, addr)
	})
	if t.Failed() || gatewayAddress == "" {
		return
	}

	t.Run("EndpointPickerRouting", func(t *testing.T) {
		gwURL := fmt.Sprintf("http://%s/v1/chat/completions",
			net.JoinHostPort(gatewayAddress, strconv.Itoa(gaieListenerPort)))
		logs, err := runGAIEProbe(ctx, t, clientset, namespace, gwURL, backend0IP, backend1IP, *gaieTimeout)
		if err != nil {
			t.Fatalf("FAIL: endpoint-picker routing probe failed: %v\nLogs:\n%s", err, logs)
		}
		if !logsContainExactLine(logs, gaieRouteOKMarker) {
			t.Fatalf("FAIL: EPP did not route requests to the designated Pods. Expected %q in probe logs:\n%s",
				gaieRouteOKMarker, logs)
		}
		t.Logf("PASS: endpoint-picker routing verified for both backend Pods")
	})
}

// checkGAIEInstalled verifies that the Gateway API Inference Extension (v1)
// and the Gateway API CRD groups are registered in the cluster.
func checkGAIEInstalled(c kubernetes.Interface) error {
	for _, gv := range []string{
		gaieInferenceGroup + "/v1",
		gaieGatewayGroup + "/v1",
	} {
		if _, err := c.Discovery().ServerResourcesForGroupVersion(gv); err != nil {
			return fmt.Errorf("%w: %s is not served (%v)", errGAIENotInstalled, gv, err)
		}
	}
	return nil
}

// buildGAIEBackendPod builds a single mock inference backend Pod that returns
// its name in every chat-completions response.
func buildGAIEBackendPod(ns, name string) *corev1.Pod {
	port := intstr.FromInt32(gaieBackendPort)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{gaieBackendLabel: "gaie-backend"},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: restrictedPodSecurityContext(),
			Containers: []corev1.Container{{
				Name:    "server",
				Image:   metricsWorkloadImage,
				Command: []string{"python3", "-u", "-c", gaieBackendScript},
				Env: []corev1.EnvVar{
					{Name: "PORT", Value: strconv.Itoa(int(gaieBackendPort))},
					{Name: "BACKEND_NAME", Value: name},
				},
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: gaieBackendPort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: port}},
					PeriodSeconds: 2,
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("50m"),
						corev1.ResourceMemory: resource.MustParse("64Mi"),
					},
					Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
				},
				SecurityContext: restrictedContainerSecurityContext(),
			}},
		},
	}
}

// buildGAIEEPP builds the lightweight EPP (lwepp) resources: ServiceAccount,
// Role, RoleBinding, Pod, and Service. The EPP watches the InferencePool's
// backend Pods and handles ext-proc endpoint selection for the gateway.
func buildGAIEEPP(ns, poolName, image string) (*corev1.ServiceAccount, *rbacv1.Role, *rbacv1.RoleBinding, *corev1.Pod, *corev1.Service) {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: gaieEPPName, Namespace: ns},
	}
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: gaieEPPName, Namespace: ns},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{gaieInferenceGroup}, Resources: []string{"inferencepools"}, Verbs: []string{"get", "list", "watch"}},
			{APIGroups: []string{gaieInferenceGroup}, Resources: []string{"inferencepools/status"}, Verbs: []string{"get", "update", "patch"}},
		},
	}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: gaieEPPName, Namespace: ns},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: gaieEPPName},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: gaieEPPName, Namespace: ns}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gaieEPPName,
			Namespace: ns,
			Labels:    map[string]string{"ai-conformance.kubernetes.io/gaie-epp": gaieEPPName},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: gaieEPPName,
			RestartPolicy:      corev1.RestartPolicyNever,
			SecurityContext:    restrictedPodSecurityContext(),
			Containers: []corev1.Container{{
				Name:  "epp",
				Image: image,
				Args:  []string{"-poolName=" + poolName, "-poolNamespace=" + ns},
				Ports: []corev1.ContainerPort{{Name: "grpc", ContainerPort: gaieEPPPort}},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("50m"),
						corev1.ResourceMemory: resource.MustParse("64Mi"),
					},
					Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
				},
				SecurityContext: restrictedContainerSecurityContext(),
			}},
		},
	}
	eppPort := intstr.FromInt32(gaieEPPPort)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: gaieEPPName, Namespace: ns},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"ai-conformance.kubernetes.io/gaie-epp": gaieEPPName},
			Ports: []corev1.ServicePort{{
				Name: "grpc", Port: gaieEPPPort, TargetPort: eppPort, Protocol: corev1.ProtocolTCP,
			}},
		},
	}
	return sa, role, rb, pod, svc
}

// buildInferencePool builds an InferencePool that selects backend Pods by
// label and wires an endpoint picker via endpointPickerRef.
func buildInferencePool(ns, name, labelKey, eppName string, eppPort, targetPort int32) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gaieInferenceGroup + "/v1",
		"kind":       "InferencePool",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"targetPortNumber": int64(targetPort),
			"selector":         map[string]any{labelKey: "gaie-backend"},
			"endpointPickerRef": map[string]any{
				"name":       eppName,
				"portNumber": int64(eppPort),
			},
		},
	}}
}

// buildGAIEGateway builds a Gateway with a single HTTP listener.
func buildGAIEGateway(ns, name, gatewayClass string, listenPort int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gaieGatewayGroup + "/v1",
		"kind":       "Gateway",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"gatewayClassName": gatewayClass,
			"listeners": []any{
				map[string]any{
					"name": "http", "protocol": "HTTP", "port": int64(listenPort),
					"allowedRoutes": map[string]any{"namespaces": map[string]any{"from": "Same"}},
				},
			},
		},
	}}
}

// buildGAIEHTTPRoute builds an HTTPRoute that routes all traffic to an
// InferencePool backend.
func buildGAIEHTTPRoute(ns, name, gatewayName, poolName string, backendPort int32) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gaieGatewayGroup + "/v1",
		"kind":       "HTTPRoute",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"parentRefs": []any{map[string]any{"name": gatewayName, "namespace": ns}},
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

// waitForGAIEGatewayProgrammed polls until the Gateway is Accepted and
// Programmed and returns its first address.
func waitForGAIEGatewayProgrammed(ctx context.Context, dyn dynamic.Interface, ns, name, class string, timeout time.Duration) (string, error) {
	var addr string
	gw, err := pollGatewayAPIObject(ctx, dyn, gaieGatewayGVR, ns, name, timeout, func(o *unstructured.Unstructured) bool {
		addr = gatewayAddress(o)
		return conditionTrue(o.Object, "Accepted", "status", "conditions") &&
			conditionTrue(o.Object, "Programmed", "status", "conditions") && addr != ""
	})
	if err != nil {
		return "", fmt.Errorf("Gateway %s/%s of GatewayClass %s was not Programmed within %v "+
			"(Accepted=%t Programmed=%t address=%q): %w; raise -gaie-timeout or verify the GatewayClass",
			ns, name, class, timeout.Round(time.Second),
			conditionTrue(gw.Object, "Accepted", "status", "conditions"),
			conditionTrue(gw.Object, "Programmed", "status", "conditions"),
			gatewayAddress(gw), err)
	}
	return addr, nil
}

// waitForInferencePoolAccepted polls until the InferencePool has Accepted=True.
func waitForInferencePoolAccepted(ctx context.Context, dyn dynamic.Interface, ns, name string, timeout time.Duration) error {
	pool, err := pollGatewayAPIObject(ctx, dyn, inferencePoolGVR, ns, name, timeout, func(o *unstructured.Unstructured) bool {
		return conditionTrue(o.Object, "Accepted", "status", "conditions")
	})
	if err != nil {
		return fmt.Errorf("InferencePool %s/%s was not Accepted within %v: %w; status: %s",
			ns, name, timeout.Round(time.Second), err, statusJSON(pool))
	}
	return nil
}

// waitForGAIEHTTPRouteAccepted polls until the HTTPRoute has Accepted=True and
// ResolvedRefs=True in the status entry for the GAIE Gateway.
func waitForGAIEHTTPRouteAccepted(ctx context.Context, dyn dynamic.Interface, ns, name, gwName string, timeout time.Duration) error {
	route, err := pollGatewayAPIObject(ctx, dyn, gaieHTTPRouteGVR, ns, name, timeout, func(o *unstructured.Unstructured) bool {
		parents, _, _ := unstructured.NestedSlice(o.Object, "status", "parents")
		for _, p := range parents {
			m, ok := p.(map[string]any)
			if !ok {
				continue
			}
			if pName, _, _ := unstructured.NestedString(m, "parentRef", "name"); pName != gwName {
				continue
			}
			if conditionTrue(m, "Accepted", "conditions") && conditionTrue(m, "ResolvedRefs", "conditions") {
				return true
			}
		}
		return false
	})
	if err != nil {
		return fmt.Errorf("HTTPRoute %s/%s was not Accepted with ResolvedRefs by Gateway %s within %v: %w; status: %s",
			ns, name, gwName, timeout.Round(time.Second), err, statusJSON(route))
	}
	return nil
}

// deleteGAIEResourcesAndWait deletes the HTTPRoute, InferencePool, and Gateway
// and waits for the Gateway to be fully removed. Cloud Gateway controllers may
// hold finalizers; explicitly deleting before the namespace avoids a race with
// the 1-minute namespace deletion timeout.
func deleteGAIEResourcesAndWait(ctx context.Context, dyn dynamic.Interface, ns string) error {
	for _, gvr := range []schema.GroupVersionResource{gaieHTTPRouteGVR, inferencePoolGVR, gaieGatewayGVR} {
		name := map[schema.GroupVersionResource]string{
			gaieHTTPRouteGVR: gaieRouteName,
			inferencePoolGVR: gaiePoolName,
			gaieGatewayGVR:   gaieGatewayName,
		}[gvr]
		if err := dyn.Resource(gvr).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete %s %s: %w", gvr.Resource, name, err)
		}
	}
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, gaieDeleteTimeout, true, func(ctx context.Context) (bool, error) {
		_, err := dyn.Resource(gaieGatewayGVR).Namespace(ns).Get(ctx, gaieGatewayName, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			return true, nil
		case err == nil, isRetryableAPIError(err):
			lastAPIError = err
			return false, nil
		case ctx.Err() != nil:
			return false, ctx.Err()
		default:
			return false, err
		}
	})
	if err != nil {
		return fmt.Errorf("Gateway %s was not deleted within %v%s: %w",
			gaieGatewayName, gaieDeleteTimeout, lastAPIErrorSuffix(lastAPIError), err)
	}
	return nil
}

// runGAIEProbe creates a probe Pod that sends inference requests through the
// gateway with endpoint-picker selection headers and waits for it to finish.
func runGAIEProbe(ctx context.Context, t *testing.T, c kubernetes.Interface, ns, gwURL, backend0IP, backend1IP string, timeout time.Duration) (string, error) {
	t.Helper()
	probe := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: gaieProberName, Namespace: ns},
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: restrictedPodSecurityContext(),
			Containers: []corev1.Container{{
				Name:    "prober",
				Image:   metricsWorkloadImage,
				Command: []string{"python3", "-u", "-c", gaieProbeScript},
				Env: []corev1.EnvVar{
					{Name: "GATEWAY_URL", Value: gwURL},
					{Name: "BACKEND_0_NAME", Value: gaieBackend0Name},
					{Name: "BACKEND_1_NAME", Value: gaieBackend1Name},
					{Name: "BACKEND_0_IP", Value: backend0IP},
					{Name: "BACKEND_1_IP", Value: backend1IP},
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("10m"),
						corev1.ResourceMemory: resource.MustParse("32Mi"),
					},
					Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")},
				},
				SecurityContext: restrictedContainerSecurityContext(),
			}},
		},
	}
	createTestPod(ctx, t, c, probe)

	var logs string
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 3*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(ns).Get(ctx, gaieProberName, metav1.GetOptions{})
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			if isRetryableAPIError(err) {
				lastAPIError = err
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return false, nil
		}
		// Pod has terminated; logs are final.
		raw, err := c.CoreV1().Pods(ns).GetLogs(gaieProberName, &corev1.PodLogOptions{Container: "prober"}).DoRaw(ctx)
		if err != nil {
			lastAPIError = err
			return false, nil
		}
		logs = string(raw)
		if pod.Status.Phase == corev1.PodFailed {
			return true, fmt.Errorf("probe pod failed (exit non-zero). Logs:\n%s", logs)
		}
		return true, nil
	})
	if err != nil && strings.Contains(err.Error(), "probe pod failed") {
		return logs, err
	}
	if err != nil {
		partial, _ := c.CoreV1().Pods(ns).GetLogs(gaieProberName, &corev1.PodLogOptions{Container: "prober"}).DoRaw(ctx)
		return string(partial), fmt.Errorf("probe pod %s/%s did not complete within %v%s: %w",
			ns, gaieProberName, timeout, lastAPIErrorSuffix(lastAPIError), err)
	}
	return logs, nil
}
