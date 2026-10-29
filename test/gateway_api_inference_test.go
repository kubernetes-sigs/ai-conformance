package conformance

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
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
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// Weights, request count, tolerance, rounds and consecutive successes follow
// the upstream Gateway API v1.6.3 HTTPRouteWeight conformance test.
const (
	gatewayAPIGatewayName    = "ai-inference-gateway"
	gatewayAPIRouteName      = "ai-inference-route"
	gatewayAPIModelV1        = "model-v1"
	gatewayAPIModelV2        = "model-v2"
	gatewayAPIProbeName      = "gateway-probe"
	gatewayAPIPort           = 8080
	gatewayAPIListenerPort   = 80
	gatewayAPIHeaderName     = "OpenAI-Project"
	gatewayAPIHeaderValue    = "ai-conformance-canary"
	gatewayAPIHeaderRequests = 20
	gatewayAPIWeightA        = 70
	gatewayAPIWeightB        = 30
	gatewayAPIRequests       = 500
	gatewayAPIRounds         = 10
	gatewayAPIConsecutive    = 3
	gatewayAPITolerance      = 0.05
	gatewayAPIPodTimeout     = 3 * time.Minute
	gatewayAPICleanupReserve = 2 * time.Minute
	gatewayAPINotConfigured  = "gateway API test is not configured; networking.ai_inference is a MUST, so install a Gateway API implementation as described in test/README.md or set -gateway-api-class=<class>"
)

var (
	gatewayAPIGroupVersion = schema.GroupVersion{Group: "gateway.networking.k8s.io", Version: "v1"}
	gatewayClassGVR        = gatewayAPIGroupVersion.WithResource("gatewayclasses")
	gatewayGVR             = gatewayAPIGroupVersion.WithResource("gateways")
	httpRouteGVR           = gatewayAPIGroupVersion.WithResource("httproutes")
)

var (
	gatewayAPIClass   *string
	gatewayAPITimeout *time.Duration
)

func init() {
	registerFlagGroup("gateway-api", "Gateway API for AI Inference Flags (TestGatewayAPIInference)")
	gatewayAPIClass = flag.String("gateway-api-class", "",
		"GatewayClass used for the test Gateway. When unset, the first Accepted GatewayClass is used and the test is skipped if there is none.")
	gatewayAPITimeout = flag.Duration("gateway-api-timeout", 10*time.Minute,
		"Total time to wait for the Gateway and HTTPRoute to be programmed and for the data plane to route traffic.")
}

// TestGatewayAPIInference verifies the Gateway API for AI Inference
// requirement: a Gateway API implementation routes inference traffic by HTTP
// header and splits it across backends by weight.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/tree/main/kars/0111-gateway-api-inference
func TestGatewayAPIInference(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if !flag.Parsed() {
		flag.Parse()
	}

	ctx := context.Background()
	clientset := getClientset(t)
	dyn := getDynamicClient(t)

	if err := gatewayAPIServed(clientset.Discovery()); err != nil {
		if *gatewayAPIClass != "" {
			t.Fatalf("ENVIRONMENT ERROR: %v", err)
		}
		t.Skipf("%s: %v", gatewayAPINotConfigured, err)
	}
	class, accepted, err := selectGatewayClass(ctx, dyn, *gatewayAPIClass)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %v", err)
	}
	if class == "" {
		t.Skipf("%s: no Accepted GatewayClass found", gatewayAPINotConfigured)
	}
	if *gatewayAPIClass != "" {
		t.Logf("Using GatewayClass %s from -gateway-api-class", class)
	} else {
		t.Logf("Auto-detected GatewayClass %s, the first by name of the Accepted classes %v; set -gateway-api-class to choose another", class, accepted)
	}
	budget := *gatewayAPITimeout
	if d, ok := t.Deadline(); ok {
		if budget, err = gatewayAPIBudget(budget, time.Until(d)); err != nil {
			t.Fatalf("ENVIRONMENT ERROR: %v", err)
		}
		if budget < *gatewayAPITimeout {
			t.Logf("Capping -gateway-api-timeout at %v to finish within the go test -timeout", budget)
		}
	}

	namespace := randomNamespaceName("gateway-api")
	if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create namespace %s: %v", namespace, err)
	}
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: %v. Please ensure this namespace is terminated manually to avoid resource leaks.", err)
		}
	})

	t.Log("Deploying two inference backends...")
	for _, name := range []string{gatewayAPIModelV1, gatewayAPIModelV2} {
		pod, svc := buildGatewayAPIBackend(namespace, name)
		createTestPod(ctx, t, clientset, pod)
		if _, err := clientset.CoreV1().Services(namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create Service %s: %v", name, err)
		}
	}

	t.Log("Creating the Gateway and HTTPRoute...")
	if _, err := dyn.Resource(gatewayGVR).Namespace(namespace).Create(ctx, buildGateway(namespace, class), metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create Gateway: %v", err)
	}
	if _, err := dyn.Resource(httpRouteGVR).Namespace(namespace).Create(ctx, buildHTTPRoute(namespace), metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create HTTPRoute: %v", err)
	}

	if _, err := waitForPodsRunning(ctx, clientset, namespace, []string{gatewayAPIModelV1, gatewayAPIModelV2}, gatewayAPIPodTimeout); err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Inference backends did not start: %v", err)
	}

	t.Logf("Waiting up to %v for the Gateway to be Programmed, the HTTPRoute Accepted and traffic routed...", budget)
	deadline := time.Now().Add(budget)
	addr, err := waitForGatewayProgrammed(ctx, dyn, namespace, class, time.Until(deadline))
	if err != nil {
		t.Fatalf("FAIL: %v", err)
	}
	if err := waitForHTTPRouteAccepted(ctx, dyn, namespace, time.Until(deadline)); err != nil {
		t.Fatalf("FAIL: %v", err)
	}
	t.Logf("Gateway address: %s", addr)

	t.Log("Sending header-matched and weighted traffic through the Gateway...")
	createTestPod(ctx, t, clientset, buildGatewayAPIProbe(namespace, addr, time.Until(deadline)))
	logs, err := waitForProbeLogs(ctx, clientset, namespace, time.Until(deadline)+gatewayAPIPodTimeout)
	if err != nil {
		t.Fatalf("FAIL: %v", err)
	}
	header, weighted, err := parseGatewayProbeLogs(logs)
	if err != nil {
		t.Fatalf("FAIL: %v. Probe logs:\n%s", err, logs)
	}
	t.Logf("Header-matched requests: %v", header)
	if header[gatewayAPIModelV2] != gatewayAPIHeaderRequests {
		t.Errorf("FAIL: All %d requests with %s: %s should reach %s, got %v",
			gatewayAPIHeaderRequests, gatewayAPIHeaderName, gatewayAPIHeaderValue, gatewayAPIModelV2, header)
	}
	if err := checkWeightedRounds(weighted); err != nil {
		t.Errorf("FAIL: %v", err)
	} else {
		t.Logf("Weighted traffic split %d/%d within %.0f%%: %v", gatewayAPIWeightA, gatewayAPIWeightB, gatewayAPITolerance*100, weighted)
	}
}

// gatewayAPIBudget caps timeout so that the backend and probe Pod waits and
// cleanup still fit in the time left before the go test -timeout.
func gatewayAPIBudget(timeout, left time.Duration) (time.Duration, error) {
	overhead := 2*gatewayAPIPodTimeout + gatewayAPICleanupReserve
	if left <= overhead {
		return 0, fmt.Errorf("the go test -timeout leaves %v, not more than the %v needed for Pods and cleanup; raise -timeout",
			left.Round(time.Second), overhead)
	}
	return min(timeout, (left - overhead).Round(time.Second)), nil
}

func gatewayAPIServed(d discovery.DiscoveryInterface) error {
	list, err := d.ServerResourcesForGroupVersion(gatewayAPIGroupVersion.String())
	if err != nil {
		return fmt.Errorf("%s is not served: %w", gatewayAPIGroupVersion, err)
	}
	served := map[string]bool{}
	for _, r := range list.APIResources {
		served[r.Name] = true
	}
	for _, gvr := range []schema.GroupVersionResource{gatewayClassGVR, gatewayGVR, httpRouteGVR} {
		if !served[gvr.Resource] {
			return fmt.Errorf("%s %s is not served", gatewayAPIGroupVersion, gvr.Resource)
		}
	}
	return nil
}

// selectGatewayClass returns the configured class if it is Accepted, or the
// first Accepted class by name together with all Accepted class names. It
// returns "" when nothing is configured and no class is Accepted.
func selectGatewayClass(ctx context.Context, dyn dynamic.Interface, configured string) (string, []string, error) {
	if configured != "" {
		gc, err := dyn.Resource(gatewayClassGVR).Get(ctx, configured, metav1.GetOptions{})
		if err != nil {
			return "", nil, fmt.Errorf("failed to get GatewayClass %s: %w", configured, err)
		}
		if !conditionTrue(gc.Object, "Accepted", "status", "conditions") {
			return "", nil, fmt.Errorf("GatewayClass %s is not Accepted: %s", configured, statusJSON(gc))
		}
		return configured, nil, nil
	}
	list, err := dyn.Resource(gatewayClassGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", nil, fmt.Errorf("failed to list GatewayClasses: %w", err)
	}
	var accepted []string
	for _, gc := range list.Items {
		if conditionTrue(gc.Object, "Accepted", "status", "conditions") {
			accepted = append(accepted, gc.GetName())
		}
	}
	if len(accepted) == 0 {
		return "", nil, nil
	}
	sort.Strings(accepted)
	return accepted[0], accepted, nil
}

func conditionTrue(obj map[string]any, condType string, path ...string) bool {
	conds, _, _ := unstructured.NestedSlice(obj, path...)
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if ok && m["type"] == condType && m["status"] == "True" {
			return true
		}
	}
	return false
}

func statusJSON(obj *unstructured.Unstructured) string {
	b, _ := json.Marshal(obj.Object["status"])
	return string(b)
}

func gatewayAddress(gw *unstructured.Unstructured) string {
	addrs, _, _ := unstructured.NestedSlice(gw.Object, "status", "addresses")
	for _, a := range addrs {
		m, ok := a.(map[string]any)
		if !ok {
			continue
		}
		// Only these types are reachable by value; others are implementation-specific.
		if typ, _ := m["type"].(string); typ != "" && typ != "IPAddress" && typ != "Hostname" {
			continue
		}
		if v, ok := m["value"].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

func routeAcceptedByGateway(route *unstructured.Unstructured) bool {
	parents, _, _ := unstructured.NestedSlice(route.Object, "status", "parents")
	for _, p := range parents {
		m, ok := p.(map[string]any)
		if !ok {
			continue
		}
		if name, _, _ := unstructured.NestedString(m, "parentRef", "name"); name != gatewayAPIGatewayName {
			continue
		}
		if conditionTrue(m, "Accepted", "conditions") && conditionTrue(m, "ResolvedRefs", "conditions") {
			return true
		}
	}
	return false
}

func waitForGatewayProgrammed(ctx context.Context, dyn dynamic.Interface, ns, class string, timeout time.Duration) (string, error) {
	var addr string
	gw, err := pollGatewayAPIObject(ctx, dyn, gatewayGVR, ns, gatewayAPIGatewayName, timeout, func(o *unstructured.Unstructured) bool {
		addr = gatewayAddress(o)
		return conditionTrue(o.Object, "Accepted", "status", "conditions") &&
			conditionTrue(o.Object, "Programmed", "status", "conditions") && addr != ""
	})
	if err != nil {
		accepted := conditionTrue(gw.Object, "Accepted", "status", "conditions")
		hint := "set -gateway-api-class to a class whose Gateways get an address reachable from Pods"
		if accepted {
			hint = "raise -gateway-api-timeout if the address is still being provisioned, or " + hint
		}
		return "", fmt.Errorf("Gateway %s of GatewayClass %s was not ready within %v (Accepted=%t, Programmed=%t, address=%q): %w; %s", gatewayAPIGatewayName, class, timeout.Round(time.Second),
			accepted, conditionTrue(gw.Object, "Programmed", "status", "conditions"), gatewayAddress(gw), err, hint)
	}
	return addr, nil
}

func waitForHTTPRouteAccepted(ctx context.Context, dyn dynamic.Interface, ns string, timeout time.Duration) error {
	route, err := pollGatewayAPIObject(ctx, dyn, httpRouteGVR, ns, gatewayAPIRouteName, timeout, routeAcceptedByGateway)
	if err != nil {
		return fmt.Errorf("HTTPRoute %s was not Accepted with ResolvedRefs by Gateway %s within %v: %w; status: %s", gatewayAPIRouteName, gatewayAPIGatewayName, timeout.Round(time.Second), err, statusJSON(route))
	}
	return nil
}

// pollGatewayAPIObject returns the last observed object, also on timeout.
func pollGatewayAPIObject(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns, name string, timeout time.Duration, done func(*unstructured.Unstructured) bool) (*unstructured.Unstructured, error) {
	last := &unstructured.Unstructured{Object: map[string]any{}}
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		o, err := dyn.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil && ctx.Err() != nil {
			return false, ctx.Err()
		}
		if isRetryableAPIError(err) || apierrors.IsNotFound(err) {
			lastAPIError = err
			return false, nil
		}
		if err != nil {
			return false, err
		}
		lastAPIError, last = nil, o
		return done(o), nil
	})
	if err != nil {
		return last, fmt.Errorf("%w%s", err, lastAPIErrorSuffix(lastAPIError))
	}
	return last, nil
}

func buildGateway(ns, class string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gatewayAPIGroupVersion.String(),
		"kind":       "Gateway",
		"metadata":   map[string]any{"name": gatewayAPIGatewayName, "namespace": ns},
		"spec": map[string]any{
			"gatewayClassName": class,
			"listeners": []any{map[string]any{
				"name": "http", "protocol": "HTTP", "port": int64(gatewayAPIListenerPort),
			}},
		},
	}}
}

// buildHTTPRoute sends requests carrying the header to backend B and splits
// all other requests across A and B by weight.
func buildHTTPRoute(ns string) *unstructured.Unstructured {
	ref := func(name string, weight int64) map[string]any {
		r := map[string]any{"name": name, "port": int64(gatewayAPIPort)}
		if weight > 0 {
			r["weight"] = weight
		}
		return r
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gatewayAPIGroupVersion.String(),
		"kind":       "HTTPRoute",
		"metadata":   map[string]any{"name": gatewayAPIRouteName, "namespace": ns},
		"spec": map[string]any{
			"parentRefs": []any{map[string]any{"name": gatewayAPIGatewayName}},
			"rules": []any{
				map[string]any{
					"matches": []any{map[string]any{"headers": []any{map[string]any{
						"name": gatewayAPIHeaderName, "value": gatewayAPIHeaderValue,
					}}}},
					"backendRefs": []any{ref(gatewayAPIModelV2, 0)},
				},
				map[string]any{
					"backendRefs": []any{ref(gatewayAPIModelV1, gatewayAPIWeightA), ref(gatewayAPIModelV2, gatewayAPIWeightB)},
				},
			},
		},
	}}
}

// gatewayAPIBackendScript answers every request with the backend's name.
const gatewayAPIBackendScript = `import http.server, os, socket
NAME = os.environ["BACKEND"].encode()
class H(http.server.BaseHTTPRequestHandler):
    def reply(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(NAME)))
        self.end_headers()
        self.wfile.write(NAME)
    do_GET = do_POST = reply
    def log_message(self, *args):
        pass
# Dual-stack when the node has IPv6, so IPv4-only and IPv6-only clusters both work.
class Server(http.server.ThreadingHTTPServer):
    address_family = socket.AF_INET6 if socket.has_dualstack_ipv6() else socket.AF_INET
    def server_bind(self):
        if self.address_family == socket.AF_INET6:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()
Server(("::" if Server.address_family == socket.AF_INET6 else "", int(os.environ["PORT"])), H).serve_forever()
`

// gatewayAPIProbeScript waits until the route is live, then prints the
// backend counts for header-matched requests and for each weighted round.
const gatewayAPIProbeScript = `import os, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
URL, A, B = os.environ["URL"], os.environ["BACKEND_A"], os.environ["BACKEND_B"]
HEADER = {os.environ["HEADER_NAME"]: os.environ["HEADER_VALUE"]}
BODY = b'{"model":"ai-conformance","messages":[{"role":"user","content":"hello"}]}'
def call(extra):
    req = urllib.request.Request(URL, data=BODY, headers={"Content-Type": "application/json", **extra})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.read().decode().strip()
    except Exception as e:
        return "error: %s" % e
def counts(n, extra):
    with ThreadPoolExecutor(10) as ex:
        res = list(ex.map(lambda _: call(extra), range(n)))
    return "%s=%d %s=%d other=%d" % (A, res.count(A), B, res.count(B), n - res.count(A) - res.count(B))
deadline = time.time() + float(os.environ["READY_SECONDS"])
seen, ok = set(), 0
while True:
    h, w = call(HEADER), call({})
    seen.add(w)
    ok = ok + 1 if h == B and w in (A, B) else 0
    if ok >= int(os.environ["CONSECUTIVE"]) and {A, B} <= seen:
        break
    if time.time() > deadline:
        print("NOT_READY after %ss header=%r default=%r seen=%r" % (os.environ["READY_SECONDS"], h, w, sorted(seen)))
        sys.exit(1)
    time.sleep(2)
print("HEADER " + counts(int(os.environ["HEADER_REQUESTS"]), HEADER))
for _ in range(int(os.environ["ROUNDS"])):
    print("WEIGHTED " + counts(int(os.environ["REQUESTS"]), {}))
`

func gatewayAPIPod(ns, name string, labels map[string]string, c corev1.Container) *corev1.Pod {
	c.Image = metricsWorkloadImage
	c.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
	}
	c.SecurityContext = restrictedContainerSecurityContext()
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: restrictedPodSecurityContext(),
			Containers:      []corev1.Container{c},
		},
	}
}

func buildGatewayAPIBackend(ns, name string) (*corev1.Pod, *corev1.Service) {
	labels := map[string]string{"app.kubernetes.io/name": name}
	pod := gatewayAPIPod(ns, name, labels, corev1.Container{
		Name:    "server",
		Command: []string{"python3", "-u", "-c", gatewayAPIBackendScript},
		Env:     []corev1.EnvVar{{Name: "BACKEND", Value: name}, {Name: "PORT", Value: strconv.Itoa(gatewayAPIPort)}},
		Ports:   []corev1.ContainerPort{{Name: "http", ContainerPort: gatewayAPIPort}},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/", Port: intstr.FromString("http")}},
			PeriodSeconds: 2,
		},
	})
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports:    []corev1.ServicePort{{Name: "http", Port: gatewayAPIPort, TargetPort: intstr.FromString("http")}},
		},
	}
	return pod, svc
}

func buildGatewayAPIProbe(ns, addr string, ready time.Duration) *corev1.Pod {
	url := fmt.Sprintf("http://%s/v1/chat/completions", net.JoinHostPort(addr, strconv.Itoa(gatewayAPIListenerPort)))
	return gatewayAPIPod(ns, gatewayAPIProbeName, nil, corev1.Container{
		Name:    "probe",
		Command: []string{"python3", "-u", "-c", gatewayAPIProbeScript},
		Env: []corev1.EnvVar{
			{Name: "URL", Value: url},
			{Name: "BACKEND_A", Value: gatewayAPIModelV1},
			{Name: "BACKEND_B", Value: gatewayAPIModelV2},
			{Name: "HEADER_NAME", Value: gatewayAPIHeaderName},
			{Name: "HEADER_VALUE", Value: gatewayAPIHeaderValue},
			{Name: "HEADER_REQUESTS", Value: strconv.Itoa(gatewayAPIHeaderRequests)},
			{Name: "REQUESTS", Value: strconv.Itoa(gatewayAPIRequests)},
			{Name: "ROUNDS", Value: strconv.Itoa(gatewayAPIRounds)},
			{Name: "CONSECUTIVE", Value: strconv.Itoa(gatewayAPIConsecutive)},
			{Name: "READY_SECONDS", Value: strconv.Itoa(int(ready.Seconds()))},
		},
	})
}

func waitForProbeLogs(ctx context.Context, c kubernetes.Interface, ns string, timeout time.Duration) (string, error) {
	var phase corev1.PodPhase
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 3*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(ns).Get(ctx, gatewayAPIProbeName, metav1.GetOptions{})
		if err != nil && ctx.Err() != nil {
			return false, ctx.Err()
		}
		if isRetryableAPIError(err) {
			lastAPIError = err
			return false, nil
		}
		if err != nil {
			return false, err
		}
		lastAPIError, phase = nil, pod.Status.Phase
		return phase == corev1.PodSucceeded || phase == corev1.PodFailed, nil
	})
	if err != nil {
		partial, _ := c.CoreV1().Pods(ns).GetLogs(gatewayAPIProbeName, &corev1.PodLogOptions{Container: "probe"}).DoRaw(ctx)
		return "", fmt.Errorf("traffic probe Pod did not finish within %v (phase %q): %w%s; probe state: %s. Probe logs:\n%s", timeout.Round(time.Second), phase, err,
			lastAPIErrorSuffix(lastAPIError), containerStatusJSON(ctx, c, ns, gatewayAPIProbeName, "probe"), partial)
	}
	raw, err := c.CoreV1().Pods(ns).GetLogs(gatewayAPIProbeName, &corev1.PodLogOptions{Container: "probe"}).DoRaw(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to read traffic probe logs: %w", err)
	}
	if phase == corev1.PodFailed {
		return "", fmt.Errorf("traffic through the Gateway did not reach the backends as routed. Probe logs:\n%s", raw)
	}
	return string(raw), nil
}

type backendCounts map[string]int

// parseGatewayProbeLogs reads the HEADER line and the WEIGHTED lines printed
// by gatewayAPIProbeScript.
func parseGatewayProbeLogs(logs string) (backendCounts, []backendCounts, error) {
	var header backendCounts
	var weighted []backendCounts
	for _, line := range strings.Split(logs, "\n") {
		kind, rest, _ := strings.Cut(strings.TrimSpace(line), " ")
		if kind != "HEADER" && kind != "WEIGHTED" {
			continue
		}
		counts := backendCounts{}
		for _, f := range strings.Fields(rest) {
			k, v, ok := strings.Cut(f, "=")
			n, err := strconv.Atoi(v)
			if !ok || err != nil {
				return nil, nil, fmt.Errorf("malformed probe line %q", line)
			}
			counts[k] = n
		}
		if kind == "HEADER" {
			header = counts
		} else {
			weighted = append(weighted, counts)
		}
	}
	if header == nil || len(weighted) == 0 {
		return nil, nil, fmt.Errorf("probe logs have no HEADER or WEIGHTED results")
	}
	return header, weighted, nil
}

// checkWeightedRounds passes when any round matches the configured weights
// within the tolerance, like the upstream test's retries.
func checkWeightedRounds(rounds []backendCounts) error {
	var errs []string
	for i, r := range rounds {
		err := checkWeightedSplit(r)
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Sprintf("round %d: %v", i+1, err))
	}
	return fmt.Errorf("weighted traffic did not split %d/%d within %.0f%%: %s", gatewayAPIWeightA, gatewayAPIWeightB, gatewayAPITolerance*100, strings.Join(errs, "; "))
}

func checkWeightedSplit(r backendCounts) error {
	if r["other"] > 0 {
		return fmt.Errorf("%d requests failed or reached no test backend (%v)", r["other"], r)
	}
	total := r[gatewayAPIModelV1] + r[gatewayAPIModelV2]
	if total == 0 {
		return fmt.Errorf("no requests reached a backend")
	}
	got := float64(r[gatewayAPIModelV1]) / float64(total)
	want := float64(gatewayAPIWeightA) / float64(gatewayAPIWeightA+gatewayAPIWeightB)
	if math.Abs(got-want) > gatewayAPITolerance {
		return fmt.Errorf("%s got %.1f%%, want %.0f%% (%v)", gatewayAPIModelV1, got*100, want*100, r)
	}
	return nil
}
