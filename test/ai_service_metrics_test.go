package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
)

// Metric names are generic so they don't favor one inference framework.
const (
	metricsWorkloadName      = "ai-metrics-stub"
	metricsWorkloadImage     = "python:3.13-alpine"
	metricsAppLabelKey       = "app.kubernetes.io/name"
	metricsPortName          = "http"
	metricsPort              = 8080
	metricsRunLabel          = "conformance_run"
	metricsRequestCount      = 10
	metricRequestsTotal      = "ai_conformance_inference_requests_total"
	metricLatency            = "ai_conformance_inference_request_duration_seconds"
	metricQueueDepth         = "ai_conformance_inference_queue_depth"
	metricsPollInterval      = 10 * time.Second
	metricsReadyTimeout      = 3 * time.Minute
	metricsPodDeadlineBuffer = 15 * time.Minute
)

var serviceMonitorGVR = schema.GroupVersionResource{Group: "monitoring.coreos.com", Version: "v1", Resource: "servicemonitors"}

var (
	metricsPrometheusService    *string
	metricsQueryURL             *string
	metricsQueryBearerTokenFile *string
	metricsScrapeManifest       *string
	metricsScrapeLabels         *string
	metricsCollectionTimeout    *time.Duration
)

func init() {
	registerFlagGroup("service-metrics", "AI Service Metrics Flags (TestAIServiceMetrics)")
	metricsPrometheusService = flag.String("service-metrics-prometheus-service", "",
		"In-cluster Prometheus-compatible query Service as <namespace>/<name>:<port> (e.g. monitoring/prometheus-operated:9090), reached through the API server service proxy. Mutually exclusive with -service-metrics-query-url. The test is skipped when neither is set.")
	metricsQueryURL = flag.String("service-metrics-query-url", "",
		"Base URL of a Prometheus-compatible HTTP query API (the test appends /api/v1/query), for monitoring systems not reachable as an in-cluster Service. Mutually exclusive with -service-metrics-prometheus-service.")
	metricsQueryBearerTokenFile = flag.String("service-metrics-query-bearer-token-file", "",
		"Optional file containing a bearer token sent with requests to -service-metrics-query-url.")
	metricsScrapeManifest = flag.String("service-metrics-scrape-manifest", "",
		"Optional path to a Go-templated YAML manifest (one or more documents) that configures the platform's monitoring system to scrape the test workload. Available fields: {{.Namespace}}, {{.ServiceName}}, {{.PortName}}, {{.Port}}, {{.AppLabelKey}}, {{.AppLabelValue}}, {{.RunID}}. When unset, a monitoring.coreos.com/v1 ServiceMonitor is created.")
	metricsScrapeLabels = flag.String("service-metrics-scrape-labels", "",
		"Comma-separated key=value labels added to the default ServiceMonitor so the platform's Prometheus selects it (e.g. release=kube-prometheus-stack).")
	metricsCollectionTimeout = flag.Duration("service-metrics-collection-timeout", 5*time.Minute,
		"How long to wait for the monitoring system to collect the workload's metrics after traffic is sent.")
}

// TestAIServiceMetrics verifies the AI Job & Inference Service Metrics
// requirement: the platform's monitoring system discovers a workload exposing
// Prometheus exposition format metrics and collects them.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/tree/main/kars/0061-ai-job-inference-service-metrics
func TestAIServiceMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if !flag.Parsed() {
		flag.Parse()
	}
	if err := validateMetricsFlags(); err != nil {
		t.Fatalf("Invalid flags: %v", err)
	}
	if *metricsPrometheusService == "" && *metricsQueryURL == "" {
		t.Skip("AI service metrics test is not configured; set -service-metrics-prometheus-service=<namespace>/<name>:<port> or -service-metrics-query-url=<url> to point the test at the platform's monitoring query API")
	}

	clientset := getClientset(t)
	querier, err := newPromQuerier(clientset)
	if err != nil {
		t.Fatalf("Failed to configure the metrics query client: %v", err)
	}

	ctx := context.Background()
	namespace := randomNamespaceName("ai-service-metrics")
	runID := rand.String(10)

	// Fail fast on a misconfigured query endpoint before creating anything.
	if err := preflightQuery(ctx, querier); err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Metrics query API preflight failed (check -service-metrics-prometheus-service / -service-metrics-query-url and credentials): %v", err)
	}

	if _, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create namespace %s: %v", namespace, err)
	}
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: %v. Please ensure this namespace is terminated manually to avoid resource leaks.", err)
		}
	})

	t.Logf("Deploying metrics workload %s/%s (run ID %s)...", namespace, metricsWorkloadName, runID)
	deployMetricsWorkload(ctx, t, clientset, namespace, runID)

	t.Log("Configuring the monitoring system to scrape the workload...")
	applyScrapeConfig(ctx, t, clientset, getDynamicClient(t), scrapeTemplateData{
		Namespace:     namespace,
		ServiceName:   metricsWorkloadName,
		PortName:      metricsPortName,
		Port:          metricsPort,
		AppLabelKey:   metricsAppLabelKey,
		AppLabelValue: metricsWorkloadName,
		RunID:         runID,
	})

	waitForMetricsWorkloadReady(ctx, t, clientset, namespace)

	t.Logf("Sending %d inference requests...", metricsRequestCount)
	sendInferenceTraffic(ctx, t, clientset, namespace, metricsRequestCount)

	t.Logf("Waiting up to %v for the monitoring system to collect the workload's metrics...", *metricsCollectionTimeout)
	verifyCollectedMetrics(ctx, t, clientset, querier, namespace, runID, metricsRequestCount)
}

func validateMetricsFlags() error {
	if *metricsPrometheusService != "" && *metricsQueryURL != "" {
		return errors.New("-service-metrics-prometheus-service and -service-metrics-query-url are mutually exclusive")
	}
	if *metricsQueryBearerTokenFile != "" && *metricsQueryURL == "" {
		return errors.New("-service-metrics-query-bearer-token-file requires -service-metrics-query-url")
	}
	if *metricsCollectionTimeout <= 0 {
		return fmt.Errorf("-service-metrics-collection-timeout must be positive, got %v", *metricsCollectionTimeout)
	}
	if *metricsScrapeManifest != "" {
		if _, err := os.Stat(*metricsScrapeManifest); err != nil {
			return fmt.Errorf("-service-metrics-scrape-manifest: %w", err)
		}
	}
	if _, err := parseKeyValueLabels(*metricsScrapeLabels); err != nil {
		return fmt.Errorf("-service-metrics-scrape-labels: %w", err)
	}
	return nil
}

// metricsStubScript is a dependency-free inference stand-in. /healthz is not
// counted, so probes don't inflate the request count.
const metricsStubScript = `
import json, os, socket, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RUN = os.environ["RUN_ID"]
PORT = int(os.environ["PORT"])
BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
lock = threading.Lock()
state = {"count": 0, "sum": 0.0, "buckets": [0] * len(BUCKETS), "inflight": 0}

def render():
    lbl = 'conformance_run="%s"' % RUN
    with lock:
        count, total, inflight = state["count"], state["sum"], state["inflight"]
        buckets = list(state["buckets"])
    out = [
        "# HELP ai_conformance_inference_requests_total Total inference requests served.",
        "# TYPE ai_conformance_inference_requests_total counter",
        "ai_conformance_inference_requests_total{%s} %d" % (lbl, count),
        "# HELP ai_conformance_inference_request_duration_seconds Inference request latency.",
        "# TYPE ai_conformance_inference_request_duration_seconds histogram",
    ]
    for le, n in zip(BUCKETS, buckets):
        out.append('ai_conformance_inference_request_duration_seconds_bucket{%s,le="%s"} %d' % (lbl, le, n))
    out += [
        'ai_conformance_inference_request_duration_seconds_bucket{%s,le="+Inf"} %d' % (lbl, count),
        "ai_conformance_inference_request_duration_seconds_sum{%s} %f" % (lbl, total),
        "ai_conformance_inference_request_duration_seconds_count{%s} %d" % (lbl, count),
        "# HELP ai_conformance_inference_queue_depth Requests currently queued or in flight.",
        "# TYPE ai_conformance_inference_queue_depth gauge",
        "ai_conformance_inference_queue_depth{%s} %d" % (lbl, inflight),
    ]
    return "\n".join(out) + "\n"

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send(self, code, body, ctype):
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/healthz":
            return self.send(200, "ok", "text/plain")
        if self.path == "/metrics":
            return self.send(200, render(), "text/plain; version=0.0.4; charset=utf-8")
        if self.path.startswith("/v1/infer"):
            return self.infer()
        self.send(404, "not found", "text/plain")

    def infer(self):
        start = time.monotonic()
        with lock:
            state["inflight"] += 1
        time.sleep(0.01)
        elapsed = time.monotonic() - start
        with lock:
            state["inflight"] -= 1
            state["count"] += 1
            state["sum"] += elapsed
            for i, le in enumerate(BUCKETS):
                if elapsed <= le:
                    state["buckets"][i] += 1
        self.send(200, json.dumps({"output": "ok"}), "application/json")

# Dual-stack when the node has IPv6, so IPv4-only and IPv6-only clusters both work.
class Server(ThreadingHTTPServer):
    address_family = socket.AF_INET6 if socket.has_dualstack_ipv6() else socket.AF_INET

    def server_bind(self):
        if self.address_family == socket.AF_INET6:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()

Server(("::" if Server.address_family == socket.AF_INET6 else "", PORT), Handler).serve_forever()
`

func buildMetricsWorkload(ns, runID string) (*corev1.Pod, *corev1.Service) {
	labels := map[string]string{metricsAppLabelKey: metricsWorkloadName}
	nonRoot := true
	uid := int64(65534)
	noEscalation := false
	deadline := metricsPodDeadlineSeconds(*metricsCollectionTimeout)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: metricsWorkloadName, Namespace: ns, Labels: labels},
		Spec: corev1.PodSpec{
			// A restart resets the counters and invalidates the run, so fail instead.
			RestartPolicy:         corev1.RestartPolicyNever,
			ActiveDeadlineSeconds: &deadline,
			// Restricted Pod Security Standard.
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:   &nonRoot,
				RunAsUser:      &uid,
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:    "server",
				Image:   metricsWorkloadImage,
				Command: []string{"python3", "-u", "-c", metricsStubScript},
				Env:     []corev1.EnvVar{{Name: "RUN_ID", Value: runID}, {Name: "PORT", Value: strconv.Itoa(metricsPort)}},
				Ports:   []corev1.ContainerPort{{Name: metricsPortName, ContainerPort: metricsPort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: "/healthz", Port: intstr.FromString(metricsPortName),
					}},
					PeriodSeconds: 2,
				},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("50m"),
						corev1.ResourceMemory: resource.MustParse("64Mi"),
					},
					Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: &noEscalation,
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
			}},
		},
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: metricsWorkloadName, Namespace: ns, Labels: labels},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name: metricsPortName, Port: metricsPort, TargetPort: intstr.FromString(metricsPortName),
			}},
		},
	}
	return pod, svc
}

// metricsPodDeadlineSeconds bounds the workload's lifetime if cleanup fails.
func metricsPodDeadlineSeconds(collection time.Duration) int64 {
	lifetime := metricsReadyTimeout + time.Minute + collection + metricsPodDeadlineBuffer
	return int64((lifetime + time.Second - 1) / time.Second)
}

func deployMetricsWorkload(ctx context.Context, t *testing.T, c kubernetes.Interface, ns, runID string) {
	t.Helper()
	pod, svc := buildMetricsWorkload(ns, runID)
	createTestPod(ctx, t, c, pod)
	if _, err := c.CoreV1().Services(ns).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create metrics workload Service: %v", err)
	}
}

// waitForMetricsWorkloadReady waits for the Pod and for /healthz to answer
// through the Service.
func waitForMetricsWorkloadReady(ctx context.Context, t *testing.T, c kubernetes.Interface, ns string) {
	t.Helper()
	var lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, metricsReadyTimeout, true, func(ctx context.Context) (bool, error) {
		pod, err := c.CoreV1().Pods(ns).Get(ctx, metricsWorkloadName, metav1.GetOptions{})
		if err != nil {
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			return false, err
		}
		lastAPIError = nil
		return podIsReady(pod), nil
	})
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Metrics workload Pod did not become Ready within %v%s: %v; container state: %s",
			metricsReadyTimeout, lastAPIErrorSuffix(lastAPIError), err, containerStatusJSON(ctx, c, ns, metricsWorkloadName, "server"))
	}

	var lastErr error
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		_, lastErr = c.CoreV1().Services(ns).ProxyGet("http", metricsWorkloadName, metricsPortName, "/healthz", nil).DoRaw(ctx)
		return lastErr == nil, nil
	})
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Metrics workload Service did not answer /healthz through the API server proxy: %v (last error: %v)", err, lastErr)
	}
}

// sendInferenceTraffic sends exactly n requests. Any failure aborts the test
// because the expected count would be ambiguous.
func sendInferenceTraffic(ctx context.Context, t *testing.T, c kubernetes.Interface, ns string, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, err := c.CoreV1().Services(ns).ProxyGet("http", metricsWorkloadName, metricsPortName, "/v1/infer", nil).DoRaw(ctx); err != nil {
			t.Fatalf("Inference request %d/%d failed; aborting because the expected request count is now ambiguous: %v", i, n, err)
		}
	}
}

type scrapeTemplateData struct {
	Namespace     string
	ServiceName   string
	PortName      string
	Port          int
	AppLabelKey   string
	AppLabelValue string
	RunID         string
}

func applyScrapeConfig(ctx context.Context, t *testing.T, c kubernetes.Interface, dyn dynamic.Interface, data scrapeTemplateData) {
	t.Helper()

	var objs []*unstructured.Unstructured
	if *metricsScrapeManifest != "" {
		raw, err := os.ReadFile(*metricsScrapeManifest)
		if err != nil {
			t.Fatalf("Failed to read -service-metrics-scrape-manifest: %v", err)
		}
		if objs, err = renderScrapeManifest(string(raw), data); err != nil {
			t.Fatalf("Failed to render -service-metrics-scrape-manifest: %v", err)
		}
	} else {
		if !serviceMonitorAvailable(c) {
			t.Fatalf("ENVIRONMENT ERROR: The %s API is not served by this cluster. If the platform's monitoring system uses a different discovery mechanism, provide it with -service-metrics-scrape-manifest", serviceMonitorGVR.GroupVersion())
		}
		labels, _ := parseKeyValueLabels(*metricsScrapeLabels) // validated in validateMetricsFlags
		objs = []*unstructured.Unstructured{buildServiceMonitor(data, labels)}
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(c.Discovery()))
	for _, obj := range objs {
		gvk := obj.GroupVersionKind()
		mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			t.Fatalf("ENVIRONMENT ERROR: Scrape configuration kind %s is not served by this cluster: %v", gvk, err)
		}
		var ri dynamic.ResourceInterface = dyn.Resource(mapping.Resource)
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			if obj.GetNamespace() == "" {
				obj.SetNamespace(data.Namespace)
			}
			ri = dyn.Resource(mapping.Resource).Namespace(obj.GetNamespace())
		}
		created, err := ri.Create(ctx, obj, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create scrape configuration %s %s: %v", gvk.Kind, obj.GetName(), err)
		}
		t.Logf("Created scrape configuration %s %s", gvk.Kind, describeObject(created))

		// Namespace deletion doesn't remove objects outside the test namespace.
		t.Cleanup(func() {
			err := ri.Delete(context.Background(), created.GetName(), metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				t.Errorf("CLEANUP FAILURE: failed to delete scrape configuration %s %s: %v", gvk.Kind, describeObject(created), err)
			}
		})
	}
}

func describeObject(o *unstructured.Unstructured) string {
	if o.GetNamespace() == "" {
		return o.GetName()
	}
	return o.GetNamespace() + "/" + o.GetName()
}

func serviceMonitorAvailable(c kubernetes.Interface) bool {
	list, err := c.Discovery().ServerResourcesForGroupVersion(serviceMonitorGVR.GroupVersion().String())
	if err != nil {
		return false
	}
	for _, r := range list.APIResources {
		if r.Name == serviceMonitorGVR.Resource {
			return true
		}
	}
	return false
}

func buildServiceMonitor(d scrapeTemplateData, labels map[string]string) *unstructured.Unstructured {
	sm := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"selector": map[string]any{
				"matchLabels": map[string]any{d.AppLabelKey: d.AppLabelValue},
			},
			"namespaceSelector": map[string]any{
				"matchNames": []any{d.Namespace},
			},
			"endpoints": []any{map[string]any{
				"port":     d.PortName,
				"path":     "/metrics",
				"interval": "15s",
			}},
		},
	}}
	sm.SetAPIVersion(serviceMonitorGVR.GroupVersion().String())
	sm.SetKind("ServiceMonitor")
	sm.SetName(d.ServiceName)
	sm.SetNamespace(d.Namespace)
	if len(labels) > 0 {
		sm.SetLabels(labels)
	}
	return sm
}

// renderScrapeManifest renders the template and decodes each YAML document.
func renderScrapeManifest(tmpl string, data scrapeTemplateData) ([]*unstructured.Unstructured, error) {
	tp, err := template.New("scrape-manifest").Option("missingkey=error").Parse(tmpl)
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}
	var buf bytes.Buffer
	if err := tp.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("executing template: %w", err)
	}

	var objs []*unstructured.Unstructured
	dec := k8syaml.NewYAMLOrJSONDecoder(&buf, 4096)
	for {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decoding YAML: %w", err)
		}
		if len(m) == 0 {
			continue // empty document, e.g. a leading "---"
		}
		obj := &unstructured.Unstructured{Object: m}
		if obj.GetAPIVersion() == "" || obj.GetKind() == "" || obj.GetName() == "" {
			return nil, fmt.Errorf("every document needs apiVersion, kind and metadata.name; got %v", m)
		}
		objs = append(objs, obj)
	}
	if len(objs) == 0 {
		return nil, errors.New("manifest contains no objects")
	}
	return objs, nil
}

// promQuerier runs an instant query against a Prometheus /api/v1/query API.
type promQuerier interface {
	Query(ctx context.Context, promql string) ([]byte, error)
}

type serviceProxyQuerier struct {
	client                kubernetes.Interface
	namespace, name, port string
}

func (q serviceProxyQuerier) Query(ctx context.Context, promql string) ([]byte, error) {
	return q.client.CoreV1().Services(q.namespace).
		ProxyGet("http", q.name, q.port, "/api/v1/query", map[string]string{"query": promql}).
		DoRaw(ctx)
}

type httpQuerier struct {
	baseURL     string
	bearerToken string
	client      *http.Client
}

func (q httpQuerier) Query(ctx context.Context, promql string) ([]byte, error) {
	u := strings.TrimSuffix(q.baseURL, "/") + "/api/v1/query?" + url.Values{"query": {promql}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if q.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+q.bearerToken)
	}
	resp, err := q.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	// Prometheus returns 4xx/5xx with a JSON error body; surface both.
	if resp.StatusCode != http.StatusOK {
		return body, fmt.Errorf("query API returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func newPromQuerier(c kubernetes.Interface) (promQuerier, error) {
	if *metricsQueryURL != "" {
		q := httpQuerier{baseURL: *metricsQueryURL, client: &http.Client{Timeout: 30 * time.Second}}
		if *metricsQueryBearerTokenFile != "" {
			tok, err := os.ReadFile(*metricsQueryBearerTokenFile)
			if err != nil {
				return nil, fmt.Errorf("reading -service-metrics-query-bearer-token-file: %w", err)
			}
			q.bearerToken = strings.TrimSpace(string(tok))
		}
		return q, nil
	}
	ns, name, port, err := parsePrometheusService(*metricsPrometheusService)
	if err != nil {
		return nil, fmt.Errorf("-service-metrics-prometheus-service: %w", err)
	}
	return serviceProxyQuerier{client: c, namespace: ns, name: name, port: port}, nil
}

// parsePrometheusService parses "<namespace>/<name>:<port>"; the port may be
// a number or a Service port name.
func parsePrometheusService(s string) (namespace, name, port string, err error) {
	nsName, port, ok := strings.Cut(s, ":")
	if !ok || port == "" {
		return "", "", "", fmt.Errorf("%q: expected <namespace>/<name>:<port>", s)
	}
	namespace, name, ok = strings.Cut(nsName, "/")
	if !ok || namespace == "" || name == "" || strings.Contains(name, "/") {
		return "", "", "", fmt.Errorf("%q: expected <namespace>/<name>:<port>", s)
	}
	return namespace, name, port, nil
}

// parseKeyValueLabels parses "k1=v1,k2=v2". An empty string yields no labels.
func parseKeyValueLabels(s string) (map[string]string, error) {
	labels := map[string]string{}
	if strings.TrimSpace(s) == "" {
		return labels, nil
	}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid label %q: expected key=value", pair)
		}
		labels[k] = v
	}
	return labels, nil
}

type promQueryResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// parseSingleSample returns found=false while the result is empty, e.g.
// before the first scrape.
func parseSingleSample(body []byte) (value float64, found bool, err error) {
	var r promQueryResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return 0, false, fmt.Errorf("decoding query response: %w", err)
	}
	if r.Status != "success" {
		return 0, false, fmt.Errorf("query failed: %s: %s", r.ErrorType, r.Error)
	}
	if r.Data.ResultType != "vector" {
		return 0, false, fmt.Errorf("expected an instant vector result, got %q", r.Data.ResultType)
	}
	switch len(r.Data.Result) {
	case 0:
		return 0, false, nil
	case 1:
	default:
		return 0, false, fmt.Errorf("expected at most one sample, got %d", len(r.Data.Result))
	}
	v := r.Data.Result[0].Value
	if len(v) != 2 {
		return 0, false, fmt.Errorf("malformed sample %v", v)
	}
	s, ok := v[1].(string)
	if !ok {
		return 0, false, fmt.Errorf("malformed sample value %v", v[1])
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parsing sample value %q: %w", s, err)
	}
	return f, true, nil
}

func preflightQuery(ctx context.Context, q promQuerier) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	body, err := q.Query(ctx, "vector(1)")
	if err != nil {
		return err
	}
	v, found, err := parseSingleSample(body)
	if err != nil {
		return err
	}
	if !found || v != 1 {
		return fmt.Errorf("vector(1) returned found=%v value=%v, want 1", found, v)
	}
	return nil
}

type metricExpectation struct {
	description string
	query       string
	want        float64
	// exact requires value == want; otherwise any sample passes.
	exact bool
}

// metricExpectations uses max() rather than sum() so a target discovered
// twice isn't double-counted.
func metricExpectations(runID string, requests int) []metricExpectation {
	sel := fmt.Sprintf(`{%s=%q}`, metricsRunLabel, runID)
	return []metricExpectation{
		{"request count", "max(" + metricRequestsTotal + sel + ")", float64(requests), true},
		{"request latency histogram count", "max(" + metricLatency + "_count" + sel + ")", float64(requests), true},
		{"queue depth gauge", "max(" + metricQueueDepth + sel + ")", 0, false},
	}
}

func verifyCollectedMetrics(ctx context.Context, t *testing.T, client kubernetes.Interface, q promQuerier, ns, runID string, requests int) {
	t.Helper()
	checks := metricExpectations(runID, requests)
	status := make([]string, len(checks))

	err := wait.PollUntilContextTimeout(ctx, metricsPollInterval, *metricsCollectionTimeout, true, func(ctx context.Context) (bool, error) {
		if err := checkMetricsWorkloadIntact(ctx, client, ns); err != nil {
			return false, err
		}
		done := true
		for i, c := range checks {
			body, err := q.Query(ctx, c.query)
			if err != nil {
				if ctx.Err() != nil {
					// Keep the previous status; a deadline error says nothing about collection.
					return false, nil
				}
				status[i] = fmt.Sprintf("%s: query error: %v", c.description, err)
				done = false
				continue
			}
			v, found, err := parseSingleSample(body)
			switch {
			case err != nil:
				status[i] = fmt.Sprintf("%s: %v", c.description, err)
				done = false
			case !found:
				status[i] = fmt.Sprintf("%s: not collected yet (%s)", c.description, c.query)
				done = false
			case c.exact && v > c.want:
				// Counters only grow, so waiting longer cannot fix this.
				return false, fmt.Errorf("%s: got %v, want %v; extra requests reached the workload", c.description, v, c.want)
			case c.exact && v != c.want:
				status[i] = fmt.Sprintf("%s: got %v, want %v", c.description, v, c.want)
				done = false
			default:
				status[i] = fmt.Sprintf("%s: ok (%v)", c.description, v)
			}
		}
		return done, nil
	})
	if err != nil {
		t.Fatalf("FAIL: Workload metrics were not collected as expected (timeout %v): %v\n  %s\n  container state: %s",
			*metricsCollectionTimeout, err, strings.Join(status, "\n  "),
			containerStatusJSON(ctx, client, ns, metricsWorkloadName, "server"))
	}
	for _, s := range status {
		t.Logf("  %s", s)
	}
	t.Logf("The monitoring system collected the workload's request count, latency and queue depth metrics")
}

// checkMetricsWorkloadIntact fails if the Pod stops or restarts, which resets
// its counters.
func checkMetricsWorkloadIntact(ctx context.Context, c kubernetes.Interface, ns string) error {
	pod, err := c.CoreV1().Pods(ns).Get(ctx, metricsWorkloadName, metav1.GetOptions{})
	if err != nil {
		if isRetryableAPIError(err) || ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("getting metrics workload Pod: %w", err)
	}
	if pod.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("metrics workload Pod is %s; its counters were lost", pod.Status.Phase)
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.RestartCount > 0 {
			return fmt.Errorf("metrics workload container %s restarted %d time(s); its counters were reset", cs.Name, cs.RestartCount)
		}
	}
	return nil
}
