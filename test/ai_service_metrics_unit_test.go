package conformance

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	fakediscovery "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestParsePrometheusService(t *testing.T) {
	tests := []struct {
		in             string
		ns, name, port string
		wantErr        bool
	}{
		{in: "monitoring/prometheus-operated:9090", ns: "monitoring", name: "prometheus-operated", port: "9090"},
		{in: "monitoring/prometheus:web", ns: "monitoring", name: "prometheus", port: "web"},
		{in: "prometheus-operated:9090", wantErr: true},
		{in: "monitoring/prometheus-operated", wantErr: true},
		{in: "monitoring/prometheus-operated:", wantErr: true},
		{in: "/prometheus:9090", wantErr: true},
		{in: "a/b/c:9090", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			ns, name, port, err := parsePrometheusService(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %s/%s:%s", ns, name, port)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ns != tt.ns || name != tt.name || port != tt.port {
				t.Fatalf("got %s/%s:%s, want %s/%s:%s", ns, name, port, tt.ns, tt.name, tt.port)
			}
		})
	}
}

func TestParseKeyValueLabels(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    map[string]string
		wantErr bool
	}{
		{name: "two labels", in: "release=kube-prometheus-stack, team=ml", want: map[string]string{"release": "kube-prometheus-stack", "team": "ml"}},
		{name: "empty", in: "", want: map[string]string{}},
		{name: "missing value separator", in: "release", wantErr: true},
		{name: "empty key", in: "=x", wantErr: true},
		{name: "empty pair", in: "a=b,,c=d", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseKeyValueLabels(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseSingleSample(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		want      float64
		wantFound bool
		wantErr   string
	}{
		{
			name:      "single sample",
			body:      `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1727870000.123,"10"]}]}}`,
			want:      10,
			wantFound: true,
		},
		{
			name: "empty result before first scrape",
			body: `{"status":"success","data":{"resultType":"vector","result":[]}}`,
		},
		{
			name:    "query error",
			body:    `{"status":"error","errorType":"bad_data","error":"parse error"}`,
			wantErr: "bad_data",
		},
		{
			name:    "matrix result",
			body:    `{"status":"success","data":{"resultType":"matrix","result":[]}}`,
			wantErr: "instant vector",
		},
		{
			name:    "more than one sample",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1"]},{"value":[1,"2"]}]}}`,
			wantErr: "at most one sample",
		},
		{
			name:    "non-numeric value",
			body:    `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"abc"]}]}}`,
			wantErr: "parsing sample value",
		},
		{
			name:    "not JSON",
			body:    `<html>502 Bad Gateway</html>`,
			wantErr: "decoding",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, found, err := parseSingleSample([]byte(tt.body))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("got err %v, want error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if found != tt.wantFound || v != tt.want {
				t.Fatalf("got (%v, %v), want (%v, %v)", v, found, tt.want, tt.wantFound)
			}
		})
	}
}

func testScrapeData() scrapeTemplateData {
	return scrapeTemplateData{
		Namespace: "ai-service-metrics-abcde", ServiceName: metricsWorkloadName, PortName: metricsPortName,
		Port: metricsPort, AppLabelKey: metricsAppLabelKey, AppLabelValue: metricsWorkloadName, RunID: "run123",
		ScrapeInterval: metricsScrapeInterval.String(),
	}
}

func TestRenderScrapeManifest(t *testing.T) {
	t.Run("multi-document template", func(t *testing.T) {
		tmpl := `---
apiVersion: monitoring.googleapis.com/v1
kind: PodMonitoring
metadata:
  name: {{.ServiceName}}
spec:
  selector:
    matchLabels:
      {{.AppLabelKey}}: {{.AppLabelValue}}
  endpoints:
  - port: {{.PortName}}
    interval: {{.ScrapeInterval}}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: scrape-{{.RunID}}
  namespace: other-ns
`
		objs, err := renderScrapeManifest(tmpl, testScrapeData())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(objs) != 2 {
			t.Fatalf("got %d objects, want 2", len(objs))
		}
		if objs[0].GetKind() != "PodMonitoring" || objs[0].GetName() != metricsWorkloadName {
			t.Errorf("first object: got %s %s", objs[0].GetKind(), objs[0].GetName())
		}
		if ep := objs[0].Object["spec"].(map[string]any)["endpoints"].([]any)[0].(map[string]any); ep["interval"] != "15s" {
			t.Errorf("interval: got %v, want 15s", ep["interval"])
		}
		if objs[0].GetNamespace() != "" {
			t.Errorf("namespace should be left empty for the test to default, got %q", objs[0].GetNamespace())
		}
		if objs[1].GetName() != "scrape-run123" || objs[1].GetNamespace() != "other-ns" {
			t.Errorf("second object: got %s/%s", objs[1].GetNamespace(), objs[1].GetName())
		}
	})

	for _, tt := range []struct {
		name    string
		tmpl    string
		wantErr string
	}{
		{name: "unknown field", tmpl: "metadata:\n  name: {{.Nope}}\n", wantErr: "executing template"},
		{name: "missing kind", tmpl: "apiVersion: v1\nmetadata:\n  name: x\n", wantErr: "apiVersion, kind"},
		{name: "empty manifest", tmpl: "---\n", wantErr: "no objects"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := renderScrapeManifest(tt.tmpl, testScrapeData())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestBuildServiceMonitor(t *testing.T) {
	d := testScrapeData()
	sm := buildServiceMonitor(d, map[string]string{"release": "kube-prometheus-stack"})

	if sm.GetAPIVersion() != "monitoring.coreos.com/v1" || sm.GetKind() != "ServiceMonitor" {
		t.Fatalf("got %s %s", sm.GetAPIVersion(), sm.GetKind())
	}
	if sm.GetNamespace() != d.Namespace || sm.GetName() != d.ServiceName {
		t.Errorf("got %s/%s", sm.GetNamespace(), sm.GetName())
	}
	if sm.GetLabels()["release"] != "kube-prometheus-stack" {
		t.Errorf("selection label missing: %v", sm.GetLabels())
	}
	spec := sm.Object["spec"].(map[string]any)
	match := spec["selector"].(map[string]any)["matchLabels"].(map[string]any)
	if match[metricsAppLabelKey] != metricsWorkloadName {
		t.Errorf("selector does not match the workload Service: %v", match)
	}
	ep := spec["endpoints"].([]any)[0].(map[string]any)
	if ep["port"] != metricsPortName || ep["path"] != "/metrics" || ep["interval"] != "15s" {
		t.Errorf("unexpected endpoint %v", ep)
	}
}

func TestBuildMetricsWorkload(t *testing.T) {
	pod, svc := buildMetricsWorkload("ns", "run123")

	// The Service must select the Pod, or nothing gets scraped.
	for k, v := range svc.Spec.Selector {
		if pod.Labels[k] != v {
			t.Errorf("Service selector %s=%s does not match Pod labels %v", k, v, pod.Labels)
		}
	}
	c := pod.Spec.Containers[0]
	if c.ReadinessProbe.HTTPGet.Path != "/healthz" {
		t.Errorf("readiness probe must use the uncounted /healthz path, got %s", c.ReadinessProbe.HTTPGet.Path)
	}
	if want := metricsPodDeadlineSeconds(*metricsCollectionTimeout); pod.Spec.ActiveDeadlineSeconds == nil || *pod.Spec.ActiveDeadlineSeconds != want {
		t.Errorf("active deadline seconds = %v, want %d", pod.Spec.ActiveDeadlineSeconds, want)
	}
	wantEnv := []corev1.EnvVar{{Name: "RUN_ID", Value: "run123"}, {Name: "PORT", Value: strconv.Itoa(metricsPort)}}
	if !reflect.DeepEqual(c.Env, wantEnv) {
		t.Errorf("env = %v, want %v", c.Env, wantEnv)
	}
	// The stub must emit exactly the metric names the verification queries.
	for _, name := range []string{metricRequestsTotal, metricLatency + "_count", metricQueueDepth, metricsRunLabel} {
		if !strings.Contains(metricsStubScript, name) {
			t.Errorf("stub script does not emit %s", name)
		}
	}
}

func TestMetricExpectations(t *testing.T) {
	checks := metricExpectations("run123")
	if len(checks) != 3 {
		t.Fatalf("got %d checks, want 3", len(checks))
	}
	for _, c := range checks {
		if !strings.HasPrefix(c.query, "max(") || !strings.Contains(c.query, `conformance_run="run123"`) {
			t.Errorf("query %q must use max() and select the run ID", c.query)
		}
	}
	if !checks[0].counter || !checks[1].counter || checks[2].counter {
		t.Errorf("request and histogram counts must be counters and the gauge must not, got %+v", checks)
	}
}

// scriptedQuerier answers each query with the next scripted value, repeating
// the last one; "" is an empty result.
type scriptedQuerier struct {
	script map[string][]string
	calls  map[string]int
}

func newScriptedQuerier(runID string, series ...[]string) *scriptedQuerier {
	q := &scriptedQuerier{script: map[string][]string{}, calls: map[string]int{}}
	for i, c := range metricExpectations(runID) {
		q.script[c.query] = series[i]
	}
	return q
}

func (q *scriptedQuerier) Query(_ context.Context, promql string) ([]byte, error) {
	vals := q.script[promql]
	v := vals[min(q.calls[promql], len(vals)-1)]
	q.calls[promql]++
	if v == "" {
		return []byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`), nil
	}
	return []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"` + v + `"]}]}}`), nil
}

func setFastMetricsPolling(t *testing.T) kubernetes.Interface {
	t.Helper()
	old := metricsPollInterval
	metricsPollInterval = time.Millisecond
	t.Cleanup(func() { metricsPollInterval = old })
	setMetricsFlags(t, metricsFlagValues{timeout: 200 * time.Millisecond})
	return fake.NewClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: metricsWorkloadName, Namespace: "ns"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	})
}

func TestWaitForBaselines(t *testing.T) {
	tests := []struct {
		name          string
		series        [][]string
		wantBaselines []float64
		wantErr       string
	}{
		{
			name:          "series appear after a few scrapes",
			series:        [][]string{{"", "", "3"}, {"", "3"}, {"", "0"}},
			wantBaselines: []float64{3, 3, 0},
		},
		{
			name:    "gauge never collected",
			series:  [][]string{{"0"}, {"0"}, {""}},
			wantErr: "queue depth gauge: not collected yet",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := setFastMetricsPolling(t)
			got, _, err := waitForBaselines(context.Background(), client, newScriptedQuerier("run123", tt.series...), "ns", "run123")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) || !strings.Contains(err.Error(), "raise -service-metrics-collection-timeout; also check that the query endpoint and any tenant header") {
					t.Fatalf("got %v, want timeout error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.wantBaselines) {
				t.Fatalf("baselines = %v, want %v", got, tt.wantBaselines)
			}
		})
	}
}

func TestWaitForIncrease(t *testing.T) {
	tests := []struct {
		name      string
		baselines []float64
		series    [][]string
		wantErr   string
		wantFast  bool
	}{
		{
			name:      "counters stored relative to the first sample",
			baselines: []float64{0, 0, 0},
			series:    [][]string{{"0", "10"}, {"0", "0", "10"}, {"0"}},
		},
		{
			name:      "nonzero baseline",
			baselines: []float64{3, 3, 0},
			series:    [][]string{{"3", "13"}, {"13"}, {"0"}},
		},
		{
			name:      "dip below baseline then recovery",
			baselines: []float64{3, 3, 0},
			series:    [][]string{{"3", "1", "13"}, {"13"}, {"0"}},
		},
		{
			name:      "extra requests",
			baselines: []float64{3, 3, 0},
			series:    [][]string{{"14"}, {"13"}, {"0"}},
			wantErr:   "request count: got 14, want 13 (baseline 3 + 10); extra requests",
			wantFast:  true,
		},
		{
			name:      "partial increase",
			baselines: []float64{3, 3, 0},
			series:    [][]string{{"8"}, {"13"}, {"0"}},
			wantErr:   "request count: got 8, want 13 (baseline 3 + 10)",
		},
		{
			name:      "counter reset",
			baselines: []float64{3, 3, 0},
			series:    [][]string{{"1"}, {"13"}, {"0"}},
			wantErr:   "below baseline 3; the counter was reset",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := setFastMetricsPolling(t)
			_, err := waitForIncrease(context.Background(), client, newScriptedQuerier("run123", tt.series...), "ns", "run123", tt.baselines, 10)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tt.wantErr)
			}
			if hinted := strings.Contains(err.Error(), "raise -service-metrics-collection-timeout"); hinted == tt.wantFast {
				t.Errorf("timeout hint present = %v, want %v: %v", hinted, !tt.wantFast, err)
			}
			if strings.Contains(err.Error(), "tenant header") {
				t.Errorf("endpoint hint present although every series was collected: %v", err)
			}
		})
	}
}

func TestHTTPQuerier(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path != "/prom/api/v1/query":
			http.Error(w, `{"status":"error","error":"wrong path"}`, http.StatusNotFound)
		case r.Header.Get("Authorization") != "Bearer s3cret":
			http.Error(w, `{"status":"error","error":"unauthorized"}`, http.StatusUnauthorized)
		case r.Header.Get("X-Scope-OrgID") != "tenant-a":
			http.Error(w, "no org id", http.StatusUnauthorized)
		case r.URL.Query().Get("query") != "vector(1)":
			http.Error(w, `{"status":"error","error":"wrong query"}`, http.StatusBadRequest)
		default:
			_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}]}}`))
		}
	}))
	defer srv.Close()
	dir := t.TempDir()
	writeFile := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	caFile := writeFile("ca.pem", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})))
	badCA := writeFile("bad.pem", "not a certificate")
	token := writeFile("token", "s3cret\n")
	wrongToken := writeFile("wrong-token", "wrong")
	const tenant = "X-Scope-OrgID=tenant-a"

	for _, tt := range []struct {
		name, tokenFile, headers, caFile string
		wantErr                          string
	}{
		{name: "token, header and ca", tokenFile: token, headers: tenant, caFile: caFile},
		{name: "token file wins over Authorization header", tokenFile: token, headers: tenant + ",Authorization=Bearer wrong", caFile: caFile},
		{name: "wrong token", tokenFile: wrongToken, headers: tenant, caFile: caFile, wantErr: "HTTP 401"},
		{name: "no tenant header", tokenFile: token, caFile: caFile, wantErr: "HTTP 401: no org id"},
		{name: "no ca", tokenFile: token, headers: tenant, wantErr: "certificate"},
		{name: "non-PEM ca", tokenFile: token, headers: tenant, caFile: badCA, wantErr: "no PEM certificates"},
		{name: "missing token file", tokenFile: filepath.Join(dir, "missing"), headers: tenant, caFile: caFile, wantErr: "bearer-token-file"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			setMetricsFlags(t, metricsFlagValues{url: srv.URL + "/prom/", tokenFile: tt.tokenFile, headers: tt.headers, caFile: tt.caFile, timeout: time.Minute})
			q, err := newPromQuerier(nil)
			if err == nil {
				err = preflightQuery(context.Background(), q)
			}
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestPermanentQueryError(t *testing.T) {
	gr := schema.GroupResource{Resource: "services"}
	for _, tt := range []struct {
		err  error
		want bool
	}{
		{&queryHTTPError{code: 400}, true},
		{&queryHTTPError{code: 401}, true},
		{&queryHTTPError{code: 403}, true},
		{&queryHTTPError{code: 404}, true},
		{&queryHTTPError{code: 422}, true},
		{apierrors.NewGenericServerResponse(403, "get", gr, "prometheus", "", 0, true), true},
		{&queryHTTPError{code: 408}, false},
		{&queryHTTPError{code: 429}, false},
		{&queryHTTPError{code: 500}, false},
		{&queryHTTPError{code: 503}, false},
		{apierrors.NewServiceUnavailable("proxy"), false},
		{apierrors.NewTooManyRequests("slow down", 1), false},
		{errors.New("connection refused"), false},
		{context.DeadlineExceeded, false},
	} {
		if got := permanentQueryError(tt.err); got != tt.want {
			t.Errorf("permanentQueryError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

type errQuerier struct {
	err   error
	calls int
}

func (q *errQuerier) Query(context.Context, string) ([]byte, error) {
	q.calls++
	return nil, q.err
}

func TestWaitForBaselinesQueryErrors(t *testing.T) {
	t.Run("client error fails fast", func(t *testing.T) {
		client := setFastMetricsPolling(t)
		q := &errQuerier{err: &queryHTTPError{code: 401, body: "unauthorized"}}
		_, _, err := waitForBaselines(context.Background(), client, q, "ns", "run")
		if err == nil || err.Error() != "request count: query API returned HTTP 401: unauthorized" || !permanentQueryError(err) || wait.Interrupted(err) || q.calls != 1 {
			t.Fatalf("got %v after %d queries, want an immediate HTTP 401 failure", err, q.calls)
		}
	})
	t.Run("server error is retried", func(t *testing.T) {
		client := setFastMetricsPolling(t)
		q := &errQuerier{err: &queryHTTPError{code: 503, body: "unavailable"}}
		_, _, err := waitForBaselines(context.Background(), client, q, "ns", "run")
		if !wait.Interrupted(err) || q.calls <= len(metricExpectations("run")) {
			t.Fatalf("got %v after %d queries, want a timeout after retries", err, q.calls)
		}
	})
}

func TestDeployMetricsWorkload(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	deployMetricsWorkload(ctx, t, client, "ns", "run123")

	pod, err := client.CoreV1().Pods("ns").Get(ctx, metricsWorkloadName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Pod not created: %v", err)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("got RestartPolicy %s, want Never so a restart cannot silently reset counters", pod.Spec.RestartPolicy)
	}
	if _, err := client.CoreV1().Services("ns").Get(ctx, metricsWorkloadName, metav1.GetOptions{}); err != nil {
		t.Fatalf("Service not created: %v", err)
	}
}

func TestServiceMonitorAvailable(t *testing.T) {
	client := fake.NewClientset()
	if serviceMonitorAvailable(client) {
		t.Fatal("got available without the monitoring.coreos.com/v1 API")
	}
	client.Discovery().(*fakediscovery.FakeDiscovery).Resources = []*metav1.APIResourceList{{
		GroupVersion: serviceMonitorGVR.GroupVersion().String(),
		APIResources: []metav1.APIResource{{Name: "podmonitors"}, {Name: "servicemonitors"}},
	}}
	if !serviceMonitorAvailable(client) {
		t.Fatal("got unavailable with servicemonitors served")
	}
}

func TestCheckMetricsWorkloadIntact(t *testing.T) {
	podWith := func(phase corev1.PodPhase, restarts int32) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: metricsWorkloadName, Namespace: "ns"},
			Status: corev1.PodStatus{
				Phase:             phase,
				ContainerStatuses: []corev1.ContainerStatus{{Name: "server", RestartCount: restarts}},
			},
		}
	}
	tests := []struct {
		name    string
		pod     *corev1.Pod
		wantErr string
	}{
		{name: "running", pod: podWith(corev1.PodRunning, 0)},
		{name: "failed", pod: podWith(corev1.PodFailed, 0), wantErr: "counters were lost"},
		{name: "restarted", pod: podWith(corev1.PodRunning, 1), wantErr: "counters were reset"},
		{name: "deleted", wantErr: "getting metrics workload Pod"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewClientset()
			if tt.pod != nil {
				client = fake.NewClientset(tt.pod)
			}
			err := checkMetricsWorkloadIntact(context.Background(), client, "ns")
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

type metricsFlagValues struct {
	promSvc, url, tokenFile, headers, caFile, manifest, labels string
	timeout                                                    time.Duration
}

func setMetricsFlags(t *testing.T, v metricsFlagValues) {
	t.Helper()
	strs := []*string{metricsPrometheusService, metricsQueryURL, metricsQueryBearerTokenFile, metricsQueryHeaders, metricsQueryCAFile, metricsScrapeManifest, metricsScrapeLabels}
	oldStrs := make([]string, len(strs))
	for i, p := range strs {
		oldStrs[i] = *p
	}
	oldTimeout := *metricsCollectionTimeout
	t.Cleanup(func() {
		for i, p := range strs {
			*p = oldStrs[i]
		}
		*metricsCollectionTimeout = oldTimeout
	})
	for i, s := range []string{v.promSvc, v.url, v.tokenFile, v.headers, v.caFile, v.manifest, v.labels} {
		*strs[i] = s
	}
	*metricsCollectionTimeout = v.timeout
}

func TestValidateMetricsFlags(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	const u = "https://prom.example"
	tests := []struct {
		name    string
		flags   metricsFlagValues
		wantErr string
	}{
		{name: "defaults", flags: metricsFlagValues{timeout: 5 * time.Minute}},
		{name: "service", flags: metricsFlagValues{promSvc: "monitoring/prometheus-operated:9090", labels: "release=kps", timeout: time.Minute}},
		{name: "url with token", flags: metricsFlagValues{url: u, tokenFile: "/token", timeout: time.Minute}},
		{name: "url with headers and ca", flags: metricsFlagValues{url: u, headers: "X-Scope-OrgID=tenant-a", caFile: caFile, timeout: time.Minute}},
		{name: "both endpoints", flags: metricsFlagValues{promSvc: "m/p:9090", url: u, timeout: time.Minute}, wantErr: "mutually exclusive"},
		{name: "token without url", flags: metricsFlagValues{promSvc: "m/p:9090", tokenFile: "/token", timeout: time.Minute}, wantErr: "bearer-token-file requires -service-metrics-query-url"},
		{name: "token only", flags: metricsFlagValues{tokenFile: "/token", timeout: time.Minute}, wantErr: "requires -service-metrics-query-url"},
		{name: "headers without url", flags: metricsFlagValues{promSvc: "m/p:9090", headers: "X-Scope-OrgID=a", timeout: time.Minute}, wantErr: "headers requires -service-metrics-query-url"},
		{name: "ca without url", flags: metricsFlagValues{promSvc: "m/p:9090", caFile: caFile, timeout: time.Minute}, wantErr: "ca-file requires -service-metrics-query-url"},
		{name: "bad headers", flags: metricsFlagValues{url: u, headers: "X-Scope-OrgID", timeout: time.Minute}, wantErr: "query-headers"},
		{name: "missing ca file", flags: metricsFlagValues{url: u, caFile: "does-not-exist.pem", timeout: time.Minute}, wantErr: "query-ca-file"},
		{name: "zero timeout", flags: metricsFlagValues{promSvc: "m/p:9090"}, wantErr: "collection-timeout"},
		{name: "missing manifest", flags: metricsFlagValues{promSvc: "m/p:9090", manifest: "does-not-exist.yaml", timeout: time.Minute}, wantErr: "scrape-manifest"},
		{name: "bad labels", flags: metricsFlagValues{promSvc: "m/p:9090", labels: "release", timeout: time.Minute}, wantErr: "scrape-labels"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setMetricsFlags(t, tt.flags)
			err := validateMetricsFlags()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestApplyScrapeConfigServiceMonitor(t *testing.T) {
	setMetricsFlags(t, metricsFlagValues{promSvc: "m/p:9090", labels: "release=kube-prometheus-stack", timeout: time.Minute})
	client := fake.NewClientset()
	client.Discovery().(*fakediscovery.FakeDiscovery).Resources = []*metav1.APIResourceList{{
		GroupVersion: serviceMonitorGVR.GroupVersion().String(),
		APIResources: []metav1.APIResource{{Name: "servicemonitors", Kind: "ServiceMonitor", Namespaced: true}},
	}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{serviceMonitorGVR: "ServiceMonitorList"})
	d := testScrapeData()
	ctx := context.Background()

	t.Run("apply", func(t *testing.T) {
		applyScrapeConfig(ctx, t, client, dyn, d)
		sm, err := dyn.Resource(serviceMonitorGVR).Namespace(d.Namespace).Get(ctx, d.ServiceName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("ServiceMonitor not created: %v", err)
		}
		if sm.GetLabels()["release"] != "kube-prometheus-stack" {
			t.Errorf("selection label missing: %v", sm.GetLabels())
		}
	})
	if _, err := dyn.Resource(serviceMonitorGVR).Namespace(d.Namespace).Get(ctx, d.ServiceName, metav1.GetOptions{}); err == nil {
		t.Fatal("ServiceMonitor was not deleted on cleanup")
	}
}

func TestApplyScrapeConfigManifest(t *testing.T) {
	manifest := filepath.Join(t.TempDir(), "scrape.yaml")
	tmpl := `apiVersion: monitoring.googleapis.com/v1
kind: PodMonitoring
metadata:
  name: {{.ServiceName}}
spec:
  selector:
    matchLabels:
      {{.AppLabelKey}}: {{.AppLabelValue}}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: scrape-{{.RunID}}
  namespace: other-ns
`
	if err := os.WriteFile(manifest, []byte(tmpl), 0o600); err != nil {
		t.Fatal(err)
	}
	setMetricsFlags(t, metricsFlagValues{promSvc: "m/p:9090", manifest: manifest, timeout: time.Minute})

	podMonitoringGVR := schema.GroupVersionResource{Group: "monitoring.googleapis.com", Version: "v1", Resource: "podmonitorings"}
	configMapGVR := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	client := fake.NewClientset()
	client.Discovery().(*fakediscovery.FakeDiscovery).Resources = []*metav1.APIResourceList{
		{GroupVersion: "monitoring.googleapis.com/v1", APIResources: []metav1.APIResource{{Name: "podmonitorings", Kind: "PodMonitoring", Namespaced: true}}},
		{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "configmaps", Kind: "ConfigMap", Namespaced: true}}},
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{podMonitoringGVR: "PodMonitoringList", configMapGVR: "ConfigMapList"})
	d := testScrapeData()
	ctx := context.Background()

	t.Run("apply", func(t *testing.T) {
		applyScrapeConfig(ctx, t, client, dyn, d)
		if _, err := dyn.Resource(podMonitoringGVR).Namespace(d.Namespace).Get(ctx, d.ServiceName, metav1.GetOptions{}); err != nil {
			t.Errorf("PodMonitoring not created in the test namespace: %v", err)
		}
		if _, err := dyn.Resource(configMapGVR).Namespace("other-ns").Get(ctx, "scrape-run123", metav1.GetOptions{}); err != nil {
			t.Errorf("ConfigMap not created in its own namespace: %v", err)
		}
	})
	if _, err := dyn.Resource(podMonitoringGVR).Namespace(d.Namespace).Get(ctx, d.ServiceName, metav1.GetOptions{}); err == nil {
		t.Error("PodMonitoring was not deleted on cleanup")
	}
	if _, err := dyn.Resource(configMapGVR).Namespace("other-ns").Get(ctx, "scrape-run123", metav1.GetOptions{}); err == nil {
		t.Error("ConfigMap was not deleted on cleanup")
	}
}
