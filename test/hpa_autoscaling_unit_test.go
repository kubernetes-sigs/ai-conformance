package conformance

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

func hpaPtr[T any](v T) *T { return &v }

// fastHPATiming shortens the scenario's timing for unit tests. The grace stays
// well above one poll, which can take tens of milliseconds under -race.
func fastHPATiming(t *testing.T) {
	t.Helper()
	poll, window, cleanup, grace := hpaPollInterval, hpaNegativeControlWindow, hpaCleanupTimeout, hpaObservationGrace
	hpaPollInterval, hpaNegativeControlWindow, hpaCleanupTimeout, hpaObservationGrace = 2*time.Millisecond, 150*time.Millisecond, time.Second, 250*time.Millisecond
	t.Cleanup(func() {
		hpaPollInterval, hpaNegativeControlWindow, hpaCleanupTimeout, hpaObservationGrace = poll, window, cleanup, grace
	})
}

func testHPAConfig(t *testing.T, timeout time.Duration) hpaConfig {
	t.Helper()
	hc, err := newHPAConfig("m", 2, "", "example.com/python:test", timeout, timeout, timeout, 0)
	if err != nil {
		t.Fatalf("newHPAConfig: %v", err)
	}
	return hc
}

// testHPAScenario returns a device-plugin scenario with one-second timeouts.
func testHPAScenario(t *testing.T, c kubernetes.Interface) *hpaScenario {
	t.Helper()
	return newHPAScenario(c, "ns", allocationModeDevicePlugin, "v1beta2", nvidiaConfig(t), testHPAConfig(t, time.Second), t.Logf)
}

// wantHPAError requires err to be nil if want is empty, and otherwise to
// contain want.
func wantHPAError(t *testing.T, err error, want string) {
	t.Helper()
	switch {
	case want == "" && err != nil:
		t.Fatalf("unexpected error: %v", err)
	case want != "" && (err == nil || !strings.Contains(err.Error(), want)):
		t.Fatalf("error = %v, want it to contain %q", err, want)
	}
}

func TestNewHPAConfig(t *testing.T) {
	type args struct {
		metric        string
		maxReplicas   int
		annotations   string
		image         string
		metricTimeout time.Duration
		scaleUp       time.Duration
		scaleDown     time.Duration
		stabilization time.Duration
	}
	valid := args{"m", 2, "", "example.com/python:test", 5 * time.Minute, 10 * time.Minute, 10 * time.Minute, time.Minute}
	tests := []struct {
		name    string
		mutate  func(*args)
		wantErr string
	}{
		{name: "valid defaults", mutate: func(*args) {}},
		{name: "zero stabilization", mutate: func(a *args) { a.stabilization = 0 }},
		{name: "empty metric name", mutate: func(a *args) { a.metric = "" }, wantErr: "hpa-custom-metric-name"},
		{name: "empty image", mutate: func(a *args) { a.image = "" }, wantErr: "hpa-workload-image"},
		{name: "max replicas below 2", mutate: func(a *args) { a.maxReplicas = 1 }, wantErr: "between 2 and 100"},
		{name: "max replicas above 100", mutate: func(a *args) { a.maxReplicas = 101 }, wantErr: "between 2 and 100"},
		{name: "zero metric timeout", mutate: func(a *args) { a.metricTimeout = 0 }, wantErr: "greater than zero"},
		{name: "zero scale-up timeout", mutate: func(a *args) { a.scaleUp = 0 }, wantErr: "greater than zero"},
		{name: "zero scale-down timeout", mutate: func(a *args) { a.scaleDown = 0 }, wantErr: "greater than zero"},
		{name: "negative stabilization", mutate: func(a *args) { a.stabilization = -time.Second }, wantErr: "whole number of seconds"},
		{name: "fractional stabilization", mutate: func(a *args) { a.stabilization = 1500 * time.Millisecond }, wantErr: "whole number of seconds"},
		{name: "stabilization above API limit", mutate: func(a *args) { a.stabilization, a.scaleDown = 61*time.Minute, 2*time.Hour }, wantErr: "between 0 and 1h"},
		{name: "stabilization not shorter than timeout", mutate: func(a *args) { a.stabilization = a.scaleDown }, wantErr: "shorter than"},
		{name: "malformed annotation", mutate: func(a *args) { a.annotations = "prometheus.io/scrape" }, wantErr: "expected key=value"},
		{name: "invalid annotation key", mutate: func(a *args) { a.annotations = "bad key=x" }, wantErr: "invalid annotation key"},
		{name: "duplicate annotation key", mutate: func(a *args) { a.annotations = "a=1,a=2" }, wantErr: "duplicate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := valid
			tt.mutate(&a)
			_, err := newHPAConfig(a.metric, a.maxReplicas, a.annotations, a.image, a.metricTimeout, a.scaleUp, a.scaleDown, a.stabilization)
			wantHPAError(t, err, tt.wantErr)
		})
	}
}

func TestParseHPAPodAnnotations(t *testing.T) {
	got, err := parseHPAPodAnnotations(" prometheus.io/scrape=true, prometheus.io/port=8080 ,empty= ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "8080", "empty": ""}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, err := parseHPAPodAnnotations(""); err != nil || len(got) != 0 {
		t.Fatalf("parseHPAPodAnnotations(\"\") = %v, %v; want empty map", got, err)
	}
}

func TestBuildHPADeployment(t *testing.T) {
	cfg := nvidiaConfig(t)
	annotations := map[string]string{"prometheus.io/scrape": "true"}

	for _, mode := range []string{allocationModeDevicePlugin, allocationModeDRA} {
		t.Run(mode, func(t *testing.T) {
			d, err := buildHPADeployment("ns", mode, cfg, "example.com/python:test", annotations)
			if err != nil {
				t.Fatalf("buildHPADeployment: %v", err)
			}
			if hpaReplicaCount(d.Spec.Replicas) != hpaMinReplicas {
				t.Errorf("replicas = %d, want %d", hpaReplicaCount(d.Spec.Replicas), hpaMinReplicas)
			}
			tmpl := d.Spec.Template
			for k, v := range d.Spec.Selector.MatchLabels {
				if tmpl.Labels[k] != v {
					t.Errorf("template label %s=%q does not match selector value %q", k, tmpl.Labels[k], v)
				}
			}
			if tmpl.Labels[hpaMetricSourceLabel] != "true" || tmpl.Annotations["prometheus.io/scrape"] != "true" {
				t.Errorf("template labels=%v annotations=%v, want the discovery label and configured annotation", tmpl.Labels, tmpl.Annotations)
			}
			annotations["mutated"] = "x"
			if _, ok := tmpl.Annotations["mutated"]; ok {
				t.Errorf("template annotations alias the caller's map")
			}
			delete(annotations, "mutated")

			spec := tmpl.Spec
			if len(spec.Containers) != 1 {
				t.Fatalf("got %d containers, want 1", len(spec.Containers))
			}
			c := spec.Containers[0]
			if c.Image != "example.com/python:test" {
				t.Errorf("image = %q, want the configured image", c.Image)
			}
			if c.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
				t.Errorf("terminationMessagePolicy = %q, want FallbackToLogsOnError so startup failures are diagnosable", c.TerminationMessagePolicy)
			}
			if len(c.Ports) != 1 || c.Ports[0].Name != hpaMetricsPortName || c.Ports[0].ContainerPort != hpaMetricsPort {
				t.Errorf("ports = %+v, want %s:%d", c.Ports, hpaMetricsPortName, hpaMetricsPort)
			}
			if c.ReadinessProbe == nil || c.ReadinessProbe.HTTPGet == nil || c.ReadinessProbe.HTTPGet.Path != "/healthz" ||
				c.ReadinessProbe.HTTPGet.Port.StrVal != hpaMetricsPortName {
				t.Errorf("readiness probe = %+v, want HTTP GET /healthz on port %s", c.ReadinessProbe, hpaMetricsPortName)
			}
			if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].MountPath != hpaMetricMountPath || c.VolumeMounts[0].SubPath != "" || c.VolumeMounts[0].SubPathExpr != "" {
				t.Errorf("volume mounts = %+v, want a directory mount at %s without subPath", c.VolumeMounts, hpaMetricMountPath)
			}
			var cmVolume *corev1.ConfigMapVolumeSource
			for _, v := range spec.Volumes {
				if v.Name == c.VolumeMounts[0].Name {
					cmVolume = v.ConfigMap
				}
			}
			if cmVolume == nil || cmVolume.Name != hpaWorkloadName || len(cmVolume.Items) != 0 {
				t.Errorf("metric volume = %+v, want ConfigMap %s with all keys projected", cmVolume, hpaWorkloadName)
			}

			extended := corev1.ResourceName(cfg.ExtendedResource)
			limit, hasLimit := c.Resources.Limits[extended]
			switch mode {
			case allocationModeDevicePlugin:
				if limit.Value() != requestedAcceleratorCount || len(spec.ResourceClaims) != 0 {
					t.Errorf("limits=%v claims=%v, want %s=1 and no claims", c.Resources.Limits, spec.ResourceClaims, extended)
				}
			case allocationModeDRA:
				if hasLimit || len(spec.ResourceClaims) != 1 || spec.ResourceClaims[0].ResourceClaimTemplateName == nil || *spec.ResourceClaims[0].ResourceClaimTemplateName != testResourceTemplateName ||
					len(c.Resources.Claims) != 1 || c.Resources.Claims[0].Name != spec.ResourceClaims[0].Name {
					t.Errorf("limits=%v podClaims=%+v containerClaims=%+v, want one template claim used by the container", c.Resources.Limits, spec.ResourceClaims, c.Resources.Claims)
				}
			}
			if !podRequestsAccelerator(&corev1.Pod{Spec: spec}, mode, cfg) {
				t.Errorf("podRequestsAccelerator = false for the workload template")
			}
		})
	}

	if _, err := buildHPADeployment("ns", "bogus", cfg, "example.com/python:test", nil); err == nil {
		t.Fatal("buildHPADeployment accepted an unknown allocation mode")
	}
}

func TestHPAWorkloadImageDefault(t *testing.T) {
	// Runtimes resolve unqualified names differently (CRI-O can reject
	// ambiguous short names), so the default names its registry.
	def := flag.Lookup("hpa-workload-image").DefValue
	if host, _, ok := strings.Cut(def, "/"); !ok || !strings.Contains(host, ".") {
		t.Fatalf("default -hpa-workload-image %q is not fully qualified", def)
	}
}

func TestBuildHPA(t *testing.T) {
	hc, err := newHPAConfig("m", 3, "", "example.com/python:test", time.Minute, time.Minute, 5*time.Minute, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	hpa := buildHPA("ns", hc)
	ref := hpa.Spec.ScaleTargetRef
	if ref.APIVersion != "apps/v1" || ref.Kind != "Deployment" || ref.Name != hpaWorkloadName {
		t.Errorf("scaleTargetRef = %+v, want apps/v1 Deployment %s", ref, hpaWorkloadName)
	}
	if hpaReplicaCount(hpa.Spec.MinReplicas) != hpaMinReplicas || hpa.Spec.MaxReplicas != 3 {
		t.Errorf("min/max = %d/%d, want %d/3", hpaReplicaCount(hpa.Spec.MinReplicas), hpa.Spec.MaxReplicas, hpaMinReplicas)
	}
	if len(hpa.Spec.Metrics) != 1 {
		t.Fatalf("got %d metrics, want 1", len(hpa.Spec.Metrics))
	}
	m := hpa.Spec.Metrics[0]
	if m.Type != autoscalingv2.PodsMetricSourceType || m.Pods == nil || m.Pods.Metric.Name != "m" {
		t.Fatalf("metric = %+v, want Pods metric \"m\"", m)
	}
	target := m.Pods.Target
	if target.Type != autoscalingv2.AverageValueMetricType || target.AverageValue == nil || target.AverageValue.Cmp(resource.MustParse("10")) != 0 {
		t.Errorf("target = %+v, want AverageValue 10", target)
	}
	b := hpa.Spec.Behavior
	if b == nil || *b.ScaleDown.StabilizationWindowSeconds != 90 || *b.ScaleUp.StabilizationWindowSeconds != 0 {
		t.Errorf("behavior = %+v, want scale-down window 90s and scale-up window 0s", b)
	}
	if hpaBaselineValue >= hpaTargetValue || hpaScaleDownValue >= hpaTargetValue || hpaScaleUpValue <= hpaTargetValue ||
		hpaBaselineValue == hpaScaleDownValue {
		t.Errorf("phase values must straddle the target and be distinct")
	}
}

func TestHPAWorkloadCommand(t *testing.T) {
	cfg := nvidiaConfig(t)
	bin := t.TempDir()
	// Stand-in for python3 so the test only exercises the startup gate.
	if err := os.WriteFile(filepath.Join(bin, "python3"), []byte("#!/bin/sh\necho SERVER_STARTED\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		devices   int
		wantStart bool
	}{{0, false}, {1, true}, {2, false}} {
		t.Run(fmt.Sprintf("%d devices", tc.devices), func(t *testing.T) {
			dev := t.TempDir()
			for i := 0; i < tc.devices; i++ {
				if err := os.WriteFile(filepath.Join(dev, fmt.Sprintf("nvidia%d", i)), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg.DevicePattern = filepath.Join(dev, "nvidia[0-9]*")
			cmd := exec.Command("/bin/sh", "-c", hpaWorkloadCommand(cfg))
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"))
			out, err := cmd.CombinedOutput()
			started := strings.Contains(string(out), "SERVER_STARTED")
			if started != tc.wantStart || (err == nil) != tc.wantStart {
				t.Fatalf("started=%t err=%v, want started=%t; output:\n%s", started, err, tc.wantStart, out)
			}
		})
	}
}

// TestHPAMetricServer runs the real workload server: the metric follows the
// mounted file, and readiness fails whenever the file cannot be served.
func TestHPAMetricServer(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed; the metric server is exercised only by the cluster test")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	dir := t.TempDir()
	valueFile := filepath.Join(dir, hpaMetricValueKey)
	write := func(content string) {
		t.Helper()
		// Replace atomically, as the kubelet does for ConfigMap volumes.
		tmp := filepath.Join(dir, "next")
		if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, valueFile); err != nil {
			t.Fatal(err)
		}
	}
	write("1")
	cmd := exec.Command(python, "-c", hpaMetricServer)
	cmd.Env = append(os.Environ(), "HPA_METRIC_FILE="+valueFile, "HPA_METRIC_SERIES="+hpaMetricSeries, fmt.Sprintf("HPA_METRIC_PORT=%d", port))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	get := func(path string) (int, string, string) {
		t.Helper()
		var resp *http.Response
		var err error
		for i := 0; i < 50; i++ {
			if resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, path)); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, resp.Header.Get("Content-Type"), string(body)
	}

	if code, _, _ := get("/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d with a valid value, want 200", code)
	}
	code, ctype, body := get(hpaMetricsPath)
	if code != http.StatusOK || !strings.HasPrefix(ctype, "text/plain; version=0.0.4") ||
		!logsContainExactLine(body, hpaMetricSeries+" 1.0") || !strings.Contains(body, "# TYPE "+hpaMetricSeries+" gauge") {
		t.Fatalf("%s = %d %q:\n%s", hpaMetricsPath, code, ctype, body)
	}
	write("100\n")
	if _, _, body := get(hpaMetricsPath); !logsContainExactLine(body, hpaMetricSeries+" 100.0") {
		t.Fatalf("metric did not follow the updated value:\n%s", body)
	}

	// Readiness must track whether a valid sample can be served.
	for _, bad := range []string{"not-a-number", "", "nan", "inf"} {
		write(bad)
		if code, _, _ := get("/healthz"); code != http.StatusServiceUnavailable {
			t.Errorf("/healthz = %d with value %q, want 503", code, bad)
		}
		if code, _, _ := get(hpaMetricsPath); code != http.StatusServiceUnavailable {
			t.Errorf("%s = %d with value %q, want 503 so no bogus sample is scraped", hpaMetricsPath, code, bad)
		}
	}
	if err := os.Remove(valueFile); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := get("/healthz"); code != http.StatusServiceUnavailable {
		t.Errorf("/healthz = %d with the value file missing, want 503", code)
	}
	write("2")
	if code, _, _ := get("/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d after the value recovered, want 200", code)
	}
	if code, _, _ := get("/other"); code != http.StatusNotFound {
		t.Errorf("/other = %d, want 404", code)
	}
}

func TestSetHPAMetricValue(t *testing.T) {
	ctx := context.Background()

	t.Run("updates the value and retries conflicts", func(t *testing.T) {
		client := fake.NewClientset(buildHPAMetricConfigMap("ns", hpaBaselineValue))
		var attempts int32
		client.PrependReactor("update", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			if atomic.AddInt32(&attempts, 1) == 1 {
				return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, hpaWorkloadName, errors.New("stale"))
			}
			return false, nil, nil
		})
		if err := setHPAMetricValue(ctx, client, "ns", hpaScaleUpValue); err != nil {
			t.Fatalf("setHPAMetricValue: %v", err)
		}
		cm, err := client.CoreV1().ConfigMaps("ns").Get(ctx, hpaWorkloadName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if cm.Data[hpaMetricValueKey] != "100" || attempts < 2 {
			t.Fatalf("value=%q after %d attempts, want 100 after a retried conflict", cm.Data[hpaMetricValueKey], attempts)
		}
	})

	t.Run("missing ConfigMap fails", func(t *testing.T) {
		if err := setHPAMetricValue(ctx, fake.NewClientset(), "ns", 1); !apierrors.IsNotFound(err) {
			t.Fatalf("error = %v, want NotFound", err)
		}
	})

	// retry.RetryOnConflict alone would return nil here (see hpaRetry).
	t.Run("update cut off by the deadline fails", func(t *testing.T) {
		c := hpaUnresponsiveServer(t)
		deadline, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if err := setHPAMetricValue(deadline, c, "ns", 1); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want the context deadline", err)
		}
	})
}

func hpaMetricItem(kind, ns, pod, metric, value string, v1beta1 bool) string {
	name := fmt.Sprintf(`"metric":{"name":%q}`, metric)
	if v1beta1 {
		name = fmt.Sprintf(`"metricName":%q`, metric)
	}
	return fmt.Sprintf(`{"describedObject":{"kind":%q,"namespace":%q,"name":%q,"apiVersion":"/v1"},%s,"timestamp":"2026-10-15T00:00:00Z","value":%q}`,
		kind, ns, pod, name, value)
}

func hpaMetricList(items ...string) []byte {
	return []byte(`{"kind":"MetricValueList","items":[` + strings.Join(items, ",") + `]}`)
}

func TestCheckHPAMetricSamples(t *testing.T) {
	const ns, metric = "ns", "m"
	pod := func(name, value string) string { return hpaMetricItem("Pod", ns, name, metric, value, false) }
	tests := []struct {
		name    string
		body    []byte
		wantErr string
	}{
		{name: "v1beta2 current samples", body: hpaMetricList(pod("a", "100"), pod("b", "100"))},
		{name: "v1beta1 current samples", body: hpaMetricList(hpaMetricItem("Pod", ns, "a", metric, "100", true), hpaMetricItem("Pod", ns, "b", metric, "100", true))},
		{name: "milli quantities compare by value", body: hpaMetricList(pod("a", "100000m"), pod("b", "100"))},
		{name: "samples for other Pods are ignored", body: hpaMetricList(pod("a", "100"), pod("b", "100"), pod("terminating", "1"))},
		{name: "identical duplicates are accepted", body: hpaMetricList(pod("a", "100"), pod("a", "100"), pod("b", "100"))},
		{name: "missing Pod", body: hpaMetricList(pod("a", "100")), wantErr: "no sample for Pod b"},
		{name: "empty list", body: hpaMetricList(), wantErr: "no sample for Pod a"},
		{name: "stale value from previous phase", body: hpaMetricList(pod("a", "100"), pod("b", "1")), wantErr: "Pod b reports 1, want 100"},
		{name: "conflicting duplicates", body: hpaMetricList(pod("a", "100"), pod("a", "1"), pod("b", "100")), wantErr: "conflicting values"},
		{name: "wrong metric name", body: hpaMetricList(hpaMetricItem("Pod", ns, "a", "other", "100", false), pod("b", "100")), wantErr: `reports metric "other"`},
		{name: "wrong namespace", body: hpaMetricList(hpaMetricItem("Pod", "elsewhere", "a", metric, "100", false), pod("b", "100")), wantErr: "unexpected object"},
		{name: "non-Pod object", body: hpaMetricList(hpaMetricItem("Deployment", ns, "a", metric, "100", false), pod("b", "100")), wantErr: "unexpected object Deployment"},
		{name: "malformed response", body: []byte("{"), wantErr: "decode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkHPAMetricSamples(tt.body, ns, metric, []string{"a", "b"}, hpaScaleUpValue)
			wantHPAError(t, err, tt.wantErr)
		})
	}
}

// customMetricsServer serves custom.metrics.k8s.io to a real clientset so the
// request path, query and version fallback are exercised end to end.
func hpaCustomMetricsServer(t *testing.T, handler func(version string, r *http.Request) (int, []byte)) kubernetes.Interface {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rest, ok := strings.CutPrefix(r.URL.Path, "/apis/custom.metrics.k8s.io/")
		if !ok {
			http.NotFound(w, r)
			return
		}
		version, _, _ := strings.Cut(rest, "/")
		code, body := handler(version, r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	c, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func hpaStatusBody(code int32, reason metav1.StatusReason) []byte {
	return []byte(fmt.Sprintf(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":%q,"code":%d}`, reason, code))
}

func TestReadHPAPodMetrics(t *testing.T) {
	ctx := context.Background()

	t.Run("queries the given version for the workload selector", func(t *testing.T) {
		var gotPath, gotSelector string
		c := hpaCustomMetricsServer(t, func(_ string, r *http.Request) (int, []byte) {
			gotPath, gotSelector = r.URL.Path, r.URL.Query().Get("labelSelector")
			if !strings.HasSuffix(r.URL.EscapedPath(), "/prometheus.googleapis.com%7Cm%7Cgauge") {
				t.Errorf("metric name not escaped as a single path segment: %s", r.URL.EscapedPath())
			}
			return http.StatusOK, hpaMetricList()
		})
		if _, err := readHPAPodMetrics(ctx, c, "v1beta2", "ns", "prometheus.googleapis.com|m|gauge"); err != nil {
			t.Fatal(err)
		}
		if want := "/apis/custom.metrics.k8s.io/v1beta2/namespaces/ns/pods/*/prometheus.googleapis.com|m|gauge"; gotPath != want {
			t.Errorf("path = %s, want %s", gotPath, want)
		}
		if gotSelector != hpaWorkloadSelector() {
			t.Errorf("labelSelector = %q, want %q", gotSelector, hpaWorkloadSelector())
		}
	})

	t.Run("queries only the resolved version", func(t *testing.T) {
		var versions []string
		c := hpaCustomMetricsServer(t, func(version string, _ *http.Request) (int, []byte) {
			versions = append(versions, version)
			return http.StatusOK, hpaMetricList(hpaMetricItem("Pod", "ns", "a", "m", "1", true))
		})
		body, err := readHPAPodMetrics(ctx, c, "v1beta1", "ns", "m")
		if err != nil || checkHPAMetricSamples(body, "ns", "m", []string{"a"}, 1) != nil {
			t.Fatalf("readHPAPodMetrics = %s, %v", body, err)
		}
		if strings.Join(versions, ",") != "v1beta1" {
			t.Errorf("queried versions %v, want only v1beta1", versions)
		}
	})

	// The adapter's own message (e.g. why a metric is not found) is kept.
	t.Run("keeps the adapter's error", func(t *testing.T) {
		var calls int32
		c := hpaCustomMetricsServer(t, func(string, *http.Request) (int, []byte) {
			atomic.AddInt32(&calls, 1)
			return http.StatusNotFound, []byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"message":"the server could not find the metric m for pods"}`)
		})
		_, err := readHPAPodMetrics(ctx, c, "v1beta2", "ns", "m")
		if !apierrors.IsNotFound(err) || !strings.Contains(err.Error(), "could not find the metric m") || calls != 1 {
			t.Fatalf("err=%v after %d calls, want the adapter's NotFound after 1 call", err, calls)
		}
	})
}

func TestWaitForMetric(t *testing.T) {
	fastHPATiming(t)
	ctx := context.Background()
	pods := []string{"a", "b"}
	scenario := func(c kubernetes.Interface, timeout time.Duration) *hpaScenario {
		return newHPAScenario(c, "ns", allocationModeDevicePlugin, "v1beta2", nvidiaConfig(t), testHPAConfig(t, timeout), t.Logf)
	}

	t.Run("waits through NotFound and stale samples until current", func(t *testing.T) {
		var calls int32
		c := hpaCustomMetricsServer(t, func(version string, _ *http.Request) (int, []byte) {
			if version != "v1beta2" {
				return http.StatusNotFound, hpaStatusBody(http.StatusNotFound, metav1.StatusReasonNotFound)
			}
			switch atomic.AddInt32(&calls, 1) {
			case 1:
				return http.StatusNotFound, hpaStatusBody(http.StatusNotFound, metav1.StatusReasonNotFound)
			case 2:
				return http.StatusOK, hpaMetricList(hpaMetricItem("Pod", "ns", "a", "m", "1", false), hpaMetricItem("Pod", "ns", "b", "m", "1", false))
			case 3:
				return http.StatusOK, hpaMetricList(hpaMetricItem("Pod", "ns", "a", "m", "100", false))
			default:
				return http.StatusOK, hpaMetricList(hpaMetricItem("Pod", "ns", "a", "m", "100", false), hpaMetricItem("Pod", "ns", "b", "m", "100", false))
			}
		})
		if err := scenario(c, 5*time.Second).waitForMetric(ctx, hpaScaleUpValue, fixedPods(pods)); err != nil {
			t.Fatalf("waitForMetric: %v", err)
		}
	})

	t.Run("times out on stale samples and reports them", func(t *testing.T) {
		c := hpaCustomMetricsServer(t, func(string, *http.Request) (int, []byte) {
			return http.StatusOK, hpaMetricList(hpaMetricItem("Pod", "ns", "a", "m", "1", false), hpaMetricItem("Pod", "ns", "b", "m", "1", false))
		})
		err := scenario(c, 100*time.Millisecond).waitForMetric(ctx, hpaScaleUpValue, fixedPods(pods))
		if err == nil || !strings.Contains(err.Error(), "Pod a reports 1, want 100") {
			t.Fatalf("error = %v, want a timeout naming the stale sample", err)
		}
	})

	t.Run("retries client-side errors until the deadline", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		host := "http://" + l.Addr().String()
		_ = l.Close() // connections are now refused
		c, err := kubernetes.NewForConfig(&rest.Config{Host: host, QPS: 1000, Burst: 1000})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		err = scenario(c, 200*time.Millisecond).waitForMetric(ctx, 1, fixedPods(pods))
		if err == nil || !strings.Contains(err.Error(), "last API error") {
			t.Fatalf("error = %v, want a timeout carrying the last API error", err)
		}
		if time.Since(start) < 150*time.Millisecond {
			t.Fatalf("client-side error aborted the wait after %s instead of being retried", time.Since(start))
		}
	})

	t.Run("fails fast on a permanent API error", func(t *testing.T) {
		c := hpaCustomMetricsServer(t, func(string, *http.Request) (int, []byte) {
			return http.StatusForbidden, hpaStatusBody(http.StatusForbidden, metav1.StatusReasonForbidden)
		})
		start := time.Now()
		if err := scenario(c, 5*time.Second).waitForMetric(ctx, 1, fixedPods(pods)); err == nil {
			t.Fatal("waitForMetric succeeded on Forbidden")
		}
		if time.Since(start) > time.Second {
			t.Fatalf("permanent error took %s to surface", time.Since(start))
		}
	})
}

func TestResolveCustomMetricsVersion(t *testing.T) {
	const g = "custom.metrics.k8s.io/"
	// The fake discovery client reports the first listed version of a group
	// as its preferred version.
	for _, tc := range []struct {
		name     string
		versions []string
		want     string
		wantErr  string
	}{
		{name: "only v1beta2", versions: []string{g + "v1beta2"}, want: "v1beta2"},
		{name: "only v1beta1", versions: []string{g + "v1beta1"}, want: "v1beta1"},
		{name: "preferred v1beta2", versions: []string{g + "v1beta2", g + "v1beta1"}, want: "v1beta2"},
		// The test must read the version the HPA controller reads.
		{name: "preferred v1beta1 is what the HPA reads", versions: []string{g + "v1beta1", g + "v1beta2"}, want: "v1beta1"},
		{name: "unsupported preferred version falls back to a listed supported one", versions: []string{g + "v1alpha1", g + "v1beta1"}, want: "v1beta1"},
		{name: "only unsupported versions", versions: []string{g + "v1alpha1"}, wantErr: "none of which the HPA supports"},
		{name: "not served", versions: []string{"external.metrics.k8s.io/v1beta1"}, wantErr: "is not served"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientset()
			fd := c.Discovery().(*fakediscovery.FakeDiscovery)
			for _, gv := range tc.versions {
				fd.Resources = append(fd.Resources, &metav1.APIResourceList{GroupVersion: gv})
			}
			got, err := resolveCustomMetricsVersion(c)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("resolveCustomMetricsVersion = %q, %v; want %q", got, err, tc.want)
			}
		})
	}

	// The fake client cannot express a preferred version other than the first
	// listed one, so serve real discovery documents.
	t.Run("preferred version that is not listed first", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/api":
				_, _ = w.Write([]byte(`{"kind":"APIVersions","versions":["v1"]}`))
			case "/apis":
				_, _ = w.Write([]byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[{"name":"custom.metrics.k8s.io",` +
					`"versions":[{"groupVersion":"custom.metrics.k8s.io/v1beta2","version":"v1beta2"},{"groupVersion":"custom.metrics.k8s.io/v1beta1","version":"v1beta1"}],` +
					`"preferredVersion":{"groupVersion":"custom.metrics.k8s.io/v1beta1","version":"v1beta1"}}]}`))
			case "/apis/custom.metrics.k8s.io/v1beta1":
				_, _ = w.Write([]byte(`{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"custom.metrics.k8s.io/v1beta1","resources":[]}`))
			default:
				http.NotFound(w, r)
			}
		}))
		t.Cleanup(srv.Close)
		c, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := resolveCustomMetricsVersion(c); err != nil || got != "v1beta1" {
			t.Fatalf("resolveCustomMetricsVersion = %q, %v; want the preferred v1beta1", got, err)
		}
	})

	// Aggregated discovery drops versions whose APIService is unavailable but
	// keeps the group.
	t.Run("registered group without an available version", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/api":
				_, _ = w.Write([]byte(`{"kind":"APIVersions","versions":["v1"]}`))
			case "/apis":
				_, _ = w.Write([]byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[{"name":"custom.metrics.k8s.io","versions":[],"preferredVersion":{"groupVersion":"","version":""}}]}`))
			default:
				http.NotFound(w, r)
			}
		}))
		t.Cleanup(srv.Close)
		c, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resolveCustomMetricsVersion(c); err == nil || !strings.Contains(err.Error(), "registered but none of its versions is available") {
			t.Fatalf("error = %v, want the unavailable-adapter message", err)
		}
	})

	t.Run("unavailable adapter is not treated as absent", func(t *testing.T) {
		c := fake.NewClientset()
		c.Discovery().(*fakediscovery.FakeDiscovery).Resources = []*metav1.APIResourceList{{GroupVersion: g + "v1beta2"}}
		c.PrependReactor("get", "resource", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewServiceUnavailable("adapter down")
		})
		if _, err := resolveCustomMetricsVersion(c); err == nil || !strings.Contains(err.Error(), "registered but not available") {
			t.Fatalf("error = %v, want an unavailable-adapter error", err)
		}
	})
}

func TestHPAFailureMessage(t *testing.T) {
	env := hpaEnvironmentError(errors.New("no capacity"))
	if got := hpaFailureMessage(env); got != "ENVIRONMENT ERROR: no capacity" {
		t.Errorf("environment error message = %q", got)
	}
	if got := hpaFailureMessage(fmt.Errorf("baseline: %w", env)); got != "ENVIRONMENT ERROR: baseline: no capacity" {
		t.Errorf("wrapped environment error message = %q", got)
	}
	if got := hpaFailureMessage(errors.New("HPA did not scale")); got != "FAIL: HPA did not scale" {
		t.Errorf("conformance failure message = %q", got)
	}
}

func TestHPAPhaseFailure(t *testing.T) {
	failedMetric := hpaScaledState(1, "1")
	failedMetric.hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{{
		Type: autoscalingv2.ScalingActive, Status: corev1.ConditionFalse, Reason: "FailedGetPodsMetric", Message: "forbidden",
	}}
	invalidSelector := hpaScaledState(1, "1")
	invalidSelector.hpa.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{{
		Type: autoscalingv2.ScalingActive, Status: corev1.ConditionFalse, Reason: "InvalidSelector",
	}}
	cannotScale := hpaScaledState(1, "100")
	cannotScale.hpa.Status.Conditions = append(cannotScale.hpa.Status.Conditions, autoscalingv2.HorizontalPodAutoscalerCondition{
		Type: autoscalingv2.AbleToScale, Status: corev1.ConditionFalse, Reason: "FailedUpdateScale", Message: "forbidden",
	})
	scaling := errors.New("HPA did not scale")
	for _, tc := range []struct {
		name        string
		err         error
		last        hpaState
		wantEnv     bool
		wantMessage string
	}{
		{name: "HPA reads the metric but does not scale", err: scaling, last: hpaScaledState(1, "100")},
		{name: "HPA cannot read the metric", err: scaling, last: failedMetric, wantEnv: true},
		// Other ScalingActive=False reasons concern the HPA or its target,
		// not the prerequisites, and stay conformance failures.
		{name: "HPA rejects its target", err: scaling, last: invalidSelector},
		{name: "HPA never observed", err: scaling, last: hpaState{}},
		{name: "HPA cannot update the scale", err: scaling, last: cannotScale, wantMessage: "AbleToScale=False (FailedUpdateScale: forbidden)"},
		{name: "already an environment error", err: hpaEnvironmentError(scaling), last: hpaScaledState(1, "100"), wantEnv: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := hpaPhaseFailure(tc.err, tc.last)
			if isHPAEnvironmentError(got) != tc.wantEnv {
				t.Fatalf("%q: environment=%t, want %t", hpaFailureMessage(got), isHPAEnvironmentError(got), tc.wantEnv)
			}
			if !errors.Is(got, scaling) {
				t.Fatalf("classification lost the original error: %v", got)
			}
			if !strings.Contains(got.Error(), tc.wantMessage) {
				t.Fatalf("%q does not report %q", got, tc.wantMessage)
			}
		})
	}
}

func TestHPAPodPrerequisiteProblems(t *testing.T) {
	pod := func(name string, status corev1.PodStatus) corev1.Pod {
		p := hpaReadyPod(name, "n", types.UID("uid-"+name))
		p.Status = status
		return p
	}
	waiting := func(reason, message string) corev1.ContainerState {
		return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: message}}
	}
	running := corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	exited := func(code int32, message string) corev1.ContainerState {
		return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Reason: "Error", Message: message}}
	}
	ready := []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	tests := []struct {
		name string
		pod  corev1.Pod
		want string // substring of the single problem reported, "" for none
	}{
		{name: "unschedulable", pod: pod("a", corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "Insufficient nvidia.com/gpu",
		}}}), want: "a is Unschedulable: Insufficient nvidia.com/gpu"},
		{name: "image pull failure", pod: pod("b", corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{State: waiting("ImagePullBackOff", "")}}}),
			want: "b cannot pull its image (ImagePullBackOff"},
		{name: "container cannot be created", pod: pod("c", corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{State: waiting("CreateContainerError", "no such device")}}}),
			want: "c cannot start its container (CreateContainerError: no such device)"},
		// The container's startup check failed: its recorded output names
		// the cause, which may be the platform or the test's own setup.
		{name: "startup check failed, crash looping", pod: pod("d", corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			State: waiting("CrashLoopBackOff", "back-off"), RestartCount: 3,
			LastTerminationState: exited(1, "RESULT: ACCELERATOR_COUNT=0\nexpected 1 accelerator device(s), found 0\n"),
		}}}), want: "d: workload container exited with code 1 (Error), restarts=3; output: RESULT: ACCELERATOR_COUNT=0 expected 1 accelerator device(s), found 0"},
		{name: "image without the workload's interpreter", pod: pod("e", corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{
			State: exited(127, "/bin/sh: exec: line 7: python3: not found"),
		}}}), want: "e: workload container exited with code 127"},
		{name: "crash looping with no recorded exit", pod: pod("f", corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{State: waiting("CrashLoopBackOff", "back-off 10s")}}}),
			want: "f: workload container is in CrashLoopBackOff, restarts=0: back-off 10s"},
		// A container that failed earlier but runs again is healthy now; its
		// past exit must not turn a later HPA failure into an environment error.
		{name: "restarted earlier, running now", pod: pod("g", corev1.PodStatus{Phase: corev1.PodRunning, Conditions: ready, ContainerStatuses: []corev1.ContainerStatus{{
			State: running, RestartCount: 1, LastTerminationState: exited(1, "transient"),
		}}})},
		{name: "healthy", pod: hpaReadyPod("h", "n", "uid-h")},
		// Pods the node ended are terminal (Phase=Failed).
		{name: "rejected at admission", pod: pod("i", corev1.PodStatus{Phase: corev1.PodFailed, Reason: "UnexpectedAdmissionError",
			Message: "Allocate failed due to requested number of devices unavailable for nvidia.com/gpu. Requested: 1, Available: 0"}),
			want: "i failed (UnexpectedAdmissionError: Allocate failed due to requested number of devices unavailable"},
		{name: "evicted", pod: pod("j", corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", Message: "The node was low on resource: memory.",
			ContainerStatuses: []corev1.ContainerStatus{{State: exited(137, "")}}}),
			want: "j: workload container exited with code 137"},
		{name: "being deleted", pod: func() corev1.Pod {
			p := pod("k", corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{State: exited(143, "")}}})
			p.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			return p
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hpaPodPrerequisiteProblems([]corev1.Pod{tt.pod})
			switch {
			case tt.want == "" && len(got) != 0:
				t.Fatalf("problems = %q, want none", got)
			case tt.want != "" && (len(got) != 1 || !strings.Contains(got[0], tt.want)):
				t.Fatalf("problems = %q, want one containing %q", got, tt.want)
			}
		})
	}

	long := strings.Repeat("x", 1000) + " tail"
	if got := hpaOneLine(long); len(got) > 310 || !strings.HasSuffix(got, " tail") {
		t.Errorf("hpaOneLine kept %d chars ending %q, want the last 300 chars", len(got), got[len(got)-10:])
	}
}

func TestWaitForStateClassification(t *testing.T) {
	fastHPATiming(t)
	ctx := context.Background()
	accept := func(context.Context, hpaState) error { return nil }

	t.Run("permanent API error", func(t *testing.T) {
		c := fake.NewClientset()
		c.PrependReactor("get", "horizontalpodautoscalers", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "horizontalpodautoscalers"}, "x", errors.New("denied"))
		})
		start := time.Now()
		_, err := testHPAScenario(t, c).waitForState(ctx, time.Second, accept)
		if !isHPAEnvironmentError(err) || !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "observe the test workload") {
			t.Fatalf("error = %q, want an environment error wrapping Forbidden", hpaFailureMessage(err))
		}
		if time.Since(start) > 500*time.Millisecond {
			t.Fatalf("permanent API error took %s to surface instead of failing fast", time.Since(start))
		}
	})

	t.Run("API never reachable", func(t *testing.T) {
		c := fake.NewClientset()
		c.PrependReactor("get", "horizontalpodautoscalers", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewServiceUnavailable("overloaded")
		})
		_, err := testHPAScenario(t, c).waitForState(ctx, 50*time.Millisecond, accept)
		if !isHPAEnvironmentError(err) || !strings.Contains(err.Error(), "could not observe") {
			t.Fatalf("error = %q, want an environment error", hpaFailureMessage(err))
		}
	})

	// The Deployment read succeeds for the first `ok` polls, then fails with
	// failure (nil: never fails).
	failingAfter := func(ok int32, failure error) *fake.Clientset {
		c := fake.NewClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}})
		var reads atomic.Int32
		c.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
			if reads.Add(1) <= ok || failure == nil {
				return false, nil, nil
			}
			return true, nil, failure
		})
		return c
	}
	notScaled := func(context.Context, hpaState) error { return errors.New("not scaled yet") }

	t.Run("permanent API error after an observation", func(t *testing.T) {
		forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, hpaWorkloadName, errors.New("RBAC changed"))
		start := time.Now()
		_, err := testHPAScenario(t, failingAfter(1, forbidden)).waitForState(ctx, time.Second, notScaled)
		if !isHPAEnvironmentError(err) || !apierrors.IsForbidden(err) || time.Since(start) > 500*time.Millisecond {
			t.Fatalf("error = %q after %s, want a prompt environment error wrapping Forbidden", hpaFailureMessage(err), time.Since(start))
		}
	})

	// The HPA may have reached the state while the API was unreachable.
	t.Run("prolonged API outage after an observation", func(t *testing.T) {
		_, err := testHPAScenario(t, failingAfter(1, apierrors.NewServiceUnavailable("overloaded"))).waitForState(ctx, time.Second, notScaled)
		if !isHPAEnvironmentError(err) || !strings.Contains(err.Error(), "was not observed for the last") || !strings.Contains(err.Error(), "overloaded") {
			t.Fatalf("error = %q, want an environment error for the unobserved end of the wait", hpaFailureMessage(err))
		}
	})

	t.Run("brief API outage, then observed failing until the deadline", func(t *testing.T) {
		c := fake.NewClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}})
		var reads atomic.Int32
		c.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
			if n := reads.Add(1); n == 2 || n == 3 {
				return true, nil, apierrors.NewServiceUnavailable("overloaded")
			}
			return false, nil, nil
		})
		_, err := testHPAScenario(t, c).waitForState(ctx, 100*time.Millisecond, notScaled)
		if err == nil || isHPAEnvironmentError(err) || !strings.Contains(err.Error(), "last observation: not scaled yet") {
			t.Fatalf("error = %q, want the conformance failure of the last observation", hpaFailureMessage(err))
		}
	})

	// A failed API read inside the check (the custom metrics read while
	// scaling up) is not the workload's problem.
	t.Run("retryable API errors inside the check are not observations", func(t *testing.T) {
		c := fake.NewClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}})
		var calls atomic.Int32
		_, err := testHPAScenario(t, c).waitForState(ctx, time.Second, func(context.Context, hpaState) error {
			if calls.Add(1) == 1 {
				return errors.New("not scaled yet")
			}
			return fmt.Errorf("custom metric for the scaled-up Pods: %w", apierrors.NewServiceUnavailable("overloaded"))
		})
		if !isHPAEnvironmentError(err) || !strings.Contains(err.Error(), "was not observed") {
			t.Fatalf("error = %q, want an environment error for the unobserved end of the wait", hpaFailureMessage(err))
		}
	})

	// The wait's own deadline cutting a request short is a timeout, not an
	// access problem.
	t.Run("deadline expiring during a request", func(t *testing.T) {
		// A grace longer than the stalled request isolates the deadline
		// handling from the observation-gap rule.
		grace := hpaObservationGrace
		hpaObservationGrace = time.Second
		defer func() { hpaObservationGrace = grace }()
		c := fake.NewClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}})
		var reads atomic.Int32
		c.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
			if reads.Add(1) == 1 {
				return false, nil, nil
			}
			time.Sleep(100 * time.Millisecond) // past the wait's deadline
			return true, nil, fmt.Errorf("client rate limiter Wait returned an error: %w", context.DeadlineExceeded)
		})
		_, err := testHPAScenario(t, c).waitForState(ctx, 30*time.Millisecond, notScaled)
		if err == nil || isHPAEnvironmentError(err) || !strings.Contains(err.Error(), "last observation: not scaled yet") {
			t.Fatalf("error = %q, want the conformance failure of the last observation", hpaFailureMessage(err))
		}
	})

	t.Run("recovers from a retryable API error", func(t *testing.T) {
		c := fake.NewClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}})
		var reads atomic.Int32
		c.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
			if reads.Add(1) == 1 {
				return true, nil, apierrors.NewTooManyRequests("throttled", 0)
			}
			return false, nil, nil
		})
		if _, err := testHPAScenario(t, c).waitForState(ctx, time.Second, func(context.Context, hpaState) error { return nil }); err != nil {
			t.Fatalf("waitForState = %v, want success after the retryable error", err)
		}
	})

	t.Run("observed but never in the wanted state", func(t *testing.T) {
		c := fake.NewClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}})
		_, err := testHPAScenario(t, c).waitForState(ctx, 50*time.Millisecond, func(context.Context, hpaState) error {
			return errors.New("not scaled")
		})
		if err == nil || isHPAEnvironmentError(err) || !strings.Contains(err.Error(), "not scaled") {
			t.Fatalf("error = %q, want a conformance failure naming the last observation", hpaFailureMessage(err))
		}
	})
}

func hpaReadyPod(name, node string, uid types.UID) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: uid, Labels: hpaWorkloadLabels()},
		Spec:       corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func hpaStatusWith(replicas int32, average string) autoscalingv2.HorizontalPodAutoscalerStatus {
	return autoscalingv2.HorizontalPodAutoscalerStatus{
		ObservedGeneration: hpaPtr(int64(1)),
		CurrentReplicas:    replicas,
		DesiredReplicas:    replicas,
		CurrentMetrics: []autoscalingv2.MetricStatus{{
			Type: autoscalingv2.PodsMetricSourceType,
			Pods: &autoscalingv2.PodsMetricStatus{
				Metric:  autoscalingv2.MetricIdentifier{Name: "m"},
				Current: autoscalingv2.MetricValueStatus{AverageValue: hpaPtr(resource.MustParse(average))},
			},
		}},
		Conditions: []autoscalingv2.HorizontalPodAutoscalerCondition{{Type: autoscalingv2.ScalingActive, Status: corev1.ConditionTrue}},
	}
}

// scaledState returns a settled state at replicas with the given observed
// per-Pod average.
func hpaScaledState(replicas int32, average string) hpaState {
	var pods []corev1.Pod
	for i := int32(0); i < replicas; i++ {
		pods = append(pods, hpaReadyPod(fmt.Sprintf("p%d", i), "node", types.UID(fmt.Sprintf("uid-%d", i))))
	}
	return hpaState{
		hpa: &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Generation: 1},
			Status:     hpaStatusWith(replicas, average),
		},
		deployment: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Generation: 3},
			Spec:       appsv1.DeploymentSpec{Replicas: hpaPtr(replicas)},
			Status:     appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: replicas, UpdatedReplicas: replicas, ReadyReplicas: replicas},
		},
		pods: pods,
	}
}

func TestCheckHPAScaledTo(t *testing.T) {
	tests := []struct {
		name    string
		want    int32
		average string
		value   int
		mutate  func(*hpaState)
		wantErr string
	}{
		{name: "scaled up", want: 2, average: "100", value: 100},
		{name: "scaled down", want: 1, average: "2", value: 2},
		{name: "HPA missing", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.hpa = nil }, wantErr: "HPA does not exist"},
		{name: "observedGeneration unset, as released HPA controllers leave it", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.hpa.Status.ObservedGeneration = nil }},
		{name: "HPA status reports an older generation", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.hpa.Status.ObservedGeneration = hpaPtr(int64(0)) }, wantErr: "not observed generation"},
		{name: "ScalingActive false", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) {
				s.hpa.Status.Conditions[0] = autoscalingv2.HorizontalPodAutoscalerCondition{Type: autoscalingv2.ScalingActive, Status: corev1.ConditionFalse, Reason: "FailedGetPodsMetric"}
			}, wantErr: "FailedGetPodsMetric"},
		{name: "ScalingActive missing", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.hpa.Status.Conditions = nil }, wantErr: "ScalingActive is not reported"},
		{name: "metric missing from HPA status", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.hpa.Status.CurrentMetrics = nil }, wantErr: "no Pods metric"},
		// The recorded average must be the phase's value, not merely on the
		// right side of the target: 1 is the baseline phase's value.
		{name: "scale-down with another phase's below-target average", want: 1, average: "1", value: 2, wantErr: "want the current value 2"},
		{name: "scale-up with an above-target average that is not the phase's", want: 2, average: "50", value: 100, wantErr: "want the current value 100"},
		{name: "equal quantities in other units match", want: 2, average: "100000m", value: 100},
		{name: "metric not above target on scale-up", want: 2, average: "10", value: 100, wantErr: "want the current value 100"},
		{name: "metric not below target on scale-down", want: 1, average: "100", value: 2, wantErr: "want the current value 2"},
		{name: "HPA desired replicas differ", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.hpa.Status.DesiredReplicas = 3 }, wantErr: "desiredReplicas=3"},
		{name: "HPA current replicas differ", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.hpa.Status.CurrentReplicas = 1 }, wantErr: "currentReplicas=1"},
		{name: "Deployment spec not scaled", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.deployment.Spec.Replicas = hpaPtr(int32(1)) }, wantErr: "spec.replicas=1"},
		{name: "Deployment status stale", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.deployment.Status.ObservedGeneration = 2 }, wantErr: "not observed generation 3"},
		{name: "Deployment not ready", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.deployment.Status.ReadyReplicas = 1 }, wantErr: "ready=1"},
		{name: "HPA scaled but new Pod missing", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.pods = s.pods[:1] }, wantErr: "found 1 active"},
		{name: "extra Pod", want: 1, average: "2", value: 2,
			mutate: func(s *hpaState) { s.pods = append(s.pods, hpaReadyPod("extra", "node", "x")) }, wantErr: "found 2 active"},
		{name: "Pod pending", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.pods[1].Status.Phase = corev1.PodPending }, wantErr: "want Running and Ready"},
		{name: "Pod running but not ready", want: 2, average: "100", value: 100,
			mutate: func(s *hpaState) { s.pods[1].Status.Conditions = nil }, wantErr: "ready=false"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := hpaScaledState(tt.want, tt.average)
			if tt.mutate != nil {
				tt.mutate(&s)
			}
			err := checkHPAScaledTo(s, "m", tt.want, tt.value)
			wantHPAError(t, err, tt.wantErr)
		})
	}
}

func TestCheckHPADecided(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*hpaState)
		wantErr string
	}{
		{name: "held"},
		{name: "Pod evicted during the window", mutate: func(s *hpaState) {
			s.inactive, s.pods = s.pods, nil
			s.inactive[0].Status.Phase = corev1.PodFailed
			s.deployment.Status.ReadyReplicas = 0
		}},
		{name: "HPA decided to scale", mutate: func(s *hpaState) { s.hpa.Status.DesiredReplicas = 2 }, wantErr: "desiredReplicas=2"},
		{name: "Deployment scaled", mutate: func(s *hpaState) { s.deployment.Spec.Replicas = hpaPtr(int32(2)) }, wantErr: "spec.replicas=2"},
		{name: "HPA records another value", mutate: func(s *hpaState) {
			s.hpa.Status.CurrentMetrics[0].Pods.Current.AverageValue = hpaPtr(resource.MustParse("2"))
		}, wantErr: "want the current value 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := hpaScaledState(1, "1")
			if tt.mutate != nil {
				tt.mutate(&s)
			}
			err := checkHPADecided(s, "m", 1, 1)
			if (tt.wantErr == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestGetHPAState(t *testing.T) {
	deleting := hpaReadyPod("deleting", "n", "u3")
	deleting.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	deleting.Finalizers = []string{"test"}
	failed := hpaReadyPod("failed", "n", "u4")
	failed.Status.Phase = corev1.PodFailed
	other := hpaReadyPod("other", "n", "u5")
	other.Labels = map[string]string{"app": "unrelated"}
	b, a := hpaReadyPod("b", "n", "u2"), hpaReadyPod("a", "n", "u1")
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}}
	client := fake.NewClientset(
		&autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}},
		deployment, &b, &a, &deleting, &failed, &other,
	)
	s, err := getHPAState(context.Background(), client, "ns")
	if err != nil {
		t.Fatalf("getHPAState: %v", err)
	}
	if got := strings.Join(s.podNames(), ","); got != "a,b" {
		t.Errorf("pods = %s, want a,b (sorted, excluding terminating, finished and unrelated Pods)", got)
	}
	var inactive []string
	for _, pod := range s.inactive {
		inactive = append(inactive, pod.Name)
	}
	if fmt.Sprint(inactive) != "[deleting failed]" || s.hpa == nil {
		t.Errorf("inactive = %v, hpa = %v; want [deleting failed] and the HPA", inactive, s.hpa)
	}

	noHPA, err := getHPAState(context.Background(), fake.NewClientset(deployment), "ns")
	if err != nil || noHPA.hpa != nil {
		t.Errorf("without an HPA: state.hpa=%v err=%v, want nil HPA and no error", noHPA.hpa, err)
	}
	if _, err := getHPAState(context.Background(), fake.NewClientset(), "ns"); !apierrors.IsNotFound(err) {
		t.Errorf("without a Deployment: error = %v, want NotFound", err)
	}
}

// TestHPAScaleUpFailure covers how a failure to get the scaled-up Pods Ready
// is classified: Pods that cannot start are an environment problem only while
// the HPA still decides on maxReplicas for the raised metric.
func TestHPAScaleUpFailure(t *testing.T) {
	s := testHPAScenario(t, fake.NewClientset())
	notReady := errors.New("not Ready")
	unschedulable := hpaReadyPod("p1", "", "u1")
	unschedulable.Status = corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
		Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "Insufficient nvidia.com/gpu",
	}}}
	rejected := hpaReadyPod("p2", "n", "u2")
	rejected.Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "UnexpectedAdmissionError", Message: "Allocate failed"}
	stateWith := func(desired int32, pods, inactive []corev1.Pod) hpaState {
		st := hpaScaledState(2, "100")
		st.hpa.Status.DesiredReplicas = desired
		st.pods, st.inactive = pods, inactive
		return st
	}
	tests := []struct {
		name    string
		state   hpaState
		wantEnv bool
		wantErr string
	}{
		{name: "unschedulable Pod while the HPA holds maxReplicas", state: stateWith(2, []corev1.Pod{unschedulable}, nil), wantEnv: true, wantErr: "Insufficient nvidia.com/gpu"},
		{name: "Pod rejected at admission", state: stateWith(2, nil, []corev1.Pod{rejected}), wantEnv: true, wantErr: "p2 failed (UnexpectedAdmissionError"},
		{name: "HPA no longer decides on maxReplicas", state: stateWith(1, []corev1.Pod{unschedulable}, nil), wantErr: "not Ready; HPA conditions"},
		{name: "no Pod problem", state: stateWith(2, nil, nil), wantErr: "not Ready; HPA conditions"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.scaleUpFailure(notReady, tt.state)
			wantHPAError(t, err, tt.wantErr)
			if isHPAEnvironmentError(err) != tt.wantEnv {
				t.Fatalf("%q classified as environment=%t, want %t", hpaFailureMessage(err), isHPAEnvironmentError(err), tt.wantEnv)
			}
		})
	}
}

func TestHPAAllocatedClaims(t *testing.T) {
	claim := func(name string, allocated bool) resourcev1.ResourceClaim {
		c := resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if allocated {
			c.Status.Allocation = &resourcev1.AllocationResult{}
		}
		return c
	}
	got := hpaAllocatedClaims([]resourcev1.ResourceClaim{claim("b", true), claim("released", false), claim("a", true)})
	if fmt.Sprint(got) != "[a b]" {
		t.Errorf("hpaAllocatedClaims = %v, want [a b]", got)
	}
}

func TestHPARunFailureMessage(t *testing.T) {
	scaling := errors.New("HPA did not scale")
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if got := hpaRunFailureMessage(expired, scaling); !strings.HasPrefix(got, "ENVIRONMENT ERROR: the go test -timeout was reached") {
		t.Errorf("expired run context: %q, want an environment error naming the timeout", got)
	}
	if got := hpaRunFailureMessage(context.Background(), scaling); got != "FAIL: HPA did not scale" {
		t.Errorf("live run context: %q, want the conformance failure unchanged", got)
	}
}

// hpaUnresponsiveServer returns a client for an API server that accepts
// requests but never answers them.
func hpaUnresponsiveServer(t *testing.T) kubernetes.Interface {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A canceled client request need not cancel r, so the handler is
		// also released when the test ends.
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	c, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL, QPS: 1000, Burst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestHPACleanup(t *testing.T) {
	fastHPATiming(t)
	ctx := context.Background()

	t.Run("bounded when the API server does not answer", func(t *testing.T) {
		s := newHPAScenario(hpaUnresponsiveServer(t), "ns", allocationModeDevicePlugin, "v1beta2", nvidiaConfig(t), testHPAConfig(t, time.Second), t.Logf)
		start := time.Now()
		var err error
		done := make(chan struct{})
		go func() { err = s.cleanup(ctx); close(done) }()
		select {
		case <-done:
		case <-time.After(hpaCleanupTimeout + 5*time.Second):
			t.Fatalf("cleanup still running after %s, want it bounded by hpaCleanupTimeout (%s)", time.Since(start), hpaCleanupTimeout)
		}
		if elapsed := time.Since(start); elapsed > hpaCleanupTimeout+time.Second {
			t.Fatalf("cleanup took %s, want it bounded by hpaCleanupTimeout (%s)", elapsed, hpaCleanupTimeout)
		}
		if err == nil || !strings.Contains(err.Error(), "delete HorizontalPodAutoscaler") || !strings.Contains(err.Error(), "delete Deployment") {
			t.Fatalf("error = %v, want both failed deletes reported", err)
		}
	})

	t.Run("continues after a failed step and reports it", func(t *testing.T) {
		c := fake.NewClientset(
			&autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}},
			&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}},
		)
		c.PrependReactor("delete", "horizontalpodautoscalers", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "autoscaling", Resource: "horizontalpodautoscalers"}, hpaWorkloadName, errors.New("denied"))
		})
		s := testHPAScenario(t, c)
		err := s.cleanup(ctx)
		if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), "delete HorizontalPodAutoscaler") {
			t.Fatalf("error = %v, want the failed HPA delete reported", err)
		}
		if _, err := c.AppsV1().Deployments("ns").Get(ctx, hpaWorkloadName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("Deployment still exists after cleanup (get error %v)", err)
		}
	})

	// The fake clientset does not garbage-collect, so the Pod outlives the
	// Deployment, as a Pod stuck terminating would.
	t.Run("reports workload Pods that are never removed", func(t *testing.T) {
		pod := hpaReadyPod("hpa-workload-1", "n", "u1")
		s := testHPAScenario(t, fake.NewClientset(&pod))
		if err := s.cleanup(ctx); err == nil || !strings.Contains(err.Error(), "remaining: 1 workload Pods") {
			t.Fatalf("cleanup error = %v, want the remaining Pod reported", err)
		}
	})

	t.Run("reports ResourceClaims that stay allocated", func(t *testing.T) {
		claim := &resourcev1.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "hpa-workload-1-claim", Namespace: "ns"},
			Status:     resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{}},
		}
		s := newHPAScenario(fake.NewClientset(claim), "ns", allocationModeDRA, "v1beta2", nvidiaConfig(t), testHPAConfig(t, time.Second), t.Logf)
		if err := s.cleanup(ctx); err == nil || !strings.Contains(err.Error(), "allocated ResourceClaims hpa-workload-1-claim") {
			t.Fatalf("cleanup error = %v, want the allocated claim reported", err)
		}
	})

	t.Run("nothing to clean up after a failed setup", func(t *testing.T) {
		s := newHPAScenario(fake.NewClientset(), "ns", allocationModeDRA, "v1beta2", nvidiaConfig(t), testHPAConfig(t, time.Second), t.Logf)
		if err := s.cleanup(ctx); err != nil {
			t.Fatalf("cleanup with nothing created = %v, want nil", err)
		}
	})
}

// TestHPACleanupFitsReserve keeps the go test -timeout reserve large enough
// for both cleanups, which run after the scenario stops.
func TestHPACleanupFitsReserve(t *testing.T) {
	const namespaceDeletion = 2 * time.Minute // deleteNamespaceAndWait: two one-minute polls
	if need := hpaCleanupTimeout + namespaceDeletion; hpaCleanupReserve < need {
		t.Fatalf("hpaCleanupReserve %s < cleanup %s + namespace deletion %s", hpaCleanupReserve, hpaCleanupTimeout, namespaceDeletion)
	}
}

func TestHPARunContext(t *testing.T) {
	now := time.Now()
	if _, _, err := hpaRunContext(now, now.Add(10*time.Minute), true); err == nil || !strings.Contains(err.Error(), "-timeout 75m") {
		t.Errorf("go test's default 10m timeout accepted: %v", err)
	}
	ctx, cancel, err := hpaRunContext(now, now.Add(75*time.Minute), true)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if d, ok := ctx.Deadline(); !ok || !d.Equal(now.Add(75*time.Minute-hpaCleanupReserve)) {
		t.Errorf("run deadline = %v, %t; want the test deadline minus the cleanup reserve", d, ok)
	}
	ctx, cancel, err = hpaRunContext(now, time.Time{}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Error("run context has a deadline although go test has none (-timeout 0)")
	}
}

func TestHoldStatePermanentAPIError(t *testing.T) {
	fastHPATiming(t)
	c := fake.NewClientset()
	c.PrependReactor("get", "horizontalpodautoscalers", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "horizontalpodautoscalers"}, "x", errors.New("denied"))
	})
	s := testHPAScenario(t, c)
	_, err := s.holdState(context.Background(), time.Second, func(context.Context, hpaState) error { return nil })
	if !isHPAEnvironmentError(err) || !apierrors.IsForbidden(err) {
		t.Fatalf("error = %q, want an environment error wrapping Forbidden", hpaFailureMessage(err))
	}
}

func TestHoldStateRequiresObservationThroughoutTheWindow(t *testing.T) {
	fastHPATiming(t)
	c := fake.NewClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}})
	start := time.Now()
	c.PrependReactor("get", "deployments", func(k8stesting.Action) (bool, runtime.Object, error) {
		if elapsed := time.Since(start); elapsed > 20*time.Millisecond && elapsed < 700*time.Millisecond {
			return true, nil, apierrors.NewServiceUnavailable("overloaded")
		}
		return false, nil, nil
	})
	s := testHPAScenario(t, c)
	_, err := s.holdState(context.Background(), time.Second, func(context.Context, hpaState) error { return nil })
	if !isHPAEnvironmentError(err) || !strings.Contains(err.Error(), "could not observe the HPA for") {
		t.Fatalf("holdState with an unobserved gap returned %v, want an environment error", err)
	}
}

// A successful API response that arrives only after an unobserved gap
// must not make the negative-control window pass retrospectively.
func TestHoldStateRejectsSlowSuccessfulObservation(t *testing.T) {
	fastHPATiming(t)
	c := fake.NewClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}})
	c.PrependReactor("get", "horizontalpodautoscalers", func(k8stesting.Action) (bool, runtime.Object, error) {
		time.Sleep(400 * time.Millisecond) // longer than the 250ms observation grace
		return false, nil, nil
	})
	s := testHPAScenario(t, c)
	_, err := s.holdState(context.Background(), 150*time.Millisecond, func(context.Context, hpaState) error { return nil })
	if !isHPAEnvironmentError(err) || !strings.Contains(err.Error(), "could not observe the HPA for") {
		t.Fatalf("slow successful observation returned %v, want an observation-gap environment error", err)
	}
}

func TestHoldStateRequiresAFinalObservation(t *testing.T) {
	fastHPATiming(t)
	c := fake.NewClientset()
	c.PrependReactor("get", "horizontalpodautoscalers", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("apiserver overloaded")
	})
	s := testHPAScenario(t, c)
	_, err := s.holdState(context.Background(), 20*time.Millisecond, func(context.Context, hpaState) error { return nil })
	if !isHPAEnvironmentError(err) || !strings.Contains(err.Error(), "could not observe") {
		t.Fatalf("holdState with no successful observation returned %v, want an error", err)
	}
}
