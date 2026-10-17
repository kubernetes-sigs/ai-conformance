package conformance

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	return testHPAConfigN(t, timeout, 2)
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

func testHPAConfigN(t *testing.T, timeout time.Duration, maxReplicas int) hpaConfig {
	t.Helper()
	hc, err := newHPAConfig("m", maxReplicas, "", "example.com/python:test", timeout, timeout, timeout, 0)
	if err != nil {
		t.Fatalf("newHPAConfig: %v", err)
	}
	return hc
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

	// A failed Node or ResourceClaim read inside the check is not the
	// workload's problem.
	t.Run("retryable API errors inside the check are not observations", func(t *testing.T) {
		c := fake.NewClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}})
		var calls atomic.Int32
		_, err := testHPAScenario(t, c).waitForState(ctx, time.Second, func(context.Context, hpaState) error {
			if calls.Add(1) == 1 {
				return errors.New("not scaled yet")
			}
			return fmt.Errorf("get node n of Pod p: %w", apierrors.NewServiceUnavailable("overloaded"))
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

func TestCheckHPARemovedPodsGone(t *testing.T) {
	scaledUp := map[types.UID]bool{"u1": true, "u2": true}
	terminating := hpaReadyPod("p2", "n", "u2")
	terminating.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	terminatingFailed := terminating
	terminatingFailed.Status.Phase = corev1.PodFailed // kubelets set a terminal phase while deleting
	evicted := hpaReadyPod("p2", "n", "u2")
	evicted.Status.Phase, evicted.Status.Reason = corev1.PodFailed, "Evicted"
	succeeded := hpaReadyPod("old", "n", "u0")
	succeeded.Status.Phase = corev1.PodSucceeded
	disrupted := terminating
	disrupted.Status.Conditions = []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: "EvictionByEvictionAPI"}}
	disruptionCleared := terminating
	disruptionCleared.Status.Conditions = []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: corev1.ConditionFalse, Reason: "EvictionByEvictionAPI"}}

	tests := []struct {
		name      string
		state     hpaState
		wantErr   string
		wantNotes string
	}{
		{name: "survivor from the scaled-up set", state: hpaState{pods: []corev1.Pod{hpaReadyPod("p1", "n", "u1")}}},
		{name: "removed Pod terminating", state: hpaState{pods: []corev1.Pod{hpaReadyPod("p1", "n", "u1")}, inactive: []corev1.Pod{terminating}}, wantErr: "p2 (Terminating)"},
		{name: "removed Pod terminating in a terminal phase", state: hpaState{pods: []corev1.Pod{hpaReadyPod("p1", "n", "u1")}, inactive: []corev1.Pod{terminatingFailed}}, wantErr: "p2 (Terminating)"},
		// Pod events outside autoscaling are allowed and noted.
		{name: "Pod evicted by its node", state: hpaState{pods: []corev1.Pod{hpaReadyPod("p1", "n", "u1")}, inactive: []corev1.Pod{evicted}},
			wantNotes: "Pod p2 was ended by its node (Failed: Evicted), not removed by the scale-down"},
		{name: "replacement for an evicted Pod", state: hpaState{pods: []corev1.Pod{hpaReadyPod("p3", "n", "u3")}, inactive: []corev1.Pod{evicted}},
			wantNotes: "Pod p3 replaced a scaled-up Pod"},
		{name: "Pod terminating after an eviction", state: hpaState{pods: []corev1.Pod{hpaReadyPod("p3", "n", "u3")}, inactive: []corev1.Pod{disrupted}},
			wantNotes: "Pod p2 is terminating after a disruption (EvictionByEvictionAPI), not removed by the scale-down"},
		// Only a DisruptionTarget=True condition exempts a terminating Pod.
		{name: "terminating Pod with a cleared disruption condition", state: hpaState{pods: []corev1.Pod{hpaReadyPod("p1", "n", "u1")}, inactive: []corev1.Pod{disruptionCleared}},
			wantErr: "p2 (Terminating)"},
		{name: "unrelated finished Pod", state: hpaState{pods: []corev1.Pod{hpaReadyPod("p1", "n", "u1")}, inactive: []corev1.Pod{succeeded}},
			wantNotes: "Pod old was ended by its node (Succeeded), not removed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if notes := hpaLifecycleNotes(tt.state, scaledUp); !strings.Contains(notes, tt.wantNotes) || (tt.wantNotes == "") != (notes == "") {
				t.Errorf("notes = %q, want %q", notes, tt.wantNotes)
			}
			err := checkHPARemovedPodsGone(tt.state)
			wantHPAError(t, err, tt.wantErr)
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

func hpaAcceleratorNode(name string, gpus int64) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	n.Status.Allocatable = corev1.ResourceList{"nvidia.com/gpu": *resource.NewQuantity(gpus, resource.DecimalSI)}
	return n
}

func hpaWorkloadPod(t *testing.T, mode, node string) corev1.Pod {
	t.Helper()
	d, err := buildHPADeployment("ns", mode, nvidiaConfig(t), "example.com/python:test", nil)
	if err != nil {
		t.Fatal(err)
	}
	pod := hpaReadyPod("p", node, "pod-uid")
	pod.Spec = d.Spec.Template.Spec
	pod.Spec.NodeName = node
	if mode == allocationModeDRA {
		pod.Status.ResourceClaimStatuses = []corev1.PodResourceClaimStatus{{Name: "claim", ResourceClaimName: hpaPtr("p-claim")}}
	}
	return pod
}

func hpaAllocatedClaim(name string, podUID types.UID, driver string, devices int, selectorNode string) *resourcev1.ResourceClaim {
	// As generated from a ResourceClaimTemplate: controlled by its Pod.
	c := &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "v1", Kind: "Pod", Name: "p", UID: podUID, Controller: hpaPtr(true),
		}}},
		Status: resourcev1.ResourceClaimStatus{
			Allocation:  &resourcev1.AllocationResult{},
			ReservedFor: []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: "p", UID: podUID}},
		},
	}
	for i := 0; i < devices; i++ {
		c.Status.Allocation.Devices.Results = append(c.Status.Allocation.Devices.Results, resourcev1.DeviceRequestAllocationResult{
			Request: testRequestName, Driver: driver, Pool: "pool", Device: fmt.Sprintf("gpu-%d", i),
		})
	}
	if selectorNode != "" {
		c.Status.Allocation.NodeSelector = &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{selectorNode}}},
		}}}
	}
	return c
}

func TestCheckHPAPodAccelerator(t *testing.T) {
	cfg := nvidiaConfig(t)
	dp := hpaWorkloadPod(t, allocationModeDevicePlugin, "gpu-node")
	dra := hpaWorkloadPod(t, allocationModeDRA, "gpu-node")
	noRequest := hpaReadyPod("p", "gpu-node", "pod-uid")
	gpuNode, cpuNode := hpaAcceleratorNode("gpu-node", 1), hpaAcceleratorNode("gpu-node", 0)
	claim := func(uid types.UID, driver string, devices int, selectorNode string) []*resourcev1.ResourceClaim {
		return []*resourcev1.ResourceClaim{hpaAllocatedClaim("p-claim", uid, driver, devices, selectorNode)}
	}
	unallocated := claim("pod-uid", cfg.DRADriver, 1, "")
	unallocated[0].Status = resourcev1.ResourceClaimStatus{}
	ownedBy := func(c *resourcev1.ResourceClaim, uid types.UID) *resourcev1.ResourceClaim {
		c.OwnerReferences = nil
		if uid != "" {
			c.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "other", UID: uid, Controller: hpaPtr(true)}}
		}
		return c
	}
	sharedWith := func(c *resourcev1.ResourceClaim, uid types.UID) *resourcev1.ResourceClaim {
		c.Status.ReservedFor = append(c.Status.ReservedFor, resourcev1.ResourceClaimConsumerReference{Resource: "pods", Name: "other", UID: uid})
		return c
	}

	tests := []struct {
		name    string
		pod     corev1.Pod
		node    *corev1.Node
		claims  []*resourcev1.ResourceClaim
		mode    string
		wantErr string
	}{
		{name: "device plugin on accelerator node", pod: dp, node: gpuNode, mode: allocationModeDevicePlugin},
		{name: "device plugin on node without the resource", pod: dp, node: cpuNode, mode: allocationModeDevicePlugin, wantErr: "allocatable nvidia.com/gpu=0"},
		{name: "device plugin Pod without request", pod: noRequest, node: gpuNode, mode: allocationModeDevicePlugin, wantErr: "does not request"},
		{name: "DRA allocated and reserved", pod: dra, node: cpuNode, mode: allocationModeDRA, claims: claim("pod-uid", cfg.DRADriver, 1, "gpu-node")},
		{name: "DRA Pod without claim", pod: noRequest, node: gpuNode, mode: allocationModeDRA, wantErr: "does not request"},
		{name: "DRA claim missing from status", pod: dra, node: gpuNode, mode: allocationModeDRA, wantErr: "0 generated ResourceClaims"},
		{name: "DRA claim not allocated", pod: dra, node: gpuNode, mode: allocationModeDRA,
			claims: unallocated, wantErr: "not allocated"},
		// Replicas must not share one claim (one device).
		{name: "DRA claim generated for another Pod", pod: dra, node: gpuNode, mode: allocationModeDRA,
			claims: []*resourcev1.ResourceClaim{ownedBy(claim("pod-uid", cfg.DRADriver, 1, "")[0], "other-uid")}, wantErr: "was not generated for Pod"},
		{name: "DRA claim without a controller owner", pod: dra, node: gpuNode, mode: allocationModeDRA,
			claims: []*resourcev1.ResourceClaim{ownedBy(claim("pod-uid", cfg.DRADriver, 1, "")[0], "")}, wantErr: "was not generated for Pod"},
		{name: "DRA claim shared with another Pod", pod: dra, node: gpuNode, mode: allocationModeDRA,
			claims: []*resourcev1.ResourceClaim{sharedWith(claim("pod-uid", cfg.DRADriver, 1, "")[0], "other-uid")}, wantErr: "want only Pod"},
		{name: "DRA claim reserved for another Pod", pod: dra, node: gpuNode, mode: allocationModeDRA, claims: []*resourcev1.ResourceClaim{func() *resourcev1.ResourceClaim {
			c := claim("pod-uid", cfg.DRADriver, 1, "")[0]
			c.Status.ReservedFor[0].UID = "other-uid"
			return c
		}()}, wantErr: "want only Pod"},
		{name: "DRA device from another driver", pod: dra, node: gpuNode, mode: allocationModeDRA, claims: claim("pod-uid", "other.example.com", 1, ""), wantErr: "from driver other.example.com"},
		{name: "DRA no devices for request", pod: dra, node: gpuNode, mode: allocationModeDRA, claims: claim("pod-uid", cfg.DRADriver, 0, ""), wantErr: "allocated 0 devices"},
		{name: "DRA too many devices", pod: dra, node: gpuNode, mode: allocationModeDRA, claims: claim("pod-uid", cfg.DRADriver, 2, ""), wantErr: "allocated 2 devices"},
		{name: "DRA Pod on node outside allocation", pod: dra, node: gpuNode, mode: allocationModeDRA, claims: claim("pod-uid", cfg.DRADriver, 1, "another-node"), wantErr: "outside the allocation node selector"},
		{name: "unknown mode", pod: dp, node: gpuNode, mode: "bogus", wantErr: "unknown allocation mode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := tt.pod
			err := checkHPAPodAccelerator(&pod, tt.node, tt.claims, tt.mode, cfg)
			wantHPAError(t, err, tt.wantErr)
		})
	}
}

func TestVerifyHPAPodsOnAccelerators(t *testing.T) {
	cfg := nvidiaConfig(t)
	ctx := context.Background()
	pod := hpaWorkloadPod(t, allocationModeDRA, "gpu-node")

	client := fake.NewClientset(hpaAcceleratorNode("gpu-node", 0), hpaAllocatedClaim("p-claim", "pod-uid", cfg.DRADriver, 1, "gpu-node"))
	if err := verifyHPAPodsOnAccelerators(ctx, client, []corev1.Pod{pod}, allocationModeDRA, cfg); err != nil {
		t.Fatalf("verifyHPAPodsOnAccelerators: %v", err)
	}
	missingClaim := fake.NewClientset(hpaAcceleratorNode("gpu-node", 0))
	if err := verifyHPAPodsOnAccelerators(ctx, missingClaim, []corev1.Pod{pod}, allocationModeDRA, cfg); err == nil || !strings.Contains(err.Error(), "get ResourceClaim") {
		t.Fatalf("missing ResourceClaim error = %v", err)
	}
	dp := hpaWorkloadPod(t, allocationModeDevicePlugin, "gone")
	if err := verifyHPAPodsOnAccelerators(ctx, fake.NewClientset(), []corev1.Pod{dp}, allocationModeDevicePlugin, cfg); err == nil || !strings.Contains(err.Error(), "get node gone") {
		t.Fatalf("missing node error = %v", err)
	}
}

// TestVerifyHPAPodsDoNotOvercommit covers what the per-Pod checks cannot see:
// the same accelerator counted for two replicas.
func TestVerifyHPAPodsDoNotOvercommit(t *testing.T) {
	cfg := nvidiaConfig(t)
	ctx := context.Background()
	draPods := func() []corev1.Pod {
		a, b := hpaWorkloadPod(t, allocationModeDRA, "gpu-node"), hpaWorkloadPod(t, allocationModeDRA, "gpu-node")
		a.Name, a.UID, a.Status.ResourceClaimStatuses[0].ResourceClaimName = "a", "uid-a", hpaPtr("a-claim")
		b.Name, b.UID, b.Status.ResourceClaimStatuses[0].ResourceClaimName = "b", "uid-b", hpaPtr("b-claim")
		return []corev1.Pod{a, b}
	}
	claims := func(deviceB string, shares bool) (*resourcev1.ResourceClaim, *resourcev1.ResourceClaim) {
		a := hpaAllocatedClaim("a-claim", "uid-a", cfg.DRADriver, 1, "")
		b := hpaAllocatedClaim("b-claim", "uid-b", cfg.DRADriver, 1, "")
		b.Status.Allocation.Devices.Results[0].Device = deviceB
		if shares {
			a.Status.Allocation.Devices.Results[0].ShareID = hpaPtr(types.UID("share-a"))
			b.Status.Allocation.Devices.Results[0].ShareID = hpaPtr(types.UID("share-b"))
		}
		return a, b
	}
	for _, tc := range []struct {
		name    string
		deviceB string
		shares  bool
		wantErr string
	}{
		{name: "distinct devices", deviceB: "gpu-1"},
		// A driver may allocate one device to several claims as separate
		// shares (allowMultipleAllocations); that is not a double allocation.
		{name: "separate shares of one device", deviceB: "gpu-0", shares: true},
		{name: "one device allocated twice", deviceB: "gpu-0", wantErr: "is allocated to both Pod a and Pod b"},
	} {
		t.Run("DRA "+tc.name, func(t *testing.T) {
			a, b := claims(tc.deviceB, tc.shares)
			c := fake.NewClientset(hpaAcceleratorNode("gpu-node", 0), a, b)
			err := verifyHPAPodsOnAccelerators(ctx, c, draPods(), allocationModeDRA, cfg)
			if (tc.wantErr == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}

	dp := func(name, node string) corev1.Pod {
		p := hpaWorkloadPod(t, allocationModeDevicePlugin, node)
		p.Name = name
		return p
	}
	t.Run("device plugin within node capacity", func(t *testing.T) {
		c := fake.NewClientset(hpaAcceleratorNode("n1", 1), hpaAcceleratorNode("n2", 2))
		if err := verifyHPAPodsOnAccelerators(ctx, c, []corev1.Pod{dp("a", "n1"), dp("b", "n2"), dp("c", "n2")}, allocationModeDevicePlugin, cfg); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("device plugin over node capacity", func(t *testing.T) {
		c := fake.NewClientset(hpaAcceleratorNode("n1", 1))
		err := verifyHPAPodsOnAccelerators(ctx, c, []corev1.Pod{dp("a", "n1"), dp("b", "n1")}, allocationModeDevicePlugin, cfg)
		if err == nil || !strings.Contains(err.Error(), "node n1 runs 2 of the test's Pods") {
			t.Fatalf("error = %v, want an overcommit error", err)
		}
	})

	t.Run("permission error is an environment error and ends the wait", func(t *testing.T) {
		fastHPATiming(t)
		c := fake.NewClientset(hpaAcceleratorNode("gpu-node", 1), &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: "ns"}})
		c.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, "gpu-node", errors.New("denied"))
		})
		s := newHPAScenario(c, "ns", allocationModeDevicePlugin, "v1beta2", cfg, testHPAConfig(t, time.Second), t.Logf)
		start := time.Now()
		_, err := s.waitForState(ctx, time.Second, func(ctx context.Context, _ hpaState) error {
			return verifyHPAPodsOnAccelerators(ctx, c, []corev1.Pod{dp("a", "gpu-node")}, allocationModeDevicePlugin, cfg)
		})
		if !isHPAEnvironmentError(err) || !apierrors.IsForbidden(err) || time.Since(start) > 500*time.Millisecond {
			t.Fatalf("error = %q after %s, want a prompt environment error wrapping Forbidden", hpaFailureMessage(err), time.Since(start))
		}
	})
}

func TestCheckHPAClaimsReleased(t *testing.T) {
	cfg := nvidiaConfig(t)
	live := *hpaAllocatedClaim("live", "live-uid", cfg.DRADriver, 1, "")
	released := resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: "released"}}
	held := *hpaAllocatedClaim("held", "removed-uid", cfg.DRADriver, 1, "")

	if err := checkHPAClaimsReleased([]resourcev1.ResourceClaim{live, released}, map[types.UID]bool{"live-uid": true}); err != nil {
		t.Errorf("live and deallocated claims rejected: %v", err)
	}
	if err := checkHPAClaimsReleased([]resourcev1.ResourceClaim{live, held}, map[types.UID]bool{"live-uid": true}); err == nil || !strings.Contains(err.Error(), "held (generated for Pod p)") {
		t.Errorf("claim of a removed Pod not reported: %v", err)
	}
	if err := checkHPAClaimsReleased([]resourcev1.ResourceClaim{live}, nil); err == nil {
		t.Error("an allocated claim passed with no live Pods")
	}
}

// simCluster stands in for the controllers TestHPAAutoscaling depends on
// (HPA, Deployment/ReplicaSet, scheduler and kubelet, DRA, the metrics
// pipeline) on top of a fake clientset. Each knob breaks one of them.
type simCluster struct {
	c    *fake.Clientset
	ns   string
	mode string
	cfg  AcceleratorConfig

	ignoreMetric      bool // the HPA never changes the replica count
	cannotScale       bool // the HPA decides on a replica count but cannot update the Deployment
	scaleAtBaseline   bool // the HPA scales up once the negative control starts, although the metric is below target
	capacity          int  // Pods that can be scheduled; the rest stay Unschedulable (negative = unlimited)
	metricsMissing    bool // the adapter never serves the metric
	hpaMetricsDenied  bool // the HPA cannot read the metric (e.g. RBAC), as reported by FailedGetPodsMetric
	newPodsUnscraped  bool // the pipeline never discovers Pods created after the first one
	pullFailure       bool // Pods after the first one cannot pull their image
	startupCheckFails bool // containers of Pods after the first one see no accelerator and crash-loop
	frozenMetrics     bool // the pipeline keeps reporting the first value it saw
	replaceOnShrink   bool // scaling down replaces every Pod instead of removing the extras
	admissionFailure  bool // Pods after the first one are rejected at admission (Phase=Failed), as when a device cannot be allocated
	evictOnScaleDown  bool // the node evicts one Pod (Phase=Failed, not deleted) once the metric is lowered
	shareFirstClaim   bool // DRA: Pods after the first one use the first Pod's ResourceClaim (one device)
	// disruptOnScaleDown evicts one Pod through the eviction API once the
	// metric is lowered, and the Pod then stays Terminating (e.g. its node
	// stopped responding) with its DRA claim still allocated.
	disruptOnScaleDown bool
	removal            simRemoval
	// The settle* knobs act while the test confirms the scaled-up state:
	// settleAPIError fails the next Node read with a retryable error, and
	// settleReplacePod deletes one scaled-up Pod once.
	settleAPIError   bool
	settleReplacePod bool
	maxReplicas      int
	settled          atomic.Bool
	nodeReadFails    atomic.Bool
	// recordedLag makes the HPA record the previous phase's average for this
	// long after each metric change, as a real HPA does until its next sync.
	recordedLag time.Duration
	// recordedStaleBelow makes the HPA keep recording the baseline's average
	// whenever the value is below target, while scaling on the current value.
	recordedStaleBelow bool

	mu          sync.Mutex
	seq         int
	recorded    int                // value the HPA last recorded
	recordedAt  time.Time          // when the HPA last recorded a value
	staleBelow  int                // first below-target value recorded
	peakLive    int                // most non-terminating Pods ever running at once
	raisedPods  map[string]bool    // Pods whose raised metric (100) the adapter served
	createdUIDs map[types.UID]bool // UIDs of every Pod created
	misbehaving bool
	frozen      string
	evicted     bool
	lastCreate  time.Time
}

func newSimCluster(t *testing.T, mode string) *simCluster {
	t.Helper()
	k := &simCluster{
		c:        fake.NewClientset(hpaAcceleratorNode("gpu-node", 8)),
		ns:       "ns",
		mode:     mode,
		cfg:      nvidiaConfig(t),
		capacity: -1,
	}
	k.c.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if k.nodeReadFails.CompareAndSwap(true, false) {
			return true, nil, apierrors.NewServiceUnavailable("simulated transient API error")
		}
		return false, nil, nil
	})
	return k
}

// simRemoval is how the simulated cluster removes a Pod.
type simRemoval int

const (
	removeCleanly          simRemoval = iota
	removeStuckTerminating            // the Pod never finishes terminating
	removeLeftFailed                  // scale-down leaves the Pod object behind as Failed
	removeKeepClaims                  // the Pod's DRA claim stays allocated
)

// start runs the simulated controllers until the test ends.
func (k *simCluster) start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			k.step(ctx)
			time.Sleep(time.Millisecond)
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
}

func (k *simCluster) metricValue(ctx context.Context) string {
	cm, err := k.c.CoreV1().ConfigMaps(k.ns).Get(ctx, hpaWorkloadName, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return cm.Data[hpaMetricValueKey]
}

func (k *simCluster) workloadPods(ctx context.Context) (live, all []corev1.Pod) {
	list, err := k.c.CoreV1().Pods(k.ns).List(ctx, metav1.ListOptions{LabelSelector: hpaWorkloadSelector()})
	if err != nil {
		return nil, nil
	}
	for _, pod := range list.Items {
		if pod.DeletionTimestamp == nil && pod.Status.Phase != corev1.PodFailed {
			live = append(live, pod)
		}
	}
	return live, list.Items
}

func (k *simCluster) step(ctx context.Context) {
	k.mu.Lock()
	defer k.mu.Unlock()
	live, all := k.workloadPods(ctx)
	d, err := k.c.AppsV1().Deployments(k.ns).Get(ctx, hpaWorkloadName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		for i := range all {
			k.removePod(ctx, &all[i], true)
		}
		return
	}
	if err != nil {
		return
	}

	if hpa, err := k.c.AutoscalingV2().HorizontalPodAutoscalers(k.ns).Get(ctx, hpaWorkloadName, metav1.GetOptions{}); err == nil {
		if v, err := strconv.Atoi(k.metricValue(ctx)); err == nil {
			k.reconcileHPA(ctx, hpa, d, live, v)
		}
	}

	if n := len(live); n > k.peakLive {
		k.peakLive = n
	}
	if k.disruptOnScaleDown && !k.evicted && k.metricValue(ctx) == strconv.Itoa(hpaScaleDownValue) && len(live) > 1 {
		k.evicted = true
		pod := &live[0]
		pod.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		pod.Finalizers = []string{"example.com/node-unreachable"}
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: "EvictionByEvictionAPI",
			Message: "Eviction API: evicting",
		})
		_, _ = k.c.CoreV1().Pods(k.ns).Update(ctx, pod, metav1.UpdateOptions{})
		live = live[1:]
	}
	if k.evictOnScaleDown && !k.evicted && k.metricValue(ctx) == strconv.Itoa(hpaScaleDownValue) && len(live) > 1 {
		k.evicted = true
		k.evict(ctx, &live[0])
		live = live[1:]
	}
	want := int(hpaReplicaCount(d.Spec.Replicas))
	if k.replaceOnShrink && len(live) > want {
		for i := range live {
			k.removePod(ctx, &live[i], false)
		}
		live = nil
	}
	for len(live) < want {
		if k.admissionFailure && time.Since(k.lastCreate) < 10*time.Millisecond {
			break // like a ReplicaSet's backoff, so rejected Pods do not pile up
		}
		k.lastCreate = time.Now()
		pod := k.createPod(ctx, d, len(live))
		if pod.Status.Phase == corev1.PodFailed {
			break
		}
		live = append(live, pod)
	}
	for len(live) > want {
		k.removePod(ctx, &live[len(live)-1], false)
		live = live[:len(live)-1]
	}
	ready := int32(0)
	for i := range live {
		if podIsReady(&live[i]) {
			ready++
		}
	}
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: int32(len(live)), UpdatedReplicas: int32(len(live)), ReadyReplicas: ready}
	_, _ = k.c.AppsV1().Deployments(k.ns).UpdateStatus(ctx, d, metav1.UpdateOptions{})
}

func (k *simCluster) reconcileHPA(ctx context.Context, hpa *autoscalingv2.HorizontalPodAutoscaler, d *appsv1.Deployment, live []corev1.Pod, value int) {
	current := hpaReplicaCount(d.Spec.Replicas)
	if k.hpaMetricsDenied {
		hpa.Status = autoscalingv2.HorizontalPodAutoscalerStatus{
			CurrentReplicas: int32(len(live)),
			DesiredReplicas: current,
			Conditions: []autoscalingv2.HorizontalPodAutoscalerCondition{{
				Type: autoscalingv2.ScalingActive, Status: corev1.ConditionFalse, Reason: "FailedGetPodsMetric",
				Message: "the HPA was unable to compute the replica count: unable to get metric m: forbidden",
			}},
		}
		_, _ = k.c.AutoscalingV2().HorizontalPodAutoscalers(k.ns).UpdateStatus(ctx, hpa, metav1.UpdateOptions{})
		return
	}
	desired := current
	switch {
	case k.misbehaving:
		desired = hpa.Spec.MaxReplicas
	case k.ignoreMetric:
	default:
		n := max(len(live), 1)
		desired = int32(math.Ceil(float64(n*value) / hpaTargetValue))
		desired = min(max(desired, hpaReplicaCount(hpa.Spec.MinReplicas)), hpa.Spec.MaxReplicas)
	}
	if desired != current && !k.cannotScale {
		d.Spec.Replicas = hpaPtr(desired)
		if updated, err := k.c.AppsV1().Deployments(k.ns).Update(ctx, d, metav1.UpdateOptions{}); err == nil {
			*d = *updated
		}
	}
	switch {
	case k.recordedAt.IsZero():
		k.recorded, k.recordedAt = value, time.Now()
	case k.recordedStaleBelow && value < hpaTargetValue && k.staleBelow != 0:
		k.recorded = k.staleBelow
	case value != k.recorded && time.Since(k.recordedAt) >= k.recordedLag:
		k.recorded, k.recordedAt = value, time.Now()
	case value != k.recorded:
		// still lagging
	default:
		k.recordedAt = time.Now()
	}
	if k.staleBelow == 0 && k.recorded < hpaTargetValue {
		k.staleBelow = k.recorded
	}
	average := resource.MustParse(strconv.Itoa(k.recorded))
	status := hpaStatusWith(int32(len(live)), "0")
	status.ObservedGeneration = nil // as released HPA controllers leave it
	status.DesiredReplicas = desired
	status.CurrentMetrics[0].Pods.Metric.Name = hpa.Spec.Metrics[0].Pods.Metric.Name
	status.CurrentMetrics[0].Pods.Current.AverageValue = &average
	hpa.Status = status
	_, _ = k.c.AutoscalingV2().HorizontalPodAutoscalers(k.ns).UpdateStatus(ctx, hpa, metav1.UpdateOptions{})
}

func (k *simCluster) createPod(ctx context.Context, d *appsv1.Deployment, index int) corev1.Pod {
	k.seq++
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%d", hpaWorkloadName, k.seq), Namespace: k.ns,
			UID: types.UID(fmt.Sprintf("uid-%d", k.seq)), Labels: d.Spec.Template.Labels,
		},
		Spec: *d.Spec.Template.Spec.DeepCopy(),
	}
	switch {
	case k.admissionFailure && index >= 1:
		pod.Spec.NodeName = "gpu-node"
		pod.Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "UnexpectedAdmissionError",
			Message: "Allocate failed due to requested number of devices unavailable for nvidia.com/gpu. Requested: 1, Available: 0, which is unexpected"}
	case k.startupCheckFails && index >= 1:
		pod.Spec.NodeName = "gpu-node"
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name:         "workload",
			RestartCount: 2,
			State:        corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, Reason: "Error", Message: "RESULT: ACCELERATOR_COUNT=0\nexpected 1 accelerator device(s), found 0\n",
			}},
		}}}
	case k.pullFailure && index >= 1:
		pod.Spec.NodeName = "gpu-node"
		pod.Status = corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{
			Name:  "workload",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Back-off pulling image"}},
		}}}
	case k.capacity >= 0 && index >= k.capacity:
		pod.Status = corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
			Message: "0/1 nodes are available: 1 Insufficient nvidia.com/gpu.",
		}}}
	default:
		pod.Spec.NodeName = "gpu-node"
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		if k.mode == allocationModeDRA {
			if k.shareFirstClaim && index >= 1 {
				pod.Status.ResourceClaimStatuses = []corev1.PodResourceClaimStatus{{Name: "claim", ResourceClaimName: hpaPtr(hpaWorkloadName + "-1-claim")}}
				break
			}
			claim := hpaAllocatedClaim(pod.Name+"-claim", pod.UID, k.cfg.DRADriver, 1, "gpu-node")
			claim.Namespace = k.ns
			claim.Status.Allocation.Devices.Results[0].Device = fmt.Sprintf("gpu-%d", k.seq) // one device per Pod
			_, _ = k.c.ResourceV1().ResourceClaims(k.ns).Create(ctx, claim, metav1.CreateOptions{})
			pod.Status.ResourceClaimStatuses = []corev1.PodResourceClaimStatus{{Name: "claim", ResourceClaimName: hpaPtr(claim.Name)}}
		}
	}
	if k.createdUIDs == nil {
		k.createdUIDs = map[types.UID]bool{}
	}
	k.createdUIDs[pod.UID] = true
	_, _ = k.c.CoreV1().Pods(k.ns).Create(ctx, &pod, metav1.CreateOptions{})
	return pod
}

// evict ends a Pod as node-pressure eviction does: the Pod object stays, in
// Phase=Failed, and its DRA claim is deallocated.
func (k *simCluster) evict(ctx context.Context, pod *corev1.Pod) {
	pod.Status.Phase, pod.Status.Reason, pod.Status.Message = corev1.PodFailed, "Evicted", "The node was low on resource: memory."
	pod.Status.Conditions = nil
	_, _ = k.c.CoreV1().Pods(k.ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	if claim, err := k.c.ResourceV1().ResourceClaims(k.ns).Get(ctx, pod.Name+"-claim", metav1.GetOptions{}); err == nil {
		claim.Status = resourcev1.ResourceClaimStatus{}
		_, _ = k.c.ResourceV1().ResourceClaims(k.ns).UpdateStatus(ctx, claim, metav1.UpdateOptions{})
	}
}

// removePod deletes a Pod as scale-down (gc=false) or Deployment garbage
// collection (gc=true) would, subject to the removal knob.
func (k *simCluster) removePod(ctx context.Context, pod *corev1.Pod, gc bool) {
	switch {
	case k.removal == removeStuckTerminating:
		if pod.DeletionTimestamp == nil {
			pod.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			pod.Finalizers = []string{"example.com/stuck"}
			_, _ = k.c.CoreV1().Pods(k.ns).Update(ctx, pod, metav1.UpdateOptions{})
		}
		return
	case k.removal == removeLeftFailed && !gc:
		pod.Status.Phase = corev1.PodFailed
		_, _ = k.c.CoreV1().Pods(k.ns).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
		return
	}
	_ = k.c.CoreV1().Pods(k.ns).Delete(ctx, pod.Name, metav1.DeleteOptions{})
	if k.removal != removeKeepClaims {
		_ = k.c.ResourceV1().ResourceClaims(k.ns).Delete(ctx, pod.Name+"-claim", metav1.DeleteOptions{})
	}
}

// checkCompletedCycle confirms, from what the simulated cluster saw, that a
// passing run really scaled up and back down.
func (k *simCluster) checkCompletedCycle(t *testing.T, maxReplicas int) {
	t.Helper()
	ctx := context.Background()
	k.mu.Lock()
	peak, raised, created := k.peakLive, len(k.raisedPods), len(k.createdUIDs)
	k.mu.Unlock()
	if peak != maxReplicas || created < maxReplicas || raised < maxReplicas {
		t.Errorf("peak running Pods=%d, distinct Pods created=%d, Pods served the raised metric=%d; want %d each", peak, created, raised, maxReplicas)
	}
	live, all := k.workloadPods(ctx)
	wantAll := 1
	if k.evicted {
		wantAll++ // the evicted Pod object stays until something deletes it
	}
	if len(live) != 1 || len(all) != wantAll {
		t.Errorf("after the run: %d live and %d total workload Pods, want 1 and %d", len(live), len(all), wantAll)
	}
	claims, err := k.c.ResourceV1().ResourceClaims(k.ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	allocated := 0
	for _, c := range claims.Items {
		if c.Status.Allocation != nil {
			allocated++
		}
	}
	want := map[bool]int{true: 1, false: 0}[k.mode == allocationModeDRA]
	if k.disruptOnScaleDown {
		want *= 2 // the disrupted Pod is still terminating and keeps its claim
	}
	if allocated != want {
		t.Errorf("after the run: %d allocated ResourceClaims, want %d", allocated, want)
	}
}

// logf passes scenario logs to t and, with scaleAtBaseline, makes the HPA
// scale up exactly when the negative-control window opens.
func (k *simCluster) logf(t *testing.T) func(string, ...any) {
	return func(format string, args ...any) {
		t.Logf(format, args...)
		if !k.scaleAtBaseline || !strings.HasPrefix(format, hpaNegativeControlLog) {
			return
		}
		k.mu.Lock()
		defer k.mu.Unlock()
		k.misbehaving = true
		ctx := context.Background()
		d, err := k.c.AppsV1().Deployments(k.ns).Get(ctx, hpaWorkloadName, metav1.GetOptions{})
		if err != nil {
			t.Errorf("simulated HPA could not read the Deployment: %v", err)
			return
		}
		hpa, err := k.c.AutoscalingV2().HorizontalPodAutoscalers(k.ns).Get(ctx, hpaWorkloadName, metav1.GetOptions{})
		if err != nil {
			t.Errorf("simulated HPA could not read itself: %v", err)
			return
		}
		d.Spec.Replicas = hpaPtr(hpa.Spec.MaxReplicas)
		if _, err := k.c.AppsV1().Deployments(k.ns).Update(ctx, d, metav1.UpdateOptions{}); err != nil {
			t.Errorf("simulated HPA could not scale the Deployment: %v", err)
		}
	}
}

// readMetrics plays the custom metrics adapter: every workload Pod reports the
// ConfigMap value.
func (k *simCluster) readMetrics(ctx context.Context, namespace, metricName string) ([]byte, error) {
	if k.metricsMissing {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "custom.metrics.k8s.io", Resource: "pods"}, metricName)
	}
	k.mu.Lock()
	value := k.metricValue(ctx)
	if k.frozenMetrics {
		if k.frozen == "" {
			k.frozen = value
		}
		value = k.frozen
	}
	k.mu.Unlock()
	_, all := k.workloadPods(ctx)
	var items []string
	for _, pod := range all {
		if k.newPodsUnscraped && pod.UID != "uid-1" {
			continue
		}
		if value == strconv.Itoa(hpaScaleUpValue) {
			k.mu.Lock()
			if k.raisedPods == nil {
				k.raisedPods = map[string]bool{}
			}
			k.raisedPods[pod.Name] = true
			k.mu.Unlock()
		}
		items = append(items, hpaMetricItem("Pod", namespace, pod.Name, metricName, value, false))
	}
	if value == strconv.Itoa(hpaScaleUpValue) && k.maxReplicas > 0 && len(items) == k.maxReplicas {
		if k.settleAPIError {
			k.nodeReadFails.Store(true)
		}
		if k.settleReplacePod && k.settled.CompareAndSwap(false, true) {
			// The adapter still serves the evicted Pod's last sample, as
			// Prometheus would until the series goes stale.
			_ = k.c.CoreV1().Pods(namespace).Delete(ctx, all[len(all)-1].Name, metav1.DeleteOptions{})
			_ = k.c.ResourceV1().ResourceClaims(namespace).Delete(ctx, all[len(all)-1].Name+"-claim", metav1.DeleteOptions{})
		}
	}
	return hpaMetricList(items...), nil
}

func TestHPAScenario(t *testing.T) {
	fastHPATiming(t)
	tests := []struct {
		name    string
		mode    string
		breakIt func(*simCluster)
		// wantEnv is whether the failure must be classified as an
		// environment error rather than a conformance failure.
		wantEnv         bool
		wantErr         string
		wantCleanupFail bool
		// maxReplicas is the HPA's maxReplicas (default 2).
		maxReplicas int
	}{
		{name: "device plugin passes", mode: allocationModeDevicePlugin},
		{name: "DRA passes", mode: allocationModeDRA},
		{name: "device plugin passes with three replicas", mode: allocationModeDevicePlugin, maxReplicas: 3},
		{name: "DRA passes with three replicas", mode: allocationModeDRA, maxReplicas: 3},
		{name: "capacity for only two of three replicas", mode: allocationModeDRA, maxReplicas: 3,
			breakIt: func(k *simCluster) { k.capacity = 2 }, wantEnv: true, wantErr: "is Unschedulable"},
		{name: "HPA ignores the metric", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.ignoreMetric = true }, wantErr: "did not scale the Deployment to 2"},
		{name: "HPA cannot update the Deployment scale", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.cannotScale = true }, wantErr: "did not scale the Deployment to 2"},
		{name: "test resources cannot be created", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) {
				k.c.PrependReactor("create", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, hpaWorkloadName, errors.New("denied"))
				})
			}, wantEnv: true, wantErr: "create metric ConfigMap"},
		// Area A regressions: normal events while the scaled-up state is
		// being confirmed must not fail a conforming HPA.
		{name: "transient API error while confirming the scale-up", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.settleAPIError = true }},
		{name: "scaled-up Pod replaced while confirming the scale-up", mode: allocationModeDRA, maxReplicas: 3,
			breakIt: func(k *simCluster) { k.settleReplacePod = true }},
		{name: "HPA records each new average after a delay", mode: allocationModeDRA,
			breakIt: func(k *simCluster) { k.recordedLag = 100 * time.Millisecond }},
		// The recorded average must be the current phase's value.
		{name: "HPA records another phase's below-target average", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.recordedStaleBelow = true }, wantErr: "recorded average 1 for \"m\", want the current value 2"},
		{name: "HPA controller cannot read the metric", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.hpaMetricsDenied = true }, wantEnv: true, wantErr: "FailedGetPodsMetric"},
		{name: "pipeline misses the scaled-up Pods", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.newPodsUnscraped = true }, wantEnv: true, wantErr: "no sample for Pod"},
		// The scaled-up Pods' own startup check fails: not an HPA failure.
		{name: "scaled-up Pods fail their accelerator startup check", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.startupCheckFails = true }, wantEnv: true, wantErr: "exited with code 1 (Error), restarts=2; output: RESULT: ACCELERATOR_COUNT=0 expected 1 accelerator device(s), found 0"},
		{name: "scaled-up Pods cannot pull the image", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.pullFailure = true }, wantEnv: true, wantErr: "ImagePullBackOff"},
		{name: "HPA scales with the metric below target", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.scaleAtBaseline = true }, wantErr: "did not hold 1 replica"},
		{name: "insufficient accelerator capacity", mode: allocationModeDRA,
			breakIt: func(k *simCluster) { k.capacity = 1 }, wantEnv: true, wantErr: "is Unschedulable: 0/1 nodes are available: 1 Insufficient nvidia.com/gpu."},
		{name: "no accelerator capacity at all", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.capacity = 0 }, wantEnv: true, wantErr: "baseline accelerator Pod did not become Ready"},
		{name: "metric never served", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.metricsMissing = true }, wantEnv: true, wantErr: "does not serve the workload metric"},
		{name: "metric updates never propagate", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.frozenMetrics = true }, wantEnv: true, wantErr: "did not report the raised metric"},
		{name: "removed Pods stay terminating", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.removal = removeStuckTerminating }, wantErr: "(Terminating)", wantCleanupFail: true},
		// A Pod left behind without being deleted no longer counts as a
		// removed replica; under DRA its still-allocated claim fails the run.
		{name: "removed Pods are left behind as Failed, holding their claims", mode: allocationModeDRA,
			breakIt: func(k *simCluster) { k.removal = removeLeftFailed }, wantErr: "still allocated for removed Pods: hpa-workload-2-claim"},
		// Pod lifecycle events outside autoscaling must not fail a
		// conforming HPA.
		{name: "scale-down replaces Pods instead of removing them", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.replaceOnShrink = true }},
		{name: "node evicts a Pod during scale-down (DRA)", mode: allocationModeDRA,
			breakIt: func(k *simCluster) { k.evictOnScaleDown = true }},
		{name: "node evicts a Pod during scale-down (device plugin)", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.evictOnScaleDown = true }},
		{name: "evicted Pod still terminating after scale-down (DRA)", mode: allocationModeDRA,
			breakIt: func(k *simCluster) { k.disruptOnScaleDown = true }},
		{name: "evicted Pod still terminating after scale-down (device plugin)", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.disruptOnScaleDown = true }},
		// Pods the node rejected are terminal, but still an environment
		// problem.
		{name: "scaled-up Pods share one ResourceClaim", mode: allocationModeDRA,
			breakIt: func(k *simCluster) { k.shareFirstClaim = true }, wantErr: "ResourceClaim hpa-workload-1-claim was not generated for Pod hpa-workload-2"},
		{name: "scaled-up Pods rejected at admission", mode: allocationModeDevicePlugin,
			breakIt: func(k *simCluster) { k.admissionFailure = true }, wantEnv: true, wantErr: "failed (UnexpectedAdmissionError: Allocate failed"},
		{name: "removed Pods keep their DRA claims", mode: allocationModeDRA,
			breakIt: func(k *simCluster) { k.removal = removeKeepClaims }, wantErr: "still allocated for removed Pods: hpa-workload-2-claim (generated for Pod p)", wantCleanupFail: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Each case has its own fake cluster; the shared timing variables
			// are only read until the parent test's cleanup restores them.
			t.Parallel()
			k := newSimCluster(t, tt.mode)
			timeout := 3 * time.Second
			if tt.wantErr != "" {
				timeout = time.Second
			}
			if tt.breakIt != nil {
				tt.breakIt(k)
			}
			maxReplicas := max(tt.maxReplicas, 2)
			k.maxReplicas = maxReplicas
			s := newHPAScenario(k.c, k.ns, tt.mode, "v1beta2", k.cfg, testHPAConfigN(t, timeout, maxReplicas), k.logf(t))
			s.readMetrics = k.readMetrics
			k.start(t)

			err := s.run(context.Background())
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("run failed: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("run error = %v, want it to contain %q", err, tt.wantErr)
			case tt.wantErr != "" && isHPAEnvironmentError(err) != tt.wantEnv:
				t.Fatalf("run error %q classified as environment=%t, want %t", hpaFailureMessage(err), isHPAEnvironmentError(err), tt.wantEnv)
			}
			if tt.wantErr == "" {
				k.checkCompletedCycle(t, maxReplicas)
			}

			cleanupErr := s.cleanup(context.Background())
			if (cleanupErr != nil) != tt.wantCleanupFail {
				t.Fatalf("cleanup error = %v, want failure %t", cleanupErr, tt.wantCleanupFail)
			}
			if !tt.wantCleanupFail {
				_, all := k.workloadPods(context.Background())
				claims, _ := k.c.ResourceV1().ResourceClaims(k.ns).List(context.Background(), metav1.ListOptions{})
				if len(all) > 0 || len(claims.Items) > 0 {
					t.Fatalf("cleanup left %d Pods and %d ResourceClaims", len(all), len(claims.Items))
				}
			}
		})
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
