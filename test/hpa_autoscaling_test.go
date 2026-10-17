package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
)

// Custom-metrics integration contract for TestHPAAutoscaling (see
// test/README.md). The platform's monitoring pipeline must scrape Pods
// carrying hpaMetricSourceLabel on the port named hpaMetricsPortName at
// hpaMetricsPath, and its custom metrics adapter must serve the
// hpaMetricSeries gauge as a per-Pod metric (custom.metrics.k8s.io, resource
// "pods") named -hpa-custom-metric-name.
//
// The gauge stands in for an inference server's per-replica request queue
// depth, a common signal for scaling accelerator-backed inference. The test
// sets it directly instead of generating load: it checks that the HPA acts
// on such a signal for accelerator Pods, not inference performance.
const (
	hpaMetricSourceLabel = "ai-conformance.kubernetes.io/hpa-metric-source"
	hpaAppLabelKey       = "app.kubernetes.io/name"
	hpaWorkloadName      = "hpa-workload"
	hpaMetricSeries      = "ai_conformance_hpa_queue_depth"
	hpaMetricsPortName   = "metrics"
	hpaMetricsPort       = 8080
	hpaMetricsPath       = "/metrics"
	hpaMetricMountPath   = "/etc/hpa-metric"
	hpaMetricValueKey    = "value"
	hpaMinReplicas       = 1

	// Queued requests per replica. Each phase uses a distinct value, and no
	// Pod can expose a value before the test writes it to the ConfigMap, so a
	// sample carrying the current phase's value was produced after that
	// write; samples from an earlier phase are rejected. Adapter timestamps
	// are not checked: adapters differ in what they report (prometheus-adapter
	// reports query time) and the test machine's clock is not comparable to
	// the cluster's.
	hpaTargetValue    = 10
	hpaBaselineValue  = 1
	hpaScaleUpValue   = 100
	hpaScaleDownValue = 2
)

// hpaNegativeControlLog starts the log line that opens the negative-control
// window (the unit-test simulator keys on it).
const hpaNegativeControlLog = "Negative control"

// Overridden by unit tests.
var (
	hpaPollInterval = 5 * time.Second
	// hpaNegativeControlWindow covers several HPA sync periods (15s by
	// default) during which the HPA must not scale on a below-target metric.
	hpaNegativeControlWindow = time.Minute
	hpaCleanupTimeout        = 5 * time.Minute
	// hpaObservationGrace is how long the test may go without a complete
	// observation of the workload (because API requests fail) before it can
	// no longer judge the HPA: a wait that ends after such a gap, or a
	// negative-control window containing one, is an environment error rather
	// than a conformance failure.
	hpaObservationGrace = time.Minute
)

const hpaCustomMetricsGroup = "custom.metrics.k8s.io"

// hpaCustomMetricsVersions are the custom metrics API versions the HPA
// controller's client supports.
var hpaCustomMetricsVersions = []string{"v1beta2", "v1beta1"}

var (
	hpaCustomMetricName       *string
	hpaMaxReplicas            *int
	hpaPodAnnotations         *string
	hpaWorkloadImage          *string
	hpaMetricTimeout          *time.Duration
	hpaScaleUpTimeout         *time.Duration
	hpaScaleDownTimeout       *time.Duration
	hpaScaleDownStabilization *time.Duration
)

func init() {
	registerFlagGroup("hpa", "HPA Autoscaling Flags (TestHPAAutoscaling)")
	hpaCustomMetricName = flag.String("hpa-custom-metric-name", "",
		"Name under which the platform's custom metrics API (custom.metrics.k8s.io) serves the test workload's "+hpaMetricSeries+" gauge as a per-Pod metric. The test is skipped when unset; see test/README.md for the full pipeline contract.")
	hpaMaxReplicas = flag.Int("hpa-max-replicas", 2,
		"HPA maxReplicas. The cluster must be able to allocate this many accelerators to the test at once. Must be at least 2.")
	hpaPodAnnotations = flag.String("hpa-pod-annotations", "",
		"Comma-separated key=value annotations added to the test Pods so the platform's monitoring pipeline discovers them (e.g. prometheus.io/scrape=true).")
	hpaWorkloadImage = flag.String("hpa-workload-image", "docker.io/library/python:3.13-alpine",
		"Image for the test workload; it must provide /bin/sh and python3. Override to use a mirror.")
	hpaMetricTimeout = flag.Duration("hpa-metric-timeout", 5*time.Minute,
		"How long to wait for a metric change to be reported by the custom metrics API for every expected Pod, and for the HPA to first act on the metric.")
	hpaScaleUpTimeout = flag.Duration("hpa-scale-up-timeout", 10*time.Minute,
		"How long to wait for the baseline Pod to become Ready, and for the HPA to scale up with every new accelerator Pod Ready.")
	hpaScaleDownTimeout = flag.Duration("hpa-scale-down-timeout", 10*time.Minute,
		"How long to wait for the HPA to scale back down and the removed Pods to be deleted (and, with DRA, their ResourceClaims deallocated), including the stabilization window.")
	hpaScaleDownStabilization = flag.Duration("hpa-scale-down-stabilization", time.Minute,
		"Scale-down stabilization window set on the test HPA (behavior.scaleDown.stabilizationWindowSeconds). Must be shorter than -hpa-scale-down-timeout.")
}

// TestHPAAutoscaling verifies that the HorizontalPodAutoscaler scales
// accelerator-backed Pods up and back down on a custom per-Pod metric.
// Ref: https://github.com/kubernetes-sigs/ai-conformance/tree/main/kars/0057-hpa-autoscaling-for-ai-workloads
func TestHPAAutoscaling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster E2E test in short mode")
	}
	if !flag.Parsed() {
		flag.Parse()
	}
	if *hpaCustomMetricName == "" {
		t.Skip("HPA autoscaling test is not configured; if the platform does not support the HorizontalPodAutoscaler with custom metrics, mark pod_autoscaling N/A, otherwise configure the custom metrics pipeline described in test/README.md and set -hpa-custom-metric-name")
	}
	hc, err := newHPAConfig(*hpaCustomMetricName, *hpaMaxReplicas, *hpaPodAnnotations, *hpaWorkloadImage,
		*hpaMetricTimeout, *hpaScaleUpTimeout, *hpaScaleDownTimeout, *hpaScaleDownStabilization)
	if err != nil {
		t.Fatalf("Invalid HPA flags: %v", err)
	}
	cfg, err := lookupAcceleratorConfig(*acceleratorType)
	if err != nil {
		t.Fatalf("Invalid -accelerator-type: %v", err)
	}
	deadline, hasDeadline := t.Deadline()
	runCtx, cancel, err := hpaRunContext(time.Now(), deadline, hasDeadline)
	if err != nil {
		t.Fatalf("Invalid -timeout: %v", err)
	}
	t.Cleanup(cancel)

	ctx := context.Background()
	clientset := getClientset(t)
	metricsVersion, err := resolveCustomMetricsVersion(clientset)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: %v", err)
	}
	mode, _, err := detectAllocationMode(ctx, clientset, *allocationMode, cfg, t.Logf)
	if err != nil {
		t.Fatalf("ENVIRONMENT ERROR: Failed to resolve allocation mode: %v", err)
	}
	t.Logf("HPA autoscaling test running with allocation mode %s, metric %q (%s/%s), maxReplicas %d", mode, hc.metricName, hpaCustomMetricsGroup, metricsVersion, hc.maxReplicas)

	namespace := randomNamespaceName("hpa-autoscaling")
	t.Cleanup(func() {
		if err := deleteNamespaceAndWait(ctx, t, clientset, namespace); err != nil {
			t.Errorf("CLEANUP FAILURE: %v. Please ensure this namespace is terminated manually to avoid resource leaks.", err)
		}
	})
	setupTestEnvironment(ctx, t, clientset, namespace, mode, cfg)

	s := newHPAScenario(clientset, namespace, mode, metricsVersion, cfg, hc, t.Logf)
	// Registered after the namespace cleanup, so it runs first.
	t.Cleanup(func() {
		if err := s.cleanup(context.Background()); err != nil {
			t.Errorf("CLEANUP FAILURE: %v", err)
		}
	})
	if err := s.run(runCtx); err != nil {
		t.Fatal(hpaRunFailureMessage(runCtx, err))
	}
}

// hpaRunFailureMessage reports a failed run. When the run context expired,
// the go test -timeout cut the test short, whatever phase was waiting.
func hpaRunFailureMessage(runCtx context.Context, err error) string {
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return hpaFailureMessage(hpaEnvironmentError(fmt.Errorf("the go test -timeout was reached before the test finished (raise it; see test/README.md): %w", err)))
	}
	return hpaFailureMessage(err)
}

// hpaCleanupReserve is kept free at the end of the go test -timeout for the
// test's cleanup: s.cleanup (at most hpaCleanupTimeout) and namespace
// deletion (deleteNamespaceAndWait, at most two one-minute polls). When the timeout itself
// expires, go test panics without running cleanups, leaving the workload and
// its accelerators allocated.
const hpaCleanupReserve = 10 * time.Minute

// hpaRunContext bounds the scenario so that it stops, and its cleanups run,
// hpaCleanupReserve before the go test deadline.
func hpaRunContext(now, deadline time.Time, hasDeadline bool) (context.Context, context.CancelFunc, error) {
	if !hasDeadline {
		ctx, cancel := context.WithCancel(context.Background())
		return ctx, cancel, nil
	}
	stop := deadline.Add(-hpaCleanupReserve)
	if !stop.After(now) {
		return nil, nil, fmt.Errorf("the go test -timeout leaves %s, less than the %s reserved for cleanup; use -timeout 75m (see test/README.md)",
			deadline.Sub(now).Round(time.Second), hpaCleanupReserve)
	}
	ctx, cancel := context.WithDeadline(context.Background(), stop)
	return ctx, cancel, nil
}

type hpaConfig struct {
	metricName       string
	maxReplicas      int32
	annotations      map[string]string
	image            string
	metricTimeout    time.Duration
	scaleUpTimeout   time.Duration
	scaleDownTimeout time.Duration
	stabilization    time.Duration
}

func newHPAConfig(metricName string, maxReplicas int, annotations, image string, metricTimeout, scaleUp, scaleDown, stabilization time.Duration) (hpaConfig, error) {
	switch {
	case metricName == "":
		return hpaConfig{}, errors.New("-hpa-custom-metric-name must not be empty")
	case image == "":
		return hpaConfig{}, errors.New("-hpa-workload-image must not be empty")
	case maxReplicas < 2 || maxReplicas > 100:
		return hpaConfig{}, fmt.Errorf("-hpa-max-replicas must be between 2 and 100, got %d", maxReplicas)
	case metricTimeout <= 0 || scaleUp <= 0 || scaleDown <= 0:
		return hpaConfig{}, errors.New("-hpa-metric-timeout, -hpa-scale-up-timeout and -hpa-scale-down-timeout must be greater than zero")
	case stabilization < 0 || stabilization > time.Hour || stabilization%time.Second != 0:
		return hpaConfig{}, errors.New("-hpa-scale-down-stabilization must be a whole number of seconds between 0 and 1h")
	case stabilization >= scaleDown:
		return hpaConfig{}, errors.New("-hpa-scale-down-stabilization must be shorter than -hpa-scale-down-timeout")
	}
	parsed, err := parseHPAPodAnnotations(annotations)
	if err != nil {
		return hpaConfig{}, fmt.Errorf("-hpa-pod-annotations: %w", err)
	}
	return hpaConfig{
		metricName:       metricName,
		maxReplicas:      int32(maxReplicas),
		annotations:      parsed,
		image:            image,
		metricTimeout:    metricTimeout,
		scaleUpTimeout:   scaleUp,
		scaleDownTimeout: scaleDown,
		stabilization:    stabilization,
	}, nil
}

func parseHPAPodAnnotations(value string) (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(value) == "" {
		return out, nil
	}
	for _, pair := range strings.Split(value, ",") {
		key, val, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("expected key=value, got %q", pair)
		}
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			return nil, fmt.Errorf("invalid annotation key %q: %s", key, strings.Join(errs, "; "))
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("duplicate annotation key %q", key)
		}
		out[key] = val
	}
	return out, nil
}

// hpaMetricServer serves the mounted ConfigMap value as a Prometheus gauge,
// re-reading it on every request so ConfigMap updates reach running Pods.
// /healthz (the readiness probe) succeeds only while the value is a finite
// number, so a Ready Pod is one that can serve a valid sample.
const hpaMetricServer = `
import math, os, socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VALUE_FILE = os.environ["HPA_METRIC_FILE"]
SERIES = os.environ["HPA_METRIC_SERIES"]
PORT = int(os.environ["HPA_METRIC_PORT"])

def read_value():
    with open(VALUE_FILE) as f:
        value = float(f.read().strip())
    if not math.isfinite(value):
        raise ValueError("metric value %r is not finite" % value)
    return value

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/healthz", "` + hpaMetricsPath + `"):
            return self.reply(404, "text/plain", b"not found\n")
        try:
            value = read_value()
        except (OSError, ValueError) as e:
            return self.reply(503, "text/plain", (str(e) + "\n").encode())
        if self.path == "/healthz":
            return self.reply(200, "text/plain", b"ok\n")
        body = "# TYPE %s gauge\n%s %r\n" % (SERIES, SERIES, value)
        self.reply(200, "text/plain; version=0.0.4; charset=utf-8", body.encode())

    def reply(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

class DualStackServer(ThreadingHTTPServer):
    address_family = socket.AF_INET6
    def server_bind(self):
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()

try:
    server = DualStackServer(("::", PORT), Handler)
except OSError:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
server.serve_forever()
`

// hpaWorkloadCommand starts the metric server only if the container sees
// exactly the requested number of accelerators; otherwise it exits and the
// Pod never becomes Ready.
func hpaWorkloadCommand(cfg AcceleratorConfig) string {
	return acceleratorProbeCommand(cfg.DevicePattern) + fmt.Sprintf(`
if [ "$count" -ne %d ]; then
  echo "expected %d accelerator device(s), found $count" >&2
  exit 1
fi
exec python3 -c "$HPA_METRIC_SERVER"`, requestedAcceleratorCount, requestedAcceleratorCount)
}

func hpaWorkloadLabels() map[string]string {
	return map[string]string{hpaAppLabelKey: hpaWorkloadName, hpaMetricSourceLabel: "true"}
}

func hpaWorkloadSelector() string {
	return labels.SelectorFromSet(hpaWorkloadLabels()).String()
}

func buildHPAMetricConfigMap(namespace string, value int) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: namespace},
		Data:       map[string]string{hpaMetricValueKey: strconv.Itoa(value)},
	}
}

func buildHPADeployment(namespace, mode string, cfg AcceleratorConfig, image string, annotations map[string]string) (*appsv1.Deployment, error) {
	container := corev1.Container{
		Name:    "workload",
		Image:   image,
		Command: []string{"/bin/sh", "-c"},
		Args:    []string{hpaWorkloadCommand(cfg)},
		Env: []corev1.EnvVar{
			{Name: "HPA_METRIC_SERVER", Value: hpaMetricServer},
			{Name: "HPA_METRIC_FILE", Value: hpaMetricMountPath + "/" + hpaMetricValueKey},
			{Name: "HPA_METRIC_SERIES", Value: hpaMetricSeries},
			{Name: "HPA_METRIC_PORT", Value: strconv.Itoa(hpaMetricsPort)},
		},
		Ports: []corev1.ContainerPort{{Name: hpaMetricsPortName, ContainerPort: hpaMetricsPort, Protocol: corev1.ProtocolTCP}},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
				Path: "/healthz",
				Port: intstr.FromString(hpaMetricsPortName),
			}},
			PeriodSeconds: 5,
		},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		}},
		// Mount the whole ConfigMap directory, never a subPath: subPath
		// mounts do not receive ConfigMap updates.
		VolumeMounts: []corev1.VolumeMount{{Name: "metric", MountPath: hpaMetricMountPath, ReadOnly: true}},
		// Puts the container's last log lines, such as the accelerator check's
		// "expected 1 accelerator device(s), found 0", into its status when it
		// exits with an error, so failures can be diagnosed from the Pod alone.
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
	}

	pod, err := buildTestPod(namespace, hpaWorkloadName, []corev1.Container{container},
		testPodConfig{grantAccelerator: true, mode: mode, cfg: cfg})
	if err != nil {
		return nil, err
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: "metric",
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: hpaWorkloadName},
		}},
	})

	replicas := int32(hpaMinReplicas)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: namespace, Labels: hpaWorkloadLabels()},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: hpaWorkloadLabels()},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: hpaWorkloadLabels(), Annotations: maps.Clone(annotations)},
				Spec:       pod.Spec,
			},
		},
	}, nil
}

func buildHPA(namespace string, hc hpaConfig) *autoscalingv2.HorizontalPodAutoscaler {
	minReplicas := int32(hpaMinReplicas)
	target := resource.MustParse(strconv.Itoa(hpaTargetValue))
	scaleUpWindow := int32(0)
	scaleDownWindow := int32(hc.stabilization / time.Second)
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: hpaWorkloadName, Namespace: namespace},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       hpaWorkloadName,
			},
			MinReplicas: &minReplicas,
			MaxReplicas: hc.maxReplicas,
			Metrics: []autoscalingv2.MetricSpec{{
				Type: autoscalingv2.PodsMetricSourceType,
				Pods: &autoscalingv2.PodsMetricSource{
					Metric: autoscalingv2.MetricIdentifier{Name: hc.metricName},
					Target: autoscalingv2.MetricTarget{Type: autoscalingv2.AverageValueMetricType, AverageValue: &target},
				},
			}},
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleUp:   &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: &scaleUpWindow},
				ScaleDown: &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: &scaleDownWindow},
			},
		},
	}
}

// hpaEnvError marks a failure of the test's prerequisites (accelerator
// capacity, image, metrics pipeline, API access) as opposed to the HPA not
// scaling correctly once those prerequisites are in place. It survives
// wrapping, so phases can add context.
type hpaEnvError struct{ err error }

func (e hpaEnvError) Error() string { return e.err.Error() }
func (e hpaEnvError) Unwrap() error { return e.err }

func hpaEnvironmentError(err error) error { return hpaEnvError{err} }

func isHPAEnvironmentError(err error) bool {
	var env hpaEnvError
	return errors.As(err, &env)
}

func hpaFailureMessage(err error) string {
	if isHPAEnvironmentError(err) {
		return "ENVIRONMENT ERROR: " + err.Error()
	}
	return "FAIL: " + err.Error()
}

// hpaMetricsUnreadable returns the HPA's own report that it could not read
// its metric (ScalingActive=False, reason FailedGetPodsMetric). That points
// at the metrics pipeline or the controller's access to it, not at scaling.
func hpaMetricsUnreadable(hpa *autoscalingv2.HorizontalPodAutoscaler) (string, bool) {
	if hpa == nil {
		return "", false
	}
	for _, c := range hpa.Status.Conditions {
		if c.Type == autoscalingv2.ScalingActive && c.Status == corev1.ConditionFalse && c.Reason == "FailedGetPodsMetric" {
			return c.Message, true
		}
	}
	return "", false
}

// hpaPhaseFailure classifies a failed HPA phase from its last observation:
// an environment error if the HPA could not read the metric, otherwise a
// conformance failure (or the environment error already carried by err).
func hpaPhaseFailure(err error, last hpaState) error {
	switch message, ok := hpaMetricsUnreadable(last.hpa); {
	case isHPAEnvironmentError(err):
		return err
	case ok:
		return hpaEnvironmentError(fmt.Errorf("the HPA controller cannot read the custom metric (FailedGetPodsMetric: %s): %w", message, err))
	case last.hpa != nil && len(last.hpa.Status.Conditions) > 0:
		return fmt.Errorf("%w; HPA conditions: %s", err, hpaConditionsSummary(last.hpa))
	default:
		return err
	}
}

// hpaConditionsSummary renders the HPA's conditions (AbleToScale,
// ScalingActive, ScalingLimited) for a failure message.
func hpaConditionsSummary(hpa *autoscalingv2.HorizontalPodAutoscaler) string {
	parts := make([]string, 0, len(hpa.Status.Conditions))
	for _, c := range hpa.Status.Conditions {
		parts = append(parts, fmt.Sprintf("%s=%s (%s: %s)", c.Type, c.Status, c.Reason, hpaOneLine(c.Message)))
	}
	return strings.Join(parts, "; ")
}

// resolveCustomMetricsVersion returns the custom metrics API version the HPA
// controller reads, chosen the same way as its client: the group's preferred
// version if the HPA supports it, otherwise the first supported version the
// group lists. v1beta1 and v1beta2 are separate APIService registrations that
// may be backed by different adapters, so the test must read the same one.
// A missing or unavailable adapter is reported here instead of as a timeout.
func resolveCustomMetricsVersion(c kubernetes.Interface) (string, error) {
	groups, err := c.Discovery().ServerGroups()
	if err != nil {
		return "", fmt.Errorf("discover API groups: %w", err)
	}
	var group *metav1.APIGroup
	for i := range groups.Groups {
		if groups.Groups[i].Name == hpaCustomMetricsGroup {
			group = &groups.Groups[i]
		}
	}
	if group == nil {
		return "", fmt.Errorf("the custom metrics API (%s) is not served; configure a custom metrics adapter as described in test/README.md", hpaCustomMetricsGroup)
	}
	version := ""
	if slices.Contains(hpaCustomMetricsVersions, group.PreferredVersion.Version) {
		version = group.PreferredVersion.Version
	} else {
		for _, v := range group.Versions {
			if slices.Contains(hpaCustomMetricsVersions, v.Version) {
				version = v.Version
				break
			}
		}
	}
	switch {
	case version == "" && len(group.Versions) == 0:
		// Aggregated discovery omits versions whose APIService is unavailable.
		return "", fmt.Errorf("%s is registered but none of its versions is available; check the custom metrics adapter's APIService and Pods", hpaCustomMetricsGroup)
	case version == "":
		return "", fmt.Errorf("%s is served in versions %v, none of which the HPA supports (%v)", hpaCustomMetricsGroup, group.Versions, hpaCustomMetricsVersions)
	}
	if _, err := c.Discovery().ServerResourcesForGroupVersion(hpaCustomMetricsGroup + "/" + version); err != nil {
		return "", fmt.Errorf("custom metrics API %s/%s is registered but not available: %w", hpaCustomMetricsGroup, version, err)
	}
	return version, nil
}

// readHPAPodMetrics queries version of the custom metrics API, as the HPA
// controller does, for the per-Pod metric of the workload Pods in namespace.
func readHPAPodMetrics(ctx context.Context, c kubernetes.Interface, version, namespace, metricName string) ([]byte, error) {
	result := c.CoreV1().RESTClient().Get().
		AbsPath("/apis", hpaCustomMetricsGroup, version).
		Namespace(namespace).Resource("pods").Name("*").SubResource(metricName).
		Param("labelSelector", hpaWorkloadSelector()).Do(ctx)
	// Error decodes the adapter's Status (e.g. why a metric is not found);
	// DoRaw would reduce it to a generic message.
	if err := result.Error(); err != nil {
		return nil, err
	}
	return result.Raw()
}

// hpaMetricValueList decodes both custom.metrics.k8s.io/v1beta2 (metric.name)
// and v1beta1 (metricName) MetricValueList responses.
type hpaMetricValueList struct {
	Items []struct {
		DescribedObject struct {
			Kind      string `json:"kind"`
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
		} `json:"describedObject"`
		MetricName string `json:"metricName"`
		Metric     struct {
			Name string `json:"name"`
		} `json:"metric"`
		Value resource.Quantity `json:"value"`
	} `json:"items"`
}

// checkHPAMetricSamples requires every expected Pod to report exactly want
// for metricName. It rejects missing Pods, stale or wrong values, conflicting
// duplicates, and samples describing a different object or metric. Samples
// for other Pods (e.g. ones still terminating) are ignored.
func checkHPAMetricSamples(body []byte, namespace, metricName string, expectedPods []string, want int) error {
	var list hpaMetricValueList
	if err := json.Unmarshal(body, &list); err != nil {
		return fmt.Errorf("decode custom metrics response: %w", err)
	}
	wantQ := resource.MustParse(strconv.Itoa(want))
	got := map[string]resource.Quantity{}
	var problems []string
	for _, item := range list.Items {
		obj := item.DescribedObject
		name := item.Metric.Name
		if name == "" {
			name = item.MetricName
		}
		switch {
		case obj.Kind != "Pod" || obj.Namespace != namespace:
			problems = append(problems, fmt.Sprintf("unexpected object %s %s/%s", obj.Kind, obj.Namespace, obj.Name))
			continue
		case name != metricName:
			problems = append(problems, fmt.Sprintf("Pod %s reports metric %q, want %q", obj.Name, name, metricName))
			continue
		}
		if prev, dup := got[obj.Name]; dup && prev.Cmp(item.Value) != 0 {
			problems = append(problems, fmt.Sprintf("Pod %s has conflicting values %s and %s", obj.Name, prev.String(), item.Value.String()))
			continue
		}
		got[obj.Name] = item.Value
	}
	for _, pod := range expectedPods {
		v, ok := got[pod]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("no sample for Pod %s", pod))
		case v.Cmp(wantQ) != 0:
			problems = append(problems, fmt.Sprintf("Pod %s reports %s, want %d", pod, v.String(), want))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// hpaState is one observation of everything the scaling assertions use.
type hpaState struct {
	// hpa is nil until the scenario creates it.
	hpa        *autoscalingv2.HorizontalPodAutoscaler
	deployment *appsv1.Deployment
	// pods are the workload Pods that are neither terminating nor finished,
	// sorted by name.
	pods []corev1.Pod
	// inactive are the remaining workload Pod objects: terminating, Succeeded
	// or Failed.
	inactive []corev1.Pod
}

func getHPAState(ctx context.Context, c kubernetes.Interface, namespace string) (hpaState, error) {
	hpa, err := c.AutoscalingV2().HorizontalPodAutoscalers(namespace).Get(ctx, hpaWorkloadName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		hpa = nil // not created yet
	case err != nil:
		return hpaState{}, err
	}
	deployment, err := c.AppsV1().Deployments(namespace).Get(ctx, hpaWorkloadName, metav1.GetOptions{})
	if err != nil {
		return hpaState{}, err
	}
	list, err := c.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: hpaWorkloadSelector()})
	if err != nil {
		return hpaState{}, err
	}
	s := hpaState{hpa: hpa, deployment: deployment}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })
	for _, pod := range list.Items {
		if pod.DeletionTimestamp == nil && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			s.pods = append(s.pods, pod)
		} else {
			s.inactive = append(s.inactive, pod)
		}
	}
	return s, nil
}

func (s hpaState) podNames() []string {
	names := make([]string, 0, len(s.pods))
	for _, pod := range s.pods {
		names = append(names, pod.Name)
	}
	return names
}

func (s hpaState) podUIDs() map[types.UID]bool {
	uids := make(map[types.UID]bool, len(s.pods))
	for _, pod := range s.pods {
		uids[pod.UID] = true
	}
	return uids
}

// checkHPAStatus requires the HPA to have reconciled its current spec on the
// custom metric, recorded the phase's metric value, and decided on want
// replicas.
//
// The HPA records as the metric's average the plain mean of the samples of
// its non-terminating, non-Pending Pods (pkg/controller/podautoscaler), so
// once every such Pod reports value, the recorded average is exactly value.
// Requiring that, rather than only the expected side of the target, ties the
// decision to the current phase. Callers poll, so transient averages while a
// change propagates are retried rather than failed.
func checkHPAStatus(hpa *autoscalingv2.HorizontalPodAutoscaler, metricName string, want int32, value int) error {
	if hpa == nil {
		return errors.New("HPA does not exist")
	}
	// Released HPA controllers (through v1.36) never set
	// status.observedGeneration, so it is enforced only when reported. The
	// test never changes the HPA spec after creating it.
	if og := hpa.Status.ObservedGeneration; og != nil && *og < hpa.Generation {
		return fmt.Errorf("HPA status has not observed generation %d", hpa.Generation)
	}
	var active *autoscalingv2.HorizontalPodAutoscalerCondition
	for i := range hpa.Status.Conditions {
		if hpa.Status.Conditions[i].Type == autoscalingv2.ScalingActive {
			active = &hpa.Status.Conditions[i]
		}
	}
	if active == nil || active.Status != corev1.ConditionTrue {
		reason := "not reported"
		if active != nil {
			reason = fmt.Sprintf("%s (%s: %s)", active.Status, active.Reason, active.Message)
		}
		return fmt.Errorf("HPA condition ScalingActive is %s", reason)
	}
	var current *resource.Quantity
	for _, m := range hpa.Status.CurrentMetrics {
		if m.Type == autoscalingv2.PodsMetricSourceType && m.Pods != nil && m.Pods.Metric.Name == metricName {
			current = m.Pods.Current.AverageValue
		}
	}
	if current == nil {
		return fmt.Errorf("HPA status.currentMetrics has no Pods metric %q", metricName)
	}
	if wantQ := resource.MustParse(strconv.Itoa(value)); current.Cmp(wantQ) != 0 {
		return fmt.Errorf("HPA recorded average %s for %q, want the current value %d", current.String(), metricName, value)
	}
	if hpa.Status.DesiredReplicas != want {
		return fmt.Errorf("HPA desiredReplicas=%d, want %d", hpa.Status.DesiredReplicas, want)
	}
	return nil
}

// checkHPAWorkload requires the Deployment and its active Pods to
// agree on want Running and Ready replicas. The test never changes the
// Deployment's replicas after creating it, so spec.replicas == want can only
// result from the HPA.
func checkHPAWorkload(s hpaState, want int32) error {
	d := s.deployment
	if d.Spec.Replicas == nil || *d.Spec.Replicas != want {
		return fmt.Errorf("Deployment spec.replicas=%d, want %d", hpaReplicaCount(d.Spec.Replicas), want)
	}
	if d.Status.ObservedGeneration < d.Generation {
		return fmt.Errorf("Deployment status has not observed generation %d", d.Generation)
	}
	if d.Status.Replicas != want || d.Status.UpdatedReplicas != want || d.Status.ReadyReplicas != want {
		return fmt.Errorf("Deployment status replicas=%d updated=%d ready=%d, want %d",
			d.Status.Replicas, d.Status.UpdatedReplicas, d.Status.ReadyReplicas, want)
	}
	if int32(len(s.pods)) != want {
		return fmt.Errorf("found %d active workload Pods %v, want %d", len(s.pods), s.podNames(), want)
	}
	for i := range s.pods {
		pod := &s.pods[i]
		if pod.Status.Phase != corev1.PodRunning || !podIsReady(pod) || pod.Spec.NodeName == "" {
			return fmt.Errorf("Pod %s is %s (ready=%t, node=%q), want Running and Ready", pod.Name, pod.Status.Phase, podIsReady(pod), pod.Spec.NodeName)
		}
	}
	return nil
}

// checkHPAScaledTo combines the HPA and workload checks for a settled state.
func checkHPAScaledTo(s hpaState, metricName string, want int32, value int) error {
	if err := checkHPAStatus(s.hpa, metricName, want, value); err != nil {
		return err
	}
	if s.hpa.Status.CurrentReplicas != want {
		return fmt.Errorf("HPA currentReplicas=%d, want %d", s.hpa.Status.CurrentReplicas, want)
	}
	return checkHPAWorkload(s, want)
}

// checkHPAHeld requires the HPA to keep deciding on want replicas for the
// phase's metric value and the Deployment to stay at want. It does not
// re-check the Pods: a Pod evicted or restarted during the window is a
// lifecycle event, not the HPA scaling.
func checkHPAHeld(s hpaState, metricName string, want int32, value int) error {
	if err := checkHPAStatus(s.hpa, metricName, want, value); err != nil {
		return err
	}
	if n := hpaReplicaCount(s.deployment.Spec.Replicas); n != want {
		return fmt.Errorf("Deployment spec.replicas=%d, want %d", n, want)
	}
	return nil
}

// checkHPANewPods requires at least minNew of the Pods to be absent from the
// baseline, so pre-existing Pods cannot satisfy a scale-up.
func checkHPANewPods(pods []corev1.Pod, baseline map[types.UID]bool, minNew int) error {
	n := 0
	for _, pod := range pods {
		if !baseline[pod.UID] {
			n++
		}
	}
	if n < minNew {
		return fmt.Errorf("%d workload Pods were created after the baseline, want at least %d", n, minNew)
	}
	return nil
}

// checkHPARemovedPodsGone requires every Pod the scale-down deleted to be
// gone, not still terminating. The ReplicaSet removes replicas only by
// deleting them, so a removed replica is a Pod object with a deletion
// timestamp until it disappears.
//
// Pod lifecycle events that are not part of autoscaling are allowed:
//   - A Pod the node ended without deleting it (Phase=Failed or Succeeded
//     with no deletion timestamp, e.g. evicted under node pressure) stays as
//     an object until something deletes it; it no longer holds an accelerator
//     (in DRA mode checkHPAClaimsReleased verifies its claim was deallocated).
//   - A Pod terminating because of a disruption, which Kubernetes marks with
//     DisruptionTarget=True before deleting it (eviction API, taint-based
//     eviction, preemption, Pod garbage collection, kubelet). The ReplicaSet
//     deletes scale-down victims without that condition, so they are never
//     exempted. A disrupted Pod may terminate slowly or not at all (e.g. on
//     an unreachable node) for reasons unrelated to the HPA.
//   - The remaining replica being a replacement for a scaled-up Pod lost in
//     one of those ways: the HPA's decision is checked through the replica
//     counts, not through which Pod survived.
func checkHPARemovedPodsGone(s hpaState) error {
	var left []string
	for _, pod := range s.inactive {
		if _, disrupted := hpaDisruption(&pod); pod.DeletionTimestamp != nil && !disrupted {
			left = append(left, pod.Name+" (Terminating)")
		}
	}
	if len(left) > 0 {
		return fmt.Errorf("removed Pods still exist: %s", strings.Join(left, ", "))
	}
	return nil
}

// hpaDisruption returns the reason of a Pod's DisruptionTarget=True
// condition, which Kubernetes sets on Pods it is about to end for a reason
// other than their owner deleting them.
func hpaDisruption(pod *corev1.Pod) (string, bool) {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.DisruptionTarget && c.Status == corev1.ConditionTrue {
			return c.Reason, true
		}
	}
	return "", false
}

// hpaClaimHolders returns the UIDs of the Pods whose ResourceClaims may still
// be allocated after the scale-down: the active Pods and Pods still
// terminating after a disruption, which keep their claims until they are
// gone.
func hpaClaimHolders(s hpaState) map[types.UID]bool {
	holders := s.podUIDs()
	for i := range s.inactive {
		if _, disrupted := hpaDisruption(&s.inactive[i]); disrupted && s.inactive[i].DeletionTimestamp != nil {
			holders[s.inactive[i].UID] = true
		}
	}
	return holders
}

// hpaLifecycleNotes describes, for the log, Pod events outside autoscaling
// that checkHPARemovedPodsGone allows: Pods the node ended, Pods terminating
// after a disruption, and replicas that replaced scaled-up Pods.
func hpaLifecycleNotes(s hpaState, scaledUp map[types.UID]bool) string {
	var notes []string
	for _, pod := range s.inactive {
		if reason, disrupted := hpaDisruption(&pod); disrupted && pod.DeletionTimestamp != nil {
			notes = append(notes, fmt.Sprintf("Pod %s is terminating after a disruption (%s), not removed by the scale-down", pod.Name, reason))
			continue
		}
		if pod.DeletionTimestamp == nil {
			state := string(pod.Status.Phase)
			if pod.Status.Reason != "" {
				state += ": " + pod.Status.Reason
			}
			notes = append(notes, fmt.Sprintf("Pod %s was ended by its node (%s), not removed by the scale-down", pod.Name, state))
		}
	}
	for _, pod := range s.pods {
		if !scaledUp[pod.UID] {
			notes = append(notes, fmt.Sprintf("Pod %s replaced a scaled-up Pod", pod.Name))
		}
	}
	if len(notes) == 0 {
		return ""
	}
	return "; note: " + strings.Join(notes, "; ")
}

func hpaReplicaCount(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

// verifyHPAPodsOnAccelerators checks that every Pod was granted an
// accelerator through the resolved allocation mode and runs on a node that
// can provide it, and that the Pods do not hold more accelerators than
// exist: in DRA mode no device is allocated to two Pods unless the driver
// allocated it as separate shares, and in device-plugin mode no node runs
// more of the Pods than it has allocatable accelerators.
func verifyHPAPodsOnAccelerators(ctx context.Context, c kubernetes.Interface, pods []corev1.Pod, mode string, cfg AcceleratorConfig) error {
	nodes := map[string]*corev1.Node{}
	podsOnNode := map[string]int64{}
	deviceHolder := map[string]string{}
	for i := range pods {
		pod := &pods[i]
		node, ok := nodes[pod.Spec.NodeName]
		if !ok {
			var err error
			if node, err = c.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{}); err != nil {
				return hpaAPIError(fmt.Errorf("get node %s of Pod %s: %w", pod.Spec.NodeName, pod.Name, err))
			}
			nodes[node.Name] = node
		}
		var claims []*resourcev1.ResourceClaim
		if mode == allocationModeDRA {
			for _, status := range pod.Status.ResourceClaimStatuses {
				if status.ResourceClaimName == nil {
					continue
				}
				claim, err := c.ResourceV1().ResourceClaims(pod.Namespace).Get(ctx, *status.ResourceClaimName, metav1.GetOptions{})
				if err != nil {
					return hpaAPIError(fmt.Errorf("get ResourceClaim %s of Pod %s: %w", *status.ResourceClaimName, pod.Name, err))
				}
				claims = append(claims, claim)
			}
		}
		if err := checkHPAPodAccelerator(pod, node, claims, mode, cfg); err != nil {
			return err
		}
		podsOnNode[node.Name] += requestedAcceleratorCount
		for _, claim := range claims {
			for _, r := range claim.Status.Allocation.Devices.Results {
				key := r.Driver + "/" + r.Pool + "/" + r.Device
				if r.ShareID != nil {
					key += "#" + string(*r.ShareID)
				}
				if other, dup := deviceHolder[key]; dup {
					return fmt.Errorf("device %s is allocated to both Pod %s and Pod %s", key, other, pod.Name)
				}
				deviceHolder[key] = pod.Name
			}
		}
	}
	if mode == allocationModeDevicePlugin {
		resourceName := corev1.ResourceName(cfg.ExtendedResource)
		for name, n := range podsOnNode {
			allocatable := nodes[name].Status.Allocatable[resourceName]
			if n > allocatable.Value() {
				return fmt.Errorf("node %s runs %d of the test's Pods, each holding %d %s, but has only %s allocatable",
					name, n/requestedAcceleratorCount, requestedAcceleratorCount, resourceName, allocatable.String())
			}
		}
	}
	return nil
}

// hpaAPIError marks a definitive API answer, such as Forbidden, as an
// environment error. NotFound and retryable errors stay plain, so the
// caller's poll retries them.
func hpaAPIError(err error) error {
	var status apierrors.APIStatus
	if errors.As(err, &status) && !apierrors.IsNotFound(err) && !isRetryableAPIError(err) {
		return hpaEnvironmentError(err)
	}
	return err
}

func checkHPAPodAccelerator(pod *corev1.Pod, node *corev1.Node, claims []*resourcev1.ResourceClaim, mode string, cfg AcceleratorConfig) error {
	if !podRequestsAccelerator(pod, mode, cfg) {
		return fmt.Errorf("Pod %s does not request an accelerator in %s mode", pod.Name, mode)
	}
	switch mode {
	case allocationModeDevicePlugin:
		resourceName := corev1.ResourceName(cfg.ExtendedResource)
		allocatable := node.Status.Allocatable[resourceName]
		if allocatable.Value() < requestedAcceleratorCount {
			return fmt.Errorf("Pod %s runs on node %s with allocatable %s=%s", pod.Name, node.Name, resourceName, allocatable.String())
		}
		return nil
	case allocationModeDRA:
		if len(claims) != 1 {
			return fmt.Errorf("Pod %s has %d generated ResourceClaims in its status, want 1", pod.Name, len(claims))
		}
		return checkHPAClaimAllocation(pod, node, claims[0], cfg)
	default:
		return fmt.Errorf("unknown allocation mode %q", mode)
	}
}

func checkHPAClaimAllocation(pod *corev1.Pod, node *corev1.Node, claim *resourcev1.ResourceClaim, cfg AcceleratorConfig) error {
	// Each replica must hold a claim generated for it from the template, not a
	// claim shared with other Pods, so every replica has its own allocation.
	if owner := metav1.GetControllerOf(claim); owner == nil || owner.Kind != "Pod" || owner.UID != pod.UID {
		return fmt.Errorf("ResourceClaim %s was not generated for Pod %s (controller owner %v)", claim.Name, pod.Name, owner)
	}
	alloc := claim.Status.Allocation
	if alloc == nil {
		return fmt.Errorf("ResourceClaim %s of Pod %s is not allocated", claim.Name, pod.Name)
	}
	if len(claim.Status.ReservedFor) != 1 || !claimReservedFor(claim, map[types.UID]bool{pod.UID: true}) {
		return fmt.Errorf("ResourceClaim %s is reserved for %v, want only Pod %s", claim.Name, claim.Status.ReservedFor, pod.Name)
	}
	devices := 0
	for _, r := range alloc.Devices.Results {
		if r.Request != testRequestName {
			continue
		}
		if r.Driver != cfg.DRADriver {
			return fmt.Errorf("ResourceClaim %s allocated device %s/%s from driver %s, want %s", claim.Name, r.Pool, r.Device, r.Driver, cfg.DRADriver)
		}
		devices++
	}
	if int64(devices) != requestedAcceleratorCount {
		return fmt.Errorf("ResourceClaim %s allocated %d devices for request %s, want %d", claim.Name, devices, testRequestName, requestedAcceleratorCount)
	}
	if alloc.NodeSelector != nil {
		matcher, err := nodeaffinity.NewNodeSelector(alloc.NodeSelector)
		if err != nil {
			return fmt.Errorf("ResourceClaim %s has an invalid allocation node selector: %w", claim.Name, err)
		}
		if !matcher.Match(node) {
			return fmt.Errorf("Pod %s runs on node %s outside the allocation node selector of ResourceClaim %s", pod.Name, node.Name, claim.Name)
		}
	}
	return nil
}

func claimReservedFor(claim *resourcev1.ResourceClaim, pods map[types.UID]bool) bool {
	for _, ref := range claim.Status.ReservedFor {
		if ref.Resource == "pods" && pods[ref.UID] {
			return true
		}
	}
	return false
}

// checkHPAClaimsReleased fails while any ResourceClaim in the list is still
// allocated without being reserved for one of the live Pods, i.e. while a
// removed replica's claim still holds its device. Generated claims may
// outlive their Pod briefly; a deallocated or deleted claim passes.
func checkHPAClaimsReleased(claims []resourcev1.ResourceClaim, live map[types.UID]bool) error {
	var held []string
	for i := range claims {
		claim := &claims[i]
		if claim.Status.Allocation == nil || claimReservedFor(claim, live) {
			continue
		}
		owner := "unknown Pod"
		if ref := metav1.GetControllerOf(claim); ref != nil {
			owner = ref.Kind + " " + ref.Name
		}
		held = append(held, fmt.Sprintf("%s (generated for %s)", claim.Name, owner))
	}
	if len(held) > 0 {
		sort.Strings(held)
		return fmt.Errorf("ResourceClaims still allocated for removed Pods: %s", strings.Join(held, ", "))
	}
	return nil
}

// hpaPodPrerequisiteProblems lists Pods held back by something other than
// the HPA: unschedulable (e.g. insufficient accelerator capacity), unable to
// pull or create the workload container, a workload container that exited
// with an error, or a Pod the node ended (Phase=Failed, e.g. evicted or
// rejected at admission because its device could not be allocated). Exited
// containers cover the container's own startup check failing (it did not see
// exactly one accelerator device) and a workload image that cannot run the
// test's script; the recorded output says which. Pods being deleted are
// skipped: their containers stop as part of a normal removal. None of these
// says anything about the HPA, so they are never reported as FAIL.
func hpaPodPrerequisiteProblems(pods []corev1.Pod) []string {
	var out []string
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if unschedulable, message := podUnschedulable(pod); unschedulable {
			out = append(out, fmt.Sprintf("%s is Unschedulable: %s", pod.Name, message))
			continue
		}
		reported := len(out)
		for _, cs := range pod.Status.ContainerStatuses {
			switch w := cs.State.Waiting; {
			case w != nil && (w.Reason == "ErrImagePull" || w.Reason == "ImagePullBackOff" || w.Reason == "InvalidImageName"):
				out = append(out, fmt.Sprintf("%s cannot pull its image (%s: %s)", pod.Name, w.Reason, w.Message))
			case w != nil && (w.Reason == "CreateContainerConfigError" || w.Reason == "CreateContainerError" || w.Reason == "RunContainerError"):
				out = append(out, fmt.Sprintf("%s cannot start its container (%s: %s)", pod.Name, w.Reason, w.Message))
			default:
				if t := hpaFailedTermination(cs); t != nil {
					out = append(out, fmt.Sprintf("%s: workload container exited with code %d (%s), restarts=%d; output: %s",
						pod.Name, t.ExitCode, t.Reason, cs.RestartCount, hpaOneLine(t.Message)))
				} else if w != nil && w.Reason == "CrashLoopBackOff" {
					out = append(out, fmt.Sprintf("%s: workload container is in CrashLoopBackOff, restarts=%d: %s", pod.Name, cs.RestartCount, hpaOneLine(w.Message)))
				}
			}
		}
		if pod.Status.Phase == corev1.PodFailed && len(out) == reported {
			out = append(out, fmt.Sprintf("%s failed (%s: %s)", pod.Name, pod.Status.Reason, hpaOneLine(pod.Status.Message)))
		}
	}
	return out
}

// allPods returns the active and inactive workload Pods.
func (s hpaState) allPods() []corev1.Pod {
	return slices.Concat(s.pods, s.inactive)
}

// hpaFailedTermination returns the failed exit of a container that is down
// now: terminated with an error, or waiting to restart after one
// (CrashLoopBackOff). A container that failed earlier but is running again
// does not count, so a past restart cannot excuse a later HPA failure.
func hpaFailedTermination(cs corev1.ContainerStatus) *corev1.ContainerStateTerminated {
	if t := cs.State.Terminated; t != nil {
		if t.ExitCode != 0 {
			return t
		}
		return nil
	}
	if w := cs.State.Waiting; w != nil && w.Reason == "CrashLoopBackOff" {
		if t := cs.LastTerminationState.Terminated; t != nil && t.ExitCode != 0 {
			return t
		}
	}
	return nil
}

// hpaOneLine shortens a container's termination message for an error line.
func hpaOneLine(message string) string {
	message = strings.Join(strings.Fields(message), " ")
	if message == "" {
		return "(none recorded)"
	}
	const limit = 300
	if len(message) > limit {
		message = "..." + message[len(message)-limit:]
	}
	return message
}

// hpaScenario drives one baseline, scale-up and scale-down cycle in a
// test-owned namespace.
type hpaScenario struct {
	c         kubernetes.Interface
	namespace string
	mode      string
	cfg       AcceleratorConfig
	hc        hpaConfig
	logf      func(format string, args ...any)
	// readMetrics queries the custom metrics API; unit tests replace it
	// because the fake clientset does not serve aggregated APIs.
	readMetrics func(ctx context.Context, namespace, metricName string) ([]byte, error)
}

func newHPAScenario(c kubernetes.Interface, namespace, mode, metricsVersion string, cfg AcceleratorConfig, hc hpaConfig, logf func(string, ...any)) *hpaScenario {
	return &hpaScenario{
		c: c, namespace: namespace, mode: mode, cfg: cfg, hc: hc, logf: logf,
		readMetrics: func(ctx context.Context, namespace, metricName string) ([]byte, error) {
			return readHPAPodMetrics(ctx, c, metricsVersion, namespace, metricName)
		},
	}
}

func (s *hpaScenario) run(ctx context.Context) error {
	if err := s.deploy(ctx); err != nil {
		return hpaEnvironmentError(err)
	}
	baseline, err := s.establishBaseline(ctx)
	if err != nil {
		return err
	}
	scaled, err := s.scaleUp(ctx, baseline)
	if err != nil {
		return err
	}
	return s.scaleDown(ctx, scaled)
}

func (s *hpaScenario) deploy(ctx context.Context) error {
	deployment, err := buildHPADeployment(s.namespace, s.mode, s.cfg, s.hc.image, s.hc.annotations)
	if err != nil {
		return err
	}
	if _, err := s.c.CoreV1().ConfigMaps(s.namespace).Create(ctx, buildHPAMetricConfigMap(s.namespace, hpaBaselineValue), metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create metric ConfigMap: %w", err)
	}
	if _, err := s.c.AppsV1().Deployments(s.namespace).Create(ctx, deployment, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create Deployment: %w", err)
	}
	return nil
}

func (s *hpaScenario) establishBaseline(ctx context.Context) (hpaState, error) {
	s.logf("Waiting for the baseline accelerator Pod to become Ready...")
	state, err := s.waitForState(ctx, s.hc.scaleUpTimeout, func(ctx context.Context, st hpaState) error {
		if st.hpa != nil {
			return errors.New("HPA exists before the baseline was established")
		}
		if err := checkHPAWorkload(st, hpaMinReplicas); err != nil {
			return err
		}
		return verifyHPAPodsOnAccelerators(ctx, s.c, st.pods, s.mode, s.cfg)
	})
	if err != nil {
		if problems := hpaPodPrerequisiteProblems(state.allPods()); len(problems) > 0 {
			err = fmt.Errorf("%w; %s", err, strings.Join(problems, "; "))
		}
		return hpaState{}, hpaEnvironmentError(fmt.Errorf("baseline accelerator Pod did not become Ready: %w", err))
	}

	s.logf("Waiting for the custom metrics API to report %d for %v...", hpaBaselineValue, state.podNames())
	if err := s.waitForMetric(ctx, hpaBaselineValue, fixedPods(state.podNames())); err != nil {
		return hpaState{}, hpaEnvironmentError(fmt.Errorf("custom metrics pipeline does not serve the workload metric (see the integration contract in test/README.md): %w", err))
	}

	// The HPA is created only now, so it cannot act before the workload and
	// its metric are known to be healthy, and any scaling it does from here
	// on is attributable to it.
	if _, err := s.c.AutoscalingV2().HorizontalPodAutoscalers(s.namespace).Create(ctx, buildHPA(s.namespace, s.hc), metav1.CreateOptions{}); err != nil {
		return hpaState{}, hpaEnvironmentError(fmt.Errorf("create HorizontalPodAutoscaler: %w", err))
	}
	atBaseline := func(_ context.Context, st hpaState) error {
		return checkHPAScaledTo(st, s.hc.metricName, hpaMinReplicas, hpaBaselineValue)
	}
	if state, err = s.waitForState(ctx, s.hc.metricTimeout, atBaseline); err != nil {
		return hpaState{}, hpaPhaseFailure(fmt.Errorf("HPA did not reconcile on the custom metric at baseline: %w", err), state)
	}
	s.logf(hpaNegativeControlLog+": the HPA must stay at %d replica for %s while the metric is below target...", hpaMinReplicas, hpaNegativeControlWindow)
	held := func(_ context.Context, st hpaState) error {
		return checkHPAHeld(st, s.hc.metricName, hpaMinReplicas, hpaBaselineValue)
	}
	if last, err := s.holdState(ctx, hpaNegativeControlWindow, held); err != nil {
		return hpaState{}, hpaPhaseFailure(fmt.Errorf("HPA did not hold %d replica with the metric below target: %w", hpaMinReplicas, err), last)
	}
	s.logf("PASS: Baseline established with Pod %v", state.podNames())
	return state, nil
}

func (s *hpaScenario) scaleUp(ctx context.Context, baseline hpaState) (hpaState, error) {
	deadline := time.Now().Add(s.hc.scaleUpTimeout)
	maxReplicas := s.hc.maxReplicas

	s.logf("Raising the custom metric to %d (target %d)...", hpaScaleUpValue, hpaTargetValue)
	if err := setHPAMetricValue(ctx, s.c, s.namespace, hpaScaleUpValue); err != nil {
		return hpaState{}, hpaEnvironmentError(fmt.Errorf("update metric ConfigMap: %w", err))
	}
	if err := s.waitForMetric(ctx, hpaScaleUpValue, fixedPods(baseline.podNames())); err != nil {
		return hpaState{}, hpaEnvironmentError(fmt.Errorf("custom metrics pipeline did not report the raised metric: %w", err))
	}

	state, err := s.waitForState(ctx, time.Until(deadline), func(_ context.Context, st hpaState) error {
		if err := checkHPAStatus(st.hpa, s.hc.metricName, maxReplicas, hpaScaleUpValue); err != nil {
			return err
		}
		if hpaReplicaCount(st.deployment.Spec.Replicas) != maxReplicas {
			return fmt.Errorf("Deployment spec.replicas=%d, want %d", hpaReplicaCount(st.deployment.Spec.Replicas), maxReplicas)
		}
		return nil
	})
	if err != nil {
		return hpaState{}, hpaPhaseFailure(fmt.Errorf("HPA did not scale the Deployment to %d replicas on the raised metric: %w", maxReplicas, err), state)
	}
	s.logf("HPA set the Deployment to %d replicas; waiting for the new accelerator Pods...", maxReplicas)

	baselineUIDs := baseline.podUIDs()
	scaledUp := func(ctx context.Context, st hpaState) error {
		if err := checkHPAScaledTo(st, s.hc.metricName, maxReplicas, hpaScaleUpValue); err != nil {
			return err
		}
		if err := checkHPANewPods(st.pods, baselineUIDs, int(maxReplicas)-len(baselineUIDs)); err != nil {
			return err
		}
		return verifyHPAPodsOnAccelerators(ctx, s.c, st.pods, s.mode, s.cfg)
	}
	if state, err = s.waitForState(ctx, time.Until(deadline), scaledUp); err != nil {
		return hpaState{}, s.scaleUpFailure(fmt.Errorf("the %d scaled-up Pods did not all become Ready on accelerators: %w", maxReplicas, err), state)
	}
	// The scaled-up state and the raised metric for exactly its Pods must hold
	// in a single observation. Polling for that, rather than re-checking once,
	// tolerates a retryable API error, an HPA sync that briefly fails to read
	// the metric, and a Pod replaced (e.g. evicted) meanwhile, whose
	// replacement must then report the metric too; a state that never
	// converges still fails.
	var metricProblem error
	state, err = s.waitForState(ctx, time.Until(deadline.Add(s.hc.metricTimeout)), func(ctx context.Context, st hpaState) error {
		metricProblem = nil
		if err := scaledUp(ctx, st); err != nil {
			return err
		}
		body, err := s.readMetrics(ctx, s.namespace, s.hc.metricName)
		if err == nil {
			err = checkHPAMetricSamples(body, s.namespace, s.hc.metricName, st.podNames(), hpaScaleUpValue)
		}
		if err != nil {
			metricProblem = err
			return hpaAPIError(fmt.Errorf("custom metric for the scaled-up Pods: %w", err))
		}
		return nil
	})
	switch {
	case err == nil:
	case metricProblem != nil:
		return hpaState{}, hpaEnvironmentError(fmt.Errorf("custom metrics pipeline did not report the raised metric for the scaled-up Pods: %w", err))
	default:
		return hpaState{}, s.scaleUpFailure(fmt.Errorf("the scaled-up state did not hold while the Pods' metrics were confirmed: %w", err), state)
	}
	s.logf("PASS: HPA scaled up to %d replicas; Pods %v are Ready on accelerators", maxReplicas, state.podNames())
	return state, nil
}

// scaleUpFailure classifies a failure to get the scaled-up Pods Ready. If the
// HPA was still deciding on maxReplicas for the raised metric, the HPA did its
// part, and Pods that cannot start (including Pods the node ended, which
// getHPAState lists as inactive) are an environment error. Otherwise the HPA's
// own state decides, as in any other phase.
func (s *hpaScenario) scaleUpFailure(err error, last hpaState) error {
	if !isHPAEnvironmentError(err) && checkHPAStatus(last.hpa, s.hc.metricName, s.hc.maxReplicas, hpaScaleUpValue) == nil {
		if problems := hpaPodPrerequisiteProblems(last.allPods()); len(problems) > 0 {
			return hpaEnvironmentError(fmt.Errorf("scaled-up Pods cannot start (%s): %w", strings.Join(problems, "; "), err))
		}
	}
	return hpaPhaseFailure(err, last)
}

func (s *hpaScenario) scaleDown(ctx context.Context, scaled hpaState) error {
	s.logf("Lowering the custom metric to %d (target %d)...", hpaScaleDownValue, hpaTargetValue)
	lowered := time.Now()
	if err := setHPAMetricValue(ctx, s.c, s.namespace, hpaScaleDownValue); err != nil {
		return hpaEnvironmentError(fmt.Errorf("update metric ConfigMap: %w", err))
	}
	if err := s.waitForMetric(ctx, hpaScaleDownValue, s.livePods); err != nil {
		return hpaEnvironmentError(fmt.Errorf("custom metrics pipeline did not report the lowered metric: %w", err))
	}

	scaledUIDs := scaled.podUIDs()
	s.logf("Waiting up to %s for the HPA to scale down after its %s stabilization window...", s.hc.scaleDownTimeout, s.hc.stabilization)
	state, err := s.waitForState(ctx, s.hc.scaleDownTimeout, func(ctx context.Context, st hpaState) error {
		if err := checkHPAScaledTo(st, s.hc.metricName, hpaMinReplicas, hpaScaleDownValue); err != nil {
			return err
		}
		if err := checkHPARemovedPodsGone(st); err != nil {
			return err
		}
		if s.mode == allocationModeDRA {
			claims, err := s.c.ResourceV1().ResourceClaims(s.namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return hpaAPIError(fmt.Errorf("list ResourceClaims: %w", err))
			}
			if err := checkHPAClaimsReleased(claims.Items, hpaClaimHolders(st)); err != nil {
				return err
			}
		}
		// The remaining Pod's metric was verified by the waitForMetric above,
		// which covered every live Pod, or, for a replacement Pod created
		// since, by the HPA recording exactly the lowered value.
		return verifyHPAPodsOnAccelerators(ctx, s.c, st.pods, s.mode, s.cfg)
	})
	if err != nil {
		return hpaPhaseFailure(fmt.Errorf("HPA did not scale back down to %d replica with the removed %s: %w", hpaMinReplicas, s.removalDescription(), err), state)
	}
	// The stabilization window is reported, not asserted: how soon a correct
	// HPA may scale down also depends on its controller's sync period, which
	// the test cannot observe.
	s.logf("PASS: HPA scaled down to %d replica (%v) with the removed %s, observed %s after the metric was lowered (scale-down stabilization window %s)%s",
		hpaMinReplicas, state.podNames(), s.removalDescription(), time.Since(lowered).Round(time.Second), s.hc.stabilization, hpaLifecycleNotes(state, scaledUIDs))
	return nil
}

// removalDescription states what the test verifies about removed replicas.
// With DRA, deallocated ResourceClaims show the devices were given back.
// With a device plugin, the test sees only that the Pod objects are gone; it
// does not observe the plugin returning the device to its pool.
func (s *hpaScenario) removalDescription() string {
	if s.mode == allocationModeDRA {
		return "Pods deleted and their ResourceClaims no longer allocated"
	}
	return "Pods deleted"
}

// fixedPods expects samples from exactly the named Pods.
func fixedPods(names []string) func(context.Context) ([]string, error) {
	return func(context.Context) ([]string, error) { return names, nil }
}

// livePods expects samples from whichever workload Pods are currently not
// terminating, for phases in which the HPA may remove Pods while it waits.
func (s *hpaScenario) livePods(ctx context.Context) ([]string, error) {
	st, err := getHPAState(ctx, s.c, s.namespace)
	return st.podNames(), err
}

// waitForMetric waits until the custom metrics API reports want for every
// Pod returned by expected, which is re-evaluated on each poll. NotFound is
// retried because adapters return it until the first scrape of a new series
// is ingested; only definitive API answers such as Forbidden end the wait
// early.
func (s *hpaScenario) waitForMetric(ctx context.Context, want int, expected func(context.Context) ([]string, error)) error {
	var pods []string
	var lastSamples, lastAPIError error
	err := wait.PollUntilContextTimeout(ctx, hpaPollInterval, s.hc.metricTimeout, true, func(ctx context.Context) (bool, error) {
		var err error
		if pods, err = expected(ctx); err != nil {
			lastAPIError = err
			return false, nil
		}
		if len(pods) == 0 {
			lastSamples = errors.New("no live workload Pods")
			return false, nil
		}
		body, err := s.readMetrics(ctx, s.namespace, s.hc.metricName)
		if err != nil {
			lastAPIError = err
			var status apierrors.APIStatus
			if errors.As(err, &status) && !apierrors.IsNotFound(err) && !isRetryableAPIError(err) {
				return false, err
			}
			return false, nil
		}
		lastAPIError = nil
		lastSamples = checkHPAMetricSamples(body, s.namespace, s.hc.metricName, pods, want)
		return lastSamples == nil, nil
	})
	if err != nil {
		detail := ""
		if lastSamples != nil {
			detail = "; last samples: " + lastSamples.Error()
		}
		return fmt.Errorf("custom metric %q did not report %d for Pods %v within %s%s%s: %w",
			s.hc.metricName, want, pods, s.hc.metricTimeout, detail, lastAPIErrorSuffix(lastAPIError), err)
	}
	return nil
}

// waitForState polls until check accepts a single observation of the HPA,
// Deployment and Pods, and returns that observation (or the last one seen).
//
// A wait ends in a conformance failure only if the workload was completely
// observed, and still not in the wanted state, within hpaObservationGrace of
// its end. A complete observation is one in which getHPAState and every API
// read made by check succeeded; a retryable error in either leaves the
// observation incomplete. If the API server stopped answering for longer
// than that, the HPA may have reached the state unseen, so the result is an
// environment error.
func (s *hpaScenario) waitForState(ctx context.Context, timeout time.Duration, check func(context.Context, hpaState) error) (hpaState, error) {
	var last hpaState
	var lastProblem, lastAPIError error
	var lastObserved time.Time
	err := wait.PollUntilContextTimeout(ctx, hpaPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		st, err := getHPAState(ctx, s.c, s.namespace)
		if err != nil {
			if ctx.Err() != nil {
				// The wait's own deadline cut the request short: a timeout,
				// not an access problem.
				return false, nil
			}
			lastAPIError = err
			if isRetryableAPIError(err) {
				return false, nil
			}
			// A permanent error reading the test's own objects (e.g.
			// Forbidden) is an access problem even after earlier observations
			// succeeded.
			return false, hpaEnvironmentError(fmt.Errorf("observe the test workload: %w", err))
		}
		last = st
		problem := check(ctx, st)
		switch {
		case ctx.Err() != nil:
			// Keep the last complete observation for the error message.
			return false, nil
		case isHPAEnvironmentError(problem):
			return false, problem
		case isRetryableAPIError(problem):
			lastAPIError = problem
			return false, nil
		}
		lastAPIError = nil
		lastObserved = time.Now()
		lastProblem = problem
		return problem == nil, nil
	})
	switch {
	case err == nil:
		return last, nil
	case isHPAEnvironmentError(err):
		return last, err
	case lastObserved.IsZero():
		// Never observed (e.g. the API server unreachable): the test's
		// access, not the workload, is the problem.
		return last, hpaEnvironmentError(fmt.Errorf("could not observe the test workload within %s%s: %w", timeout, lastAPIErrorSuffix(lastAPIError), err))
	case time.Since(lastObserved) > hpaObservationGrace:
		return last, hpaEnvironmentError(fmt.Errorf("the test workload was not observed for the last %s of the %s wait, so the HPA may have reached the wanted state unseen; last observation: %v%s: %w",
			time.Since(lastObserved).Round(time.Second), timeout, lastProblem, lastAPIErrorSuffix(lastAPIError), err))
	default:
		return last, fmt.Errorf("not reached within %s; last observation: %v%s: %w", timeout, lastProblem, lastAPIErrorSuffix(lastAPIError), err)
	}
}

// holdState requires check to accept every observation for window, ending
// with a successful observation at or after the end of the window, and
// returns the last observation. Retryable API errors skip an observation
// rather than failing it, but the window must not go unobserved for longer
// than hpaObservationGrace, or the HPA could have scaled and returned unseen.
func (s *hpaScenario) holdState(ctx context.Context, window time.Duration, check func(context.Context, hpaState) error) (hpaState, error) {
	start := time.Now()
	end := start.Add(window)
	lastObserved := start
	var last hpaState
	var lastAPIError error
	for {
		st, err := getHPAState(ctx, s.c, s.namespace)
		switch {
		case err == nil:
			last = st
			lastObserved = time.Now()
			if problem := check(ctx, st); problem != nil {
				return last, problem
			}
			if !time.Now().Before(end) {
				return last, nil
			}
		case isRetryableAPIError(err):
			lastAPIError = err
		default:
			return last, hpaEnvironmentError(fmt.Errorf("observe the test workload: %w", err))
		}
		if gap := time.Since(lastObserved); gap > hpaObservationGrace {
			return last, hpaEnvironmentError(fmt.Errorf("could not observe the HPA for %s during the %s window%s", gap.Round(time.Millisecond), window, lastAPIErrorSuffix(lastAPIError)))
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(hpaPollInterval):
		}
	}
}

// cleanup deletes the HPA first, so it cannot react to the Deployment's
// deletion, then the Deployment, and waits until no workload Pod object
// remains and, in DRA mode, no ResourceClaim in the namespace is still
// allocated, so a later accelerator test does not race the release. Every
// step is attempted even if an earlier one fails, all of them share one
// hpaCleanupTimeout deadline, so a request the API server never answers
// cannot block the test, and every failure is returned.
func (s *hpaScenario) cleanup(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, hpaCleanupTimeout)
	defer cancel()
	deleteIgnoringNotFound := func(what string, del func() error) error {
		err := hpaRetry(retry.DefaultBackoff, isRetryableAPIError, func() error {
			if err := del(); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("delete %s: %w", what, err)
		}
		return nil
	}
	foreground := metav1.DeletePropagationForeground
	errs := []error{
		deleteIgnoringNotFound("HorizontalPodAutoscaler", func() error {
			return s.c.AutoscalingV2().HorizontalPodAutoscalers(s.namespace).Delete(ctx, hpaWorkloadName, metav1.DeleteOptions{})
		}),
		deleteIgnoringNotFound("Deployment", func() error {
			return s.c.AppsV1().Deployments(s.namespace).Delete(ctx, hpaWorkloadName, metav1.DeleteOptions{PropagationPolicy: &foreground})
		}),
	}

	var remaining string
	var lastAPIError error
	err := wait.PollUntilContextCancel(ctx, hpaPollInterval, true, func(ctx context.Context) (bool, error) {
		pods, err := s.c.CoreV1().Pods(s.namespace).List(ctx, metav1.ListOptions{LabelSelector: hpaWorkloadSelector()})
		if err != nil {
			lastAPIError = err
			return false, nil
		}
		if len(pods.Items) > 0 {
			remaining = fmt.Sprintf("%d workload Pods", len(pods.Items))
			return false, nil
		}
		if s.mode == allocationModeDRA {
			claims, err := s.c.ResourceV1().ResourceClaims(s.namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				lastAPIError = err
				return false, nil
			}
			if err := checkHPAClaimsReleased(claims.Items, nil); err != nil {
				remaining = err.Error()
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		errs = append(errs, fmt.Errorf("workload Pods and allocations were not removed within %s (remaining: %s)%s: %w",
			hpaCleanupTimeout, remaining, lastAPIErrorSuffix(lastAPIError), err))
	}
	return errors.Join(errs...)
}

// hpaRetry is retry.OnError, except that it returns fn's last error. OnError
// returns nil when fn fails with an error wrapping a context deadline that is
// not retriable (it treats it as its own interruption), so a request cut off
// by the deadline would otherwise look successful.
func hpaRetry(backoff wait.Backoff, retriable func(error) bool, fn func() error) error {
	var last error
	err := retry.OnError(backoff, retriable, func() error {
		last = fn()
		return last
	})
	if err == nil {
		err = last
	}
	return err
}

// setHPAMetricValue changes the value every workload Pod reports.
func setHPAMetricValue(ctx context.Context, c kubernetes.Interface, namespace string, value int) error {
	return hpaRetry(retry.DefaultRetry, apierrors.IsConflict, func() error {
		cm, err := c.CoreV1().ConfigMaps(namespace).Get(ctx, hpaWorkloadName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[hpaMetricValueKey] = strconv.Itoa(value)
		_, err = c.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{})
		return err
	})
}
