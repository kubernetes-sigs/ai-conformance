package conformance

import (
	"strings"
	"testing"
)

func TestBuildInferencePool(t *testing.T) {
	pool := buildInferencePool("test-ns", "my-pool", "app", "backend", 8080)

	if pool.GetKind() != "InferencePool" {
		t.Errorf("kind: got %q, want InferencePool", pool.GetKind())
	}
	if pool.GetName() != "my-pool" {
		t.Errorf("name: got %q, want my-pool", pool.GetName())
	}
	if pool.GetNamespace() != "test-ns" {
		t.Errorf("namespace: got %q, want test-ns", pool.GetNamespace())
	}
	spec, _, _ := nestedMap(pool.Object, "spec")
	port, _, _ := nestedInt64(spec, "targetPortNumber")
	if port != 8080 {
		t.Errorf("targetPortNumber: got %d, want 8080", port)
	}
	sel, _, _ := nestedMap(spec, "selector")
	if sel["app"] != "backend" {
		t.Errorf("selector app: got %v, want backend", sel["app"])
	}
}

func TestBuildInferenceModel(t *testing.T) {
	model := buildInferenceModel("test-ns", "my-model-obj", "my-model-name", "my-pool")

	if model.GetKind() != "InferenceModel" {
		t.Errorf("kind: got %q, want InferenceModel", model.GetKind())
	}
	spec, _, _ := nestedMap(model.Object, "spec")
	if spec["modelName"] != "my-model-name" {
		t.Errorf("modelName: got %v, want my-model-name", spec["modelName"])
	}
	if spec["criticality"] != "Standard" {
		t.Errorf("criticality: got %v, want Standard", spec["criticality"])
	}
	poolRef, _, _ := nestedMap(spec, "poolRef")
	if poolRef["name"] != "my-pool" {
		t.Errorf("poolRef.name: got %v, want my-pool", poolRef["name"])
	}
}

func TestBuildGAIEGateway(t *testing.T) {
	gw := buildGAIEGateway("test-ns", "my-gw", "my-class", 80)

	if gw.GetKind() != "Gateway" {
		t.Errorf("kind: got %q, want Gateway", gw.GetKind())
	}
	spec, _, _ := nestedMap(gw.Object, "spec")
	if spec["gatewayClassName"] != "my-class" {
		t.Errorf("gatewayClassName: got %v, want my-class", spec["gatewayClassName"])
	}
	listeners, ok := spec["listeners"].([]any)
	if !ok || len(listeners) != 1 {
		t.Fatalf("listeners: got %v", spec["listeners"])
	}
	l, _ := listeners[0].(map[string]any)
	if l["protocol"] != "HTTP" {
		t.Errorf("listener protocol: got %v, want HTTP", l["protocol"])
	}
	if l["port"] != int64(80) {
		t.Errorf("listener port: got %v, want 80", l["port"])
	}
}

func TestBuildGAIEHTTPRoute(t *testing.T) {
	route := buildGAIEHTTPRoute("test-ns", "my-route", "my-gw", "my-pool", 8080)

	if route.GetKind() != "HTTPRoute" {
		t.Errorf("kind: got %q, want HTTPRoute", route.GetKind())
	}
	spec, _, _ := nestedMap(route.Object, "spec")
	parents, ok := spec["parentRefs"].([]any)
	if !ok || len(parents) != 1 {
		t.Fatalf("parentRefs: got %v", spec["parentRefs"])
	}
	parent, _ := parents[0].(map[string]any)
	if parent["name"] != "my-gw" {
		t.Errorf("parentRef name: got %v, want my-gw", parent["name"])
	}

	rules, ok := spec["rules"].([]any)
	if !ok || len(rules) != 1 {
		t.Fatalf("rules: got %v", spec["rules"])
	}
	rule, _ := rules[0].(map[string]any)
	backends, ok := rule["backendRefs"].([]any)
	if !ok || len(backends) != 1 {
		t.Fatalf("backendRefs: got %v", rule["backendRefs"])
	}
	backend, _ := backends[0].(map[string]any)
	if backend["kind"] != "InferencePool" {
		t.Errorf("backendRef kind: got %v, want InferencePool", backend["kind"])
	}
	if backend["name"] != "my-pool" {
		t.Errorf("backendRef name: got %v, want my-pool", backend["name"])
	}
	if backend["port"] != int64(8080) {
		t.Errorf("backendRef port: got %v, want 8080", backend["port"])
	}
}

func TestBuildGAIEBackend(t *testing.T) {
	pod, svc := buildGAIEBackend("test-ns", "test-model")

	if pod.Name != gaieBackendName {
		t.Errorf("pod name: got %q, want %q", pod.Name, gaieBackendName)
	}
	if pod.Namespace != "test-ns" {
		t.Errorf("pod namespace: got %q, want test-ns", pod.Namespace)
	}
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("containers: got %d, want 1", len(pod.Spec.Containers))
	}
	c := pod.Spec.Containers[0]
	if c.ReadinessProbe == nil {
		t.Error("pod is missing readiness probe")
	}
	if !strings.Contains(strings.Join(c.Command, " "), "python3") {
		t.Errorf("container command should invoke python3, got %v", c.Command)
	}
	found := false
	for _, env := range c.Env {
		if env.Name == "MODEL_NAME" && env.Value == "test-model" {
			found = true
		}
	}
	if !found {
		t.Errorf("MODEL_NAME env var not set to test-model in %v", c.Env)
	}

	if svc.Name != gaieBackendName {
		t.Errorf("service name: got %q, want %q", svc.Name, gaieBackendName)
	}
	if len(svc.Spec.Selector) == 0 {
		t.Error("service has no selector")
	}
	if _, ok := svc.Spec.Selector[gaieBackendLabel]; !ok {
		t.Errorf("service selector missing key %q", gaieBackendLabel)
	}
}

func TestGAIEProbeScript(t *testing.T) {
	for _, want := range []string{"GAIE_TEST: PASS", "GAIE_TEST: FAIL", "GAIE_TEST: DONE",
		"GATEWAY_ADDRESS", "MODEL_NAME", "GAIE_ROUTING_OK"} {
		if !strings.Contains(gaieProbeScript, want) {
			t.Errorf("gaieProbeScript missing expected string %q", want)
		}
	}
}

func TestGAIEBackendScript(t *testing.T) {
	for _, want := range []string{"GAIE_ROUTING_OK", "/completions", "/healthz", "/v1/models"} {
		if !strings.Contains(gaieBackendScript, want) {
			t.Errorf("gaieBackendScript missing expected string %q", want)
		}
	}
}

// nestedMap extracts a map[string]any from a nested object path.
func nestedMap(obj map[string]any, key string) (map[string]any, bool, error) {
	v, ok := obj[key]
	if !ok {
		return nil, false, nil
	}
	m, ok := v.(map[string]any)
	return m, ok, nil
}

// nestedInt64 extracts an int64 from a map by key.
func nestedInt64(obj map[string]any, key string) (int64, bool, error) {
	v, ok := obj[key]
	if !ok {
		return 0, false, nil
	}
	i, ok := v.(int64)
	return i, ok, nil
}
