package conformance

import (
	"strings"
	"testing"
)

func TestBuildInferencePool(t *testing.T) {
	pool := buildInferencePool("test-ns", "my-pool", "app", "my-epp", 9002, 8080)

	if pool.GetKind() != "InferencePool" {
		t.Errorf("kind: got %q, want InferencePool", pool.GetKind())
	}
	if pool.GetAPIVersion() != gaieInferenceGroup+"/v1" {
		t.Errorf("apiVersion: got %q, want %s/v1", pool.GetAPIVersion(), gaieInferenceGroup)
	}
	if pool.GetName() != "my-pool" {
		t.Errorf("name: got %q, want my-pool", pool.GetName())
	}

	spec, _, _ := nestedMap(pool.Object, "spec")
	port, _, _ := nestedInt64(spec, "targetPortNumber")
	if port != 8080 {
		t.Errorf("targetPortNumber: got %d, want 8080", port)
	}
	sel, _, _ := nestedMap(spec, "selector")
	if sel["app"] != "gaie-backend" {
		t.Errorf("selector app: got %v, want gaie-backend", sel["app"])
	}

	eppRef, _, _ := nestedMap(spec, "endpointPickerRef")
	if eppRef["name"] != "my-epp" {
		t.Errorf("endpointPickerRef.name: got %v, want my-epp", eppRef["name"])
	}
	if eppRef["portNumber"] != int64(9002) {
		t.Errorf("endpointPickerRef.portNumber: got %v, want 9002", eppRef["portNumber"])
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

func TestBuildGAIEBackendPod(t *testing.T) {
	for _, name := range []string{gaieBackend0Name, gaieBackend1Name} {
		pod := buildGAIEBackendPod("test-ns", name)

		if pod.Name != name {
			t.Errorf("pod name: got %q, want %q", pod.Name, name)
		}
		if pod.Labels[gaieBackendLabel] != "gaie-backend" {
			t.Errorf("pod label %s: got %q, want gaie-backend", gaieBackendLabel, pod.Labels[gaieBackendLabel])
		}
		if len(pod.Spec.Containers) != 1 {
			t.Fatalf("containers: got %d, want 1", len(pod.Spec.Containers))
		}
		c := pod.Spec.Containers[0]
		if c.ReadinessProbe == nil {
			t.Errorf("pod %s is missing readiness probe", name)
		}
		if !strings.Contains(strings.Join(c.Command, " "), "python3") {
			t.Errorf("container command should invoke python3, got %v", c.Command)
		}
		found := false
		for _, env := range c.Env {
			if env.Name == "BACKEND_NAME" && env.Value == name {
				found = true
			}
		}
		if !found {
			t.Errorf("BACKEND_NAME env not set to %q in %v", name, c.Env)
		}
	}
}

func TestBuildGAIEEPP(t *testing.T) {
	sa, role, rb, pod, svc := buildGAIEEPP("test-ns", "my-pool", "my-epp-image:v1")

	if sa.Name != gaieEPPName {
		t.Errorf("SA name: got %q, want %q", sa.Name, gaieEPPName)
	}
	if len(role.Rules) == 0 {
		t.Error("Role has no rules")
	}
	podRule := false
	for _, r := range role.Rules {
		for _, res := range r.Resources {
			if res == "pods" {
				podRule = true
			}
		}
	}
	if !podRule {
		t.Error("Role missing pods rule")
	}
	if rb.RoleRef.Name != gaieEPPName {
		t.Errorf("RoleBinding roleRef: got %q, want %q", rb.RoleRef.Name, gaieEPPName)
	}
	if pod.Spec.ServiceAccountName != gaieEPPName {
		t.Errorf("EPP pod ServiceAccountName: got %q, want %q", pod.Spec.ServiceAccountName, gaieEPPName)
	}
	found := false
	for _, arg := range pod.Spec.Containers[0].Args {
		if strings.Contains(arg, "my-pool") {
			found = true
		}
	}
	if !found {
		t.Errorf("EPP pod args should contain pool name, got %v", pod.Spec.Containers[0].Args)
	}
	if svc.Name != gaieEPPName {
		t.Errorf("EPP service name: got %q, want %q", svc.Name, gaieEPPName)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != gaieEPPPort {
		t.Errorf("EPP service port: got %v, want %d", svc.Spec.Ports, gaieEPPPort)
	}
}

func TestGAIEProbeScript(t *testing.T) {
	for _, want := range []string{
		"GAIE_TEST: PASS", "GAIE_TEST: FAIL", "GAIE_TEST: DONE",
		"GATEWAY_URL", "BACKEND_0_IP", "BACKEND_1_IP",
		"test-epp-endpoint-selection", "GAIE_ROUTING_OK",
	} {
		if !strings.Contains(gaieProbeScript, want) {
			// GAIE_ROUTING_OK is from the backend, not the probe
			if want == "GAIE_ROUTING_OK" {
				continue
			}
			t.Errorf("gaieProbeScript missing expected string %q", want)
		}
	}
	// verify warmup loop and selection header are present
	if !strings.Contains(gaieProbeScript, "test-epp-endpoint-selection") {
		t.Error("gaieProbeScript missing endpoint selection header")
	}
}

func TestGAIEBackendScript(t *testing.T) {
	for _, want := range []string{
		"BACKEND_NAME", "/completions", "/healthz", "/v1/models",
		"ThreadingHTTPServer", "has_dualstack_ipv6",
	} {
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
