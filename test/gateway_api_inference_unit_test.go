package conformance

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakediscovery "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGatewayAPIServed(t *testing.T) {
	client := fake.NewClientset()
	disc := client.Discovery().(*fakediscovery.FakeDiscovery)
	if err := gatewayAPIServed(disc); err == nil {
		t.Fatal("expected error when the group version is not served")
	}
	disc.Resources = []*metav1.APIResourceList{{
		GroupVersion: gatewayAPIGroupVersion.String(),
		APIResources: []metav1.APIResource{{Name: "gatewayclasses"}, {Name: "gateways"}},
	}}
	if err := gatewayAPIServed(disc); err == nil || !strings.Contains(err.Error(), "httproutes") {
		t.Fatalf("expected httproutes error, got %v", err)
	}
	disc.Resources[0].APIResources = append(disc.Resources[0].APIResources, metav1.APIResource{Name: "httproutes"})
	if err := gatewayAPIServed(disc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testGatewayClass(name, accepted string) *unstructured.Unstructured {
	gc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gatewayAPIGroupVersion.String(),
		"kind":       "GatewayClass",
		"metadata":   map[string]any{"name": name},
	}}
	if accepted != "" {
		gc.Object["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Accepted", "status": accepted}}}
	}
	return gc
}

func TestSelectGatewayClass(t *testing.T) {
	newDyn := func(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
		return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
			map[schema.GroupVersionResource]string{gatewayClassGVR: "GatewayClassList"}, objs...)
	}
	ctx := context.Background()
	tests := []struct {
		name       string
		objs       []runtime.Object
		configured string
		want       string
		accepted   []string
		wantErr    bool
	}{
		{name: "none", want: ""},
		{name: "none accepted", objs: []runtime.Object{testGatewayClass("x", "False"), testGatewayClass("y", "")}, want: ""},
		{name: "first accepted by name", objs: []runtime.Object{testGatewayClass("z", "True"), testGatewayClass("b", "True"), testGatewayClass("a", "False")}, want: "b", accepted: []string{"b", "z"}},
		{name: "configured accepted", objs: []runtime.Object{testGatewayClass("a", "True"), testGatewayClass("c", "True")}, configured: "c", want: "c"},
		{name: "configured not accepted", objs: []runtime.Object{testGatewayClass("c", "False")}, configured: "c", wantErr: true},
		{name: "configured missing", configured: "c", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, accepted, err := selectGatewayClass(ctx, newDyn(tt.objs...), tt.configured)
			if tt.wantErr != (err != nil) {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want || !slices.Equal(accepted, tt.accepted) {
				t.Fatalf("got %q %v, want %q %v", got, accepted, tt.want, tt.accepted)
			}
		})
	}
}

func TestGatewayStatusHelpers(t *testing.T) {
	gw := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{
		"addresses":  []any{map[string]any{"type": "IPAddress", "value": "10.0.0.1"}},
		"conditions": []any{map[string]any{"type": "Programmed", "status": "True"}, map[string]any{"type": "Accepted", "status": "False"}},
	}}}
	if got := gatewayAddress(gw); got != "10.0.0.1" {
		t.Fatalf("address = %q", got)
	}
	if !conditionTrue(gw.Object, "Programmed", "status", "conditions") || conditionTrue(gw.Object, "Accepted", "status", "conditions") {
		t.Fatal("conditionTrue mismatch")
	}
	if gatewayAddress(&unstructured.Unstructured{Object: map[string]any{}}) != "" {
		t.Fatal("expected empty address")
	}
	named := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"addresses": []any{
		map[string]any{"type": "example.com/NamedAddress", "value": "gw-internal"},
		map[string]any{"value": "10.0.0.2"},
	}}}}
	if got := gatewayAddress(named); got != "10.0.0.2" {
		t.Fatalf("address = %q, want IP after implementation-specific type", got)
	}

	parent := func(name, accepted, resolved string) any {
		return map[string]any{
			"parentRef": map[string]any{"name": name},
			"conditions": []any{
				map[string]any{"type": "Accepted", "status": accepted},
				map[string]any{"type": "ResolvedRefs", "status": resolved},
			},
		}
	}
	route := func(parents ...any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"parents": parents}}}
	}
	if !routeAcceptedByGateway(route(parent("other", "False", "False"), parent(gatewayAPIGatewayName, "True", "True"))) {
		t.Fatal("expected route accepted")
	}
	if !routeAcceptedByGateway(route(parent(gatewayAPIGatewayName, "False", "False"), parent(gatewayAPIGatewayName, "True", "True"))) {
		t.Fatal("expected a later accepted entry for the same Gateway to count")
	}
	if routeAcceptedByGateway(route(parent(gatewayAPIGatewayName, "True", "False"))) {
		t.Fatal("expected unresolved refs to fail")
	}
	if routeAcceptedByGateway(route(parent("other", "True", "True"))) {
		t.Fatal("expected other parent to be ignored")
	}
}

func TestBuildHTTPRoute(t *testing.T) {
	rules, _, _ := unstructured.NestedSlice(buildHTTPRoute("ns").Object, "spec", "rules")
	if len(rules) != 2 {
		t.Fatalf("got %d rules", len(rules))
	}
	headers, _, _ := unstructured.NestedSlice(rules[0].(map[string]any)["matches"].([]any)[0].(map[string]any), "headers")
	if h := headers[0].(map[string]any); h["name"] != gatewayAPIHeaderName || h["value"] != gatewayAPIHeaderValue {
		t.Fatalf("header match = %v", h)
	}
	refs := rules[1].(map[string]any)["backendRefs"].([]any)
	if refs[0].(map[string]any)["weight"] != int64(gatewayAPIWeightA) || refs[1].(map[string]any)["weight"] != int64(gatewayAPIWeightB) {
		t.Fatalf("weighted refs = %v", refs)
	}
}

func TestBuildGatewayAPIProbe(t *testing.T) {
	for addr, want := range map[string]string{
		"10.0.0.1":    "http://10.0.0.1:80/v1/chat/completions",
		"fd00::1":     "http://[fd00::1]:80/v1/chat/completions",
		"gw.internal": "http://gw.internal:80/v1/chat/completions",
	} {
		pod := buildGatewayAPIProbe("ns", addr, 10*time.Minute)
		env := map[string]string{}
		for _, e := range pod.Spec.Containers[0].Env {
			env[e.Name] = e.Value
		}
		if env["URL"] != want || env["READY_SECONDS"] != "600" || env["WEIGHT_V1"] != "0.7" || env["TOLERANCE"] != "0.05" {
			t.Fatalf("env for %s = %v, want URL %q, READY_SECONDS 600, WEIGHT_V1 0.7 and TOLERANCE 0.05", addr, env, want)
		}
	}
}

func TestParseGatewayProbeLogs(t *testing.T) {
	header, weighted, err := parseGatewayProbeLogs("HEADER model-v1=0 model-v2=19 other=1\nSAMPLE_ERROR error: HTTP Error 502: Bad Gateway\nHEADER model-v1=0 model-v2=20 other=0\nWEIGHTED model-v1=354 model-v2=145 other=1\nSAMPLE_ERROR error: timed out\nWEIGHTED model-v1=340 model-v2=160 other=0\n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if header[gatewayAPIModelV2] != 20 || header["other"] != 0 || len(weighted) != 2 || weighted[1][gatewayAPIModelV1] != 340 {
		t.Fatalf("got %v %v", header, weighted)
	}
	for _, logs := range []string{"", "HEADER model-v1=0 model-v2=20 other=0", "HEADER model-v1=x\nWEIGHTED model-v1=1"} {
		if _, _, err := parseGatewayProbeLogs(logs); err == nil {
			t.Fatalf("expected error for %q", logs)
		}
	}
}

func TestCheckWeightedRounds(t *testing.T) {
	r := func(a, b, other int) backendCounts {
		return backendCounts{gatewayAPIModelV1: a, gatewayAPIModelV2: b, "other": other}
	}
	tests := []struct {
		name   string
		rounds []backendCounts
		ok     bool
	}{
		{name: "exact", rounds: []backendCounts{r(350, 150, 0)}, ok: true},
		{name: "edge of tolerance", rounds: []backendCounts{r(325, 175, 0)}, ok: true},
		{name: "even split", rounds: []backendCounts{r(250, 250, 0)}},
		{name: "all to one backend", rounds: []backendCounts{r(500, 0, 0)}},
		{name: "failed requests", rounds: []backendCounts{r(350, 149, 1)}},
		{name: "no traffic", rounds: []backendCounts{r(0, 0, 0)}},
		{name: "retry passes", rounds: []backendCounts{r(320, 180, 0), r(352, 148, 0)}, ok: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkWeightedRounds(tt.rounds); (err == nil) != tt.ok {
				t.Fatalf("err = %v, ok %v", err, tt.ok)
			}
		})
	}
}

func TestGatewayAPIBudget(t *testing.T) {
	tests := []struct {
		name     string
		timeout  time.Duration
		left     time.Duration
		overhead time.Duration
		want     time.Duration
		ok       bool
	}{
		{name: "ample time", timeout: 10 * time.Minute, left: 40 * time.Minute, overhead: 13 * time.Minute, want: 10 * time.Minute, ok: true},
		{name: "capped", timeout: 10 * time.Minute, left: 17 * time.Minute, overhead: 13 * time.Minute, want: 4 * time.Minute, ok: true},
		{name: "capped after Pods started", timeout: 10 * time.Minute, left: 17 * time.Minute, overhead: 10 * time.Minute, want: 7 * time.Minute, ok: true},
		{name: "only overhead left", timeout: 10 * time.Minute, left: 13 * time.Minute, overhead: 13 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := gatewayAPIBudget(tt.timeout, tt.left, tt.overhead)
			if (err == nil) != tt.ok || got != tt.want {
				t.Fatalf("got %v, err %v; want %v, ok %v", got, err, tt.want, tt.ok)
			}
		})
	}
}
