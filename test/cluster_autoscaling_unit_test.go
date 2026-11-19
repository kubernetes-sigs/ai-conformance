package conformance

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestParseNodePoolLabel(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantKey   string
		wantValue string
		wantErr   string
	}{
		{
			name:      "simple label",
			input:     "agentpool=gpupool",
			wantKey:   "agentpool",
			wantValue: "gpupool",
		},
		{
			name:      "qualified label",
			input:     "cloud.google.com/gke-nodepool=gpu-pool",
			wantKey:   "cloud.google.com/gke-nodepool",
			wantValue: "gpu-pool",
		},
		{name: "empty", input: "", wantErr: "exactly one"},
		{name: "missing equals", input: "agentpool", wantErr: "exactly one"},
		{name: "multiple equals", input: "a=b=c", wantErr: "exactly one"},
		{name: "empty key", input: "=value", wantErr: "non-empty"},
		{name: "empty value", input: "key=", wantErr: "non-empty"},
		{name: "invalid key", input: "bad key=value", wantErr: "invalid label key"},
		{name: "invalid value", input: "key=bad/value", wantErr: "invalid label value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, value, err := parseNodePoolLabel(tt.input)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseNodePoolLabel(%q) error = %v, want containing %q", tt.input, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseNodePoolLabel(%q) unexpected error: %v", tt.input, err)
			}
			if key != tt.wantKey || value != tt.wantValue {
				t.Fatalf("parseNodePoolLabel(%q) = %q, %q; want %q, %q", tt.input, key, value, tt.wantKey, tt.wantValue)
			}
		})
	}
}

func TestValidateAutoscalerDurations(t *testing.T) {
	valid := []time.Duration{time.Minute, time.Minute, 10 * time.Minute, 30 * time.Second}
	if err := validateAutoscalerDurations(valid[0], valid[1], valid[2], valid[3]); err != nil {
		t.Fatalf("validateAutoscalerDurations unexpected error: %v", err)
	}

	tests := []struct {
		name                                string
		pending, scaleUp, scaleDown, stable time.Duration
	}{
		{name: "zero pending timeout", scaleUp: time.Minute, scaleDown: time.Minute},
		{name: "zero scale-up timeout", pending: time.Minute, scaleDown: time.Minute},
		{name: "zero scale-down timeout", pending: time.Minute, scaleUp: time.Minute},
		{name: "negative stability window", pending: time.Minute, scaleUp: time.Minute, scaleDown: time.Minute, stable: -time.Second},
		{name: "stability window equals scale-down timeout", pending: time.Minute, scaleUp: time.Minute, scaleDown: time.Minute, stable: time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateAutoscalerDurations(tt.pending, tt.scaleUp, tt.scaleDown, tt.stable); err == nil {
				t.Fatal("validateAutoscalerDurations expected error")
			}
		})
	}
}

func TestAutoscalerPodDeadlineSeconds(t *testing.T) {
	pending := 1500 * time.Millisecond
	scaleUp := 2500 * time.Millisecond
	scaleDown := 3500 * time.Millisecond
	want := int64((baselinePodTimeout + pending + scaleUp + scaleDown + autoscalerPodDeadlineBuffer + time.Second - 1) / time.Second)
	if got := autoscalerPodDeadlineSeconds(pending, scaleUp, scaleDown); got != want {
		t.Fatalf("autoscalerPodDeadlineSeconds = %d, want %d", got, want)
	}
}

func TestBuildAutoscalingPod(t *testing.T) {
	cfg := nvidiaConfig(t)
	const deadlineSeconds int64 = 7200

	t.Run("device plugin", func(t *testing.T) {
		pod, err := buildAutoscalingPod("ns", "pod", "run-1", "agentpool", "gpu", allocationModeDevicePlugin, cfg, deadlineSeconds, true)
		if err != nil {
			t.Fatalf("buildAutoscalingPod unexpected error: %v", err)
		}

		if pod.Labels[autoscalerRunLabelKey] != "run-1" {
			t.Fatalf("run label = %q, want run-1", pod.Labels[autoscalerRunLabelKey])
		}
		if pod.Spec.NodeSelector["agentpool"] != "gpu" {
			t.Fatalf("node selector = %v, want agentpool=gpu", pod.Spec.NodeSelector)
		}
		resourceName := corev1.ResourceName(cfg.ExtendedResource)
		if got := pod.Spec.Containers[0].Resources.Limits[resourceName]; got.Cmp(resource.MustParse("1")) != 0 {
			t.Fatalf("accelerator limit = %s, want 1", got.String())
		}
		verifyAutoscalingPodLifetime(t, pod, deadlineSeconds)
		verifyAutoscalingAntiAffinity(t, pod, "run-1")
	})

	t.Run("DRA", func(t *testing.T) {
		pod, err := buildAutoscalingPod("ns", "pod", "run-2", "agentpool", "gpu", allocationModeDRA, cfg, deadlineSeconds, true)
		if err != nil {
			t.Fatalf("buildAutoscalingPod unexpected error: %v", err)
		}

		if len(pod.Spec.ResourceClaims) != 1 || pod.Spec.ResourceClaims[0].Name != "claim" {
			t.Fatalf("Pod ResourceClaims = %v, want one claim", pod.Spec.ResourceClaims)
		}
		if len(pod.Spec.Containers[0].Resources.Claims) != 1 ||
			pod.Spec.Containers[0].Resources.Claims[0].Name != "claim" {
			t.Fatalf("container claims = %v, want one claim", pod.Spec.Containers[0].Resources.Claims)
		}
		verifyAutoscalingPodLifetime(t, pod, deadlineSeconds)
		verifyAutoscalingAntiAffinity(t, pod, "run-2")
	})

	t.Run("trigger omits anti-affinity", func(t *testing.T) {
		pod, err := buildAutoscalingPod("ns", "pod", "run-3", "agentpool", "gpu", allocationModeDRA, cfg, deadlineSeconds, false)
		if err != nil {
			t.Fatalf("buildAutoscalingPod unexpected error: %v", err)
		}
		if pod.Spec.Affinity != nil {
			t.Fatalf("trigger affinity = %v, want nil", pod.Spec.Affinity)
		}
		if _, ok := pod.Labels[autoscalerRunLabelKey]; ok {
			t.Fatalf("trigger labels = %v, want no autoscaler run label", pod.Labels)
		}
		verifyAutoscalingPodLifetime(t, pod, deadlineSeconds)
	})
}

func verifyAutoscalingPodLifetime(t *testing.T, pod *corev1.Pod, wantDeadlineSeconds int64) {
	t.Helper()
	if pod.Spec.ActiveDeadlineSeconds == nil || *pod.Spec.ActiveDeadlineSeconds != wantDeadlineSeconds {
		t.Fatalf("active deadline seconds = %v, want %d", pod.Spec.ActiveDeadlineSeconds, wantDeadlineSeconds)
	}
	if got := pod.Spec.Containers[0].Args; len(got) != 1 || got[0] != "sleep infinity" {
		t.Fatalf("container args = %v, want [sleep infinity]", got)
	}
}

func verifyAutoscalingAntiAffinity(t *testing.T, pod *corev1.Pod, runLabelValue string) {
	t.Helper()
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAntiAffinity == nil {
		t.Fatal("Pod anti-affinity is missing")
	}
	terms := pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 {
		t.Fatalf("required anti-affinity terms = %d, want 1", len(terms))
	}
	if terms[0].TopologyKey != corev1.LabelHostname {
		t.Fatalf("topology key = %q, want %q", terms[0].TopologyKey, corev1.LabelHostname)
	}
	if got := terms[0].LabelSelector.MatchLabels[autoscalerRunLabelKey]; got != runLabelValue {
		t.Fatalf("anti-affinity run label = %q, want %q", got, runLabelValue)
	}
}

func TestReadyPoolNodes(t *testing.T) {
	matchingReady := gpuNode("matching-ready", 1, true, false)
	matchingReady.Labels = map[string]string{"agentpool": "gpu"}
	matchingNotReady := gpuNode("matching-not-ready", 1, false, false)
	matchingNotReady.Labels = map[string]string{"agentpool": "gpu"}
	matchingUnschedulable := gpuNode("matching-unschedulable", 1, true, true)
	matchingUnschedulable.Labels = map[string]string{"agentpool": "gpu"}
	otherPool := gpuNode("other-pool", 1, true, false)
	otherPool.Labels = map[string]string{"agentpool": "cpu"}

	client := fake.NewClientset(matchingReady, matchingNotReady, matchingUnschedulable, otherPool)
	nodes, err := readyPoolNodes(context.Background(), client, "agentpool", "gpu")
	if err != nil {
		t.Fatalf("readyPoolNodes unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("readyPoolNodes returned %d nodes, want 1: %v", len(nodes), nodes)
	}
	if _, ok := nodes["matching-ready"]; !ok {
		t.Fatalf("readyPoolNodes = %v, want matching-ready", nodes)
	}
}

func TestPodUnschedulable(t *testing.T) {
	tests := []struct {
		name        string
		conditions  []corev1.PodCondition
		want        bool
		wantMessage string
	}{
		{name: "no conditions"},
		{
			name: "unschedulable",
			conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
				Reason: corev1.PodReasonUnschedulable, Message: "0/1 nodes are available",
			}},
			want:        true,
			wantMessage: "0/1 nodes are available",
		},
		{
			name: "scheduled",
			conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionTrue,
			}},
		},
		{
			name: "different reason",
			conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "SchedulingGated",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Status: corev1.PodStatus{Conditions: tt.conditions}}
			got, message := podUnschedulable(pod)
			if got != tt.want || message != tt.wantMessage {
				t.Fatalf("podUnschedulable() = %v, %q; want %v, %q", got, message, tt.want, tt.wantMessage)
			}
		})
	}
}

func TestVerifyBaselinePlacement(t *testing.T) {
	baseline := map[string]corev1.Node{
		"node-a": {ObjectMeta: metav1.ObjectMeta{Name: "node-a"}},
		"node-b": {ObjectMeta: metav1.ObjectMeta{Name: "node-b"}},
	}

	t.Run("covers every baseline node", func(t *testing.T) {
		pods := map[string]*corev1.Pod{
			"pod-a": {ObjectMeta: metav1.ObjectMeta{Name: "pod-a"}, Spec: corev1.PodSpec{NodeName: "node-a"}},
			"pod-b": {ObjectMeta: metav1.ObjectMeta{Name: "pod-b"}, Spec: corev1.PodSpec{NodeName: "node-b"}},
		}
		if err := verifyBaselinePlacement(pods, baseline); err != nil {
			t.Fatalf("verifyBaselinePlacement unexpected error: %v", err)
		}
	})

	t.Run("rejects duplicate placement", func(t *testing.T) {
		pods := map[string]*corev1.Pod{
			"pod-a": {ObjectMeta: metav1.ObjectMeta{Name: "pod-a"}, Spec: corev1.PodSpec{NodeName: "node-a"}},
			"pod-b": {ObjectMeta: metav1.ObjectMeta{Name: "pod-b"}, Spec: corev1.PodSpec{NodeName: "node-a"}},
		}
		if err := verifyBaselinePlacement(pods, baseline); err == nil {
			t.Fatal("verifyBaselinePlacement expected duplicate-placement error")
		}
	})
}

func TestSameNodeIdentitySet(t *testing.T) {
	original := map[string]corev1.Node{
		"node-a": {ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: types.UID("uid-a")}},
	}
	same := map[string]corev1.Node{
		"node-a": {ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: types.UID("uid-a")}},
	}
	replaced := map[string]corev1.Node{
		"node-a": {ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: types.UID("uid-b")}},
	}
	if !sameNodeIdentitySet(original, same) {
		t.Fatal("sameNodeIdentitySet rejected the same node UID")
	}
	if sameNodeIdentitySet(original, replaced) {
		t.Fatal("sameNodeIdentitySet accepted a replacement node with the same name")
	}
}

func TestVerifyPoolIsIdle(t *testing.T) {
	cfg := nvidiaConfig(t)
	poolNode := labeledNode("node-a", map[string]string{"agentpool": "gpu"}, true)
	poolNodes := map[string]corev1.Node{"node-a": *poolNode}

	t.Run("filters terminal Pods at the API server", func(t *testing.T) {
		client := fake.NewClientset()
		if err := verifyPoolIsIdle(context.Background(), client, "test", poolNodes, "agentpool", "gpu", allocationModeDevicePlugin, cfg); err != nil {
			t.Fatalf("verifyPoolIsIdle unexpected error: %v", err)
		}
		actions := client.Actions()
		if len(actions) != 1 {
			t.Fatalf("client actions = %d, want 1", len(actions))
		}
		listAction, ok := actions[0].(k8stesting.ListAction)
		if !ok {
			t.Fatalf("client action = %T, want ListAction", actions[0])
		}
		wantSelector, err := fields.ParseSelector(nonTerminalPodFieldSelector())
		if err != nil {
			t.Fatalf("parse expected Pod field selector: %v", err)
		}
		if got, want := listAction.GetListRestrictions().Fields.String(), wantSelector.String(); got != want {
			t.Fatalf("Pod field selector = %q, want %q", got, want)
		}
	})

	t.Run("ignores test and DaemonSet Pods", func(t *testing.T) {
		controller := true
		client := fake.NewClientset(
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "baseline", Namespace: "test"}, Spec: corev1.PodSpec{NodeName: "node-a"}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "daemon", Namespace: "system",
				OwnerReferences: []metav1.OwnerReference{{Kind: "DaemonSet", Name: "daemon", Controller: &controller}},
			}, Spec: corev1.PodSpec{NodeName: "node-a"}},
		)
		if err := verifyPoolIsIdle(context.Background(), client, "test", poolNodes, "agentpool", "gpu", allocationModeDevicePlugin, cfg); err != nil {
			t.Fatalf("verifyPoolIsIdle unexpected error: %v", err)
		}
	})

	t.Run("ignores transient platform validation Pods", func(t *testing.T) {
		client := fake.NewClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "nvidia-cuda-validator-66cwf", Namespace: "nvidia-gpu-operator"},
			Spec: corev1.PodSpec{
				NodeName:      "node-a",
				RestartPolicy: corev1.RestartPolicyOnFailure,
			},
		})
		if err := verifyPoolIsIdle(context.Background(), client, "test", poolNodes, "agentpool", "gpu", allocationModeDevicePlugin, cfg); err != nil {
			t.Fatalf("verifyPoolIsIdle unexpected error: %v", err)
		}
	})

	t.Run("rejects a batch Pod in a user namespace", func(t *testing.T) {
		client := fake.NewClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "user-batch", Namespace: "other"},
			Spec: corev1.PodSpec{
				NodeName:      "node-a",
				RestartPolicy: corev1.RestartPolicyNever,
			},
		})
		if err := verifyPoolIsIdle(context.Background(), client, "test", poolNodes, "agentpool", "gpu", allocationModeDevicePlugin, cfg); err == nil {
			t.Fatal("verifyPoolIsIdle expected foreign-workload error")
		}
	})

	t.Run("rejects a platform Pod that requests an accelerator", func(t *testing.T) {
		resourceName := corev1.ResourceName(cfg.ExtendedResource)
		client := fake.NewClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "platform-gpu-job", Namespace: "nvidia-gpu-operator"},
			Spec: corev1.PodSpec{
				NodeName:      "node-a",
				RestartPolicy: corev1.RestartPolicyOnFailure,
				Containers: []corev1.Container{{
					Name: "worker",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{resourceName: resource.MustParse("1")},
					},
				}},
			},
		})
		if err := verifyPoolIsIdle(context.Background(), client, "test", poolNodes, "agentpool", "gpu", allocationModeDevicePlugin, cfg); err == nil {
			t.Fatal("verifyPoolIsIdle expected foreign-workload error")
		}
	})

	t.Run("rejects a foreign scheduled Pod", func(t *testing.T) {
		client := fake.NewClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "other"},
			Spec:       corev1.PodSpec{NodeName: "node-a"},
		})
		if err := verifyPoolIsIdle(context.Background(), client, "test", poolNodes, "agentpool", "gpu", allocationModeDevicePlugin, cfg); err == nil {
			t.Fatal("verifyPoolIsIdle expected foreign-workload error")
		}
	})

	t.Run("rejects foreign pending accelerator demand", func(t *testing.T) {
		resourceName := corev1.ResourceName(cfg.ExtendedResource)
		client := fake.NewClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "other"},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{"agentpool": "gpu"},
				Containers: []corev1.Container{{
					Name: "worker",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{resourceName: resource.MustParse("1")},
					},
				}},
			},
		})
		if err := verifyPoolIsIdle(context.Background(), client, "test", poolNodes, "agentpool", "gpu", allocationModeDevicePlugin, cfg); err == nil {
			t.Fatal("verifyPoolIsIdle expected pending-demand error")
		}
	})

	t.Run("rejects other pending target-pool demand", func(t *testing.T) {
		client := fake.NewClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "foreign-cpu", Namespace: "other"},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{"agentpool": "gpu"},
			},
		})
		if err := verifyPoolIsIdle(context.Background(), client, "test", poolNodes, "agentpool", "gpu", allocationModeDevicePlugin, cfg); err == nil {
			t.Fatal("verifyPoolIsIdle expected target-pool demand error")
		}
	})

	t.Run("rejects foreign DRA claims outside the pool", func(t *testing.T) {
		templateName := "foreign-template"
		client := fake.NewClientset(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "foreign-dra", Namespace: "other"},
			Spec: corev1.PodSpec{
				NodeName:       "other-node",
				ResourceClaims: []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimTemplateName: &templateName}},
			},
		})
		if err := verifyPoolIsIdle(context.Background(), client, "test", poolNodes, "agentpool", "gpu", allocationModeDRA, cfg); err == nil {
			t.Fatal("verifyPoolIsIdle expected foreign DRA demand error")
		}
	})

	t.Run("rejects allocated ResourceClaims outside the test namespace", func(t *testing.T) {
		client := fake.NewClientset(claim("other", "allocated", true))
		if err := verifyPoolIsIdle(context.Background(), client, "test", poolNodes, "agentpool", "gpu", allocationModeDRA, cfg); err == nil {
			t.Fatal("verifyPoolIsIdle expected foreign allocated-claim error")
		}
	})
}

func TestWaitForScaleDown(t *testing.T) {
	baseline := gpuNode("baseline", 1, true, false)
	baseline.UID = types.UID("baseline-uid")
	baseline.Labels = map[string]string{"agentpool": "gpu"}
	scaled := gpuNode("scaled", 1, false, false)
	scaled.UID = types.UID("scaled-uid")
	scaled.Labels = map[string]string{"agentpool": "gpu"}
	baselineNodes := map[string]corev1.Node{baseline.Name: *baseline}
	scaledRef := nodeReference{name: scaled.Name, uid: scaled.UID}

	t.Run("accepts a retained NotReady Node object", func(t *testing.T) {
		client := fake.NewClientset(baseline, scaled)
		if err := waitForScaleDown(context.Background(), client, "agentpool", "gpu", scaledRef, baselineNodes, 0, time.Second); err != nil {
			t.Fatalf("waitForScaleDown unexpected error: %v", err)
		}
	})

	t.Run("rejects a Ready scaled Node even when cordoned", func(t *testing.T) {
		readyScaled := scaled.DeepCopy()
		readyScaled.Status.Conditions[0].Status = corev1.ConditionTrue
		readyScaled.Spec.Unschedulable = true
		client := fake.NewClientset(baseline, readyScaled)
		if err := waitForScaleDown(context.Background(), client, "agentpool", "gpu", scaledRef, baselineNodes, 0, 50*time.Millisecond); err == nil {
			t.Fatal("waitForScaleDown expected Ready scaled Node to block scale-down")
		}
	})

	t.Run("rejects baseline replacement", func(t *testing.T) {
		replacement := baseline.DeepCopy()
		replacement.UID = types.UID("replacement-uid")
		client := fake.NewClientset(replacement, scaled)
		if err := waitForScaleDown(context.Background(), client, "agentpool", "gpu", scaledRef, baselineNodes, 0, 50*time.Millisecond); err == nil {
			t.Fatal("waitForScaleDown expected baseline replacement to block scale-down")
		}
	})

	t.Run("rejects a cordoned baseline node", func(t *testing.T) {
		cordoned := baseline.DeepCopy()
		cordoned.Spec.Unschedulable = true
		client := fake.NewClientset(cordoned, scaled)
		if err := waitForScaleDown(context.Background(), client, "agentpool", "gpu", scaledRef, baselineNodes, 0, 50*time.Millisecond); err == nil {
			t.Fatal("waitForScaleDown expected cordoned baseline node to block scale-down")
		}
	})

	t.Run("rejects an additional Ready overshoot node", func(t *testing.T) {
		extra := gpuNode("extra", 1, true, false)
		extra.UID = types.UID("extra-uid")
		extra.Labels = map[string]string{"agentpool": "gpu"}
		client := fake.NewClientset(baseline, scaled, extra)
		if err := waitForScaleDown(context.Background(), client, "agentpool", "gpu", scaledRef, baselineNodes, 0, 50*time.Millisecond); err == nil {
			t.Fatal("waitForScaleDown expected extra Ready node to block scale-down")
		}
	})
}

func TestVerifyAutoscalerPoolCapacityRejectsDRABackedExtendedResource(t *testing.T) {
	cfg := nvidiaConfig(t)
	node := gpuNode("node-a", 1, true, false)
	nodes := map[string]corev1.Node{node.Name: *node}
	client := fake.NewClientset(extendedResourceDeviceClass("mapped", cfg.ExtendedResource))
	if err := verifyAutoscalerPoolCapacity(context.Background(), client, nodes, allocationModeDevicePlugin, cfg); err == nil || !strings.Contains(err.Error(), "would not be deterministic") {
		t.Fatalf("verifyAutoscalerPoolCapacity error = %v, want DRA-backed extended-resource rejection", err)
	}
}

func TestCreateBaselinePDB(t *testing.T) {
	client := fake.NewClientset()
	createBaselinePDB(context.Background(), t, client, "ns", "run-1", 3)
	pdb, err := client.PolicyV1().PodDisruptionBudgets("ns").Get(context.Background(), "autoscaler-baseline", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get created PodDisruptionBudget: %v", err)
	}
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.IntValue() != 3 {
		t.Fatalf("minAvailable = %v, want 3", pdb.Spec.MinAvailable)
	}
	if got := pdb.Spec.Selector.MatchLabels[autoscalerRunLabelKey]; got != "run-1" {
		t.Fatalf("run label = %q, want run-1", got)
	}
}
