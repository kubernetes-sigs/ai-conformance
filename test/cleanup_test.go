package conformance

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// Unit tests for the pod/claim cleanup helpers (fake clientset, no cluster).

func podWithClaimStatus(ns, name, claimName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Status: corev1.PodStatus{
			ResourceClaimStatuses: []corev1.PodResourceClaimStatus{{Name: "claim", ResourceClaimName: &claimName}},
		},
	}
}

func claim(ns, name string, allocated bool) *resourcev1.ResourceClaim {
	rc := &resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	if allocated {
		rc.Status.Allocation = &resourcev1.AllocationResult{}
	}
	return rc
}

func TestWaitForPodsRunning(t *testing.T) {
	ns := "ns"

	t.Run("returns all running pods", func(t *testing.T) {
		client := fake.NewClientset(
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: ns}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: ns}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
		)
		pods, err := waitForPodsRunning(context.Background(), client, ns, []string{"a", "b"}, time.Second)
		if err != nil {
			t.Fatalf("waitForPodsRunning unexpected error: %v", err)
		}
		if len(pods) != 2 || pods["a"] == nil || pods["b"] == nil {
			t.Fatalf("waitForPodsRunning returned %v, want Pods a and b", pods)
		}
	})

	t.Run("times out while a pod is pending", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		client := fake.NewClientset(
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: ns}, Status: corev1.PodStatus{Phase: corev1.PodRunning}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: ns}, Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{{
					Type:    corev1.PodScheduled,
					Status:  corev1.ConditionFalse,
					Reason:  corev1.PodReasonUnschedulable,
					Message: "0/3 nodes are available: 3 node(s) didn't match Pod's node affinity/selector.",
				}},
			}},
		)
		_, err := waitForPodsRunning(ctx, client, ns, []string{"a", "b"}, time.Second)
		if err == nil {
			t.Fatal("waitForPodsRunning expected timeout")
		}
		want := "pod b is Pending: 0/3 nodes are available: 3 node(s) didn't match Pod's node affinity/selector."
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
		if strings.Contains(err.Error(), "pod a") {
			t.Errorf("error %q mentions running pod a", err)
		}
	})

	t.Run("a Get failing at the deadline is a timeout, not an API error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		client := fake.NewClientset()
		client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			// What client-go returns when its rate limiter would have
			// to wait past the context deadline.
			<-ctx.Done()
			return true, nil, errors.New("client rate limiter Wait returned an error: rate: Wait(n=1) would exceed context deadline")
		})
		_, err := waitForPodsRunning(ctx, client, ns, []string{"a"}, time.Second)
		if err == nil {
			t.Fatal("waitForPodsRunning expected timeout")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error %q should wrap context.DeadlineExceeded", err)
		}
	})

	t.Run("fails fast on a permanent API error", func(t *testing.T) {
		client := fake.NewClientset()
		client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "a", errors.New("denied"))
		})
		start := time.Now()
		if _, err := waitForPodsRunning(context.Background(), client, ns, []string{"a"}, time.Second); err == nil {
			t.Fatal("waitForPodsRunning expected permanent API error")
		}
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Fatalf("permanent API error took %s to return", elapsed)
		}
	})
}

func TestPodGeneratedClaims(t *testing.T) {
	ns := "ns"

	t.Run("prefers the live pod", func(t *testing.T) {
		client := fake.NewClientset(podWithClaimStatus(ns, "p", "live-claim"))
		claims, err := podGeneratedClaims(context.Background(), client, ns, "p", podWithClaimStatus(ns, "p", "stale-claim"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(claims) != 1 || claims[0] != "live-claim" {
			t.Fatalf("claims = %v, want [live-claim]", claims)
		}
	})

	t.Run("falls back to the known pod when the live pod is gone", func(t *testing.T) {
		client := fake.NewClientset()
		claims, err := podGeneratedClaims(context.Background(), client, ns, "p", podWithClaimStatus(ns, "p", "known-claim"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(claims) != 1 || claims[0] != "known-claim" {
			t.Fatalf("claims = %v, want [known-claim]", claims)
		}
	})

	t.Run("surfaces non-NotFound GET errors while still using the known pod", func(t *testing.T) {
		client := fake.NewClientset()
		client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("etcdserver: request timed out")
		})
		claims, err := podGeneratedClaims(context.Background(), client, ns, "p", podWithClaimStatus(ns, "p", "known-claim"))
		if err == nil || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("expected the GET error to surface, got %v", err)
		}
		if len(claims) != 1 || claims[0] != "known-claim" {
			t.Fatalf("claims = %v, want [known-claim] despite the error", claims)
		}
	})

	t.Run("returns nothing when the pod is gone, nothing is known, and no claims exist", func(t *testing.T) {
		client := fake.NewClientset()
		claims, err := podGeneratedClaims(context.Background(), client, ns, "p", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(claims) != 0 {
			t.Fatalf("claims = %v, want none", claims)
		}
	})

	t.Run("scans owned claims when the live pod's claim status is not yet patched", func(t *testing.T) {
		// The resourceclaim controller creates the generated claim before
		// patching pod.status.resourceClaimStatuses; cleanup can race that
		// window and must still find the allocated claim.
		livePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: ns}}
		owned := claim(ns, "p-claim-race", true)
		owned.OwnerReferences = []metav1.OwnerReference{{Kind: "Pod", Name: "p", APIVersion: "v1"}}
		client := fake.NewClientset(livePod, owned)
		claims, err := podGeneratedClaims(context.Background(), client, ns, "p", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(claims) != 1 || claims[0] != "p-claim-race" {
			t.Fatalf("claims = %v, want [p-claim-race]", claims)
		}
	})

	t.Run("finds an orphaned claim via owner reference when the pod was never observed", func(t *testing.T) {
		orphan := claim(ns, "p-claim-x7k2", true)
		orphan.OwnerReferences = []metav1.OwnerReference{{Kind: "Pod", Name: "p", APIVersion: "v1"}}
		unrelated := claim(ns, "other-claim", true)
		unrelated.OwnerReferences = []metav1.OwnerReference{{Kind: "Pod", Name: "other-pod", APIVersion: "v1"}}
		client := fake.NewClientset(orphan, unrelated)
		claims, err := podGeneratedClaims(context.Background(), client, ns, "p", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(claims) != 1 || claims[0] != "p-claim-x7k2" {
			t.Fatalf("claims = %v, want [p-claim-x7k2]", claims)
		}
	})

	t.Run("propagates claim-list errors during the owner scan", func(t *testing.T) {
		client := fake.NewClientset()
		client.PrependReactor("list", "resourceclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("etcdserver: request timed out")
		})
		_, err := podGeneratedClaims(context.Background(), client, ns, "p", nil)
		if err == nil || !strings.Contains(err.Error(), "failed to list ResourceClaims") {
			t.Fatalf("expected the list error to surface, got %v", err)
		}
	})
}

func TestDeleteAndAwaitRelease(t *testing.T) {
	ns := "ns"

	t.Run("waits for pod deletion and released claim", func(t *testing.T) {
		client := fake.NewClientset(
			podWithClaimStatus(ns, "p", "c"),
			claim(ns, "c", false), // deallocated: no devices held
		)
		if err := deleteAndAwaitRelease(context.Background(), client, ns, "p", []string{"c"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("processes claims even when the pod is already gone", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		client := fake.NewClientset(claim(ns, "c", true)) // no pod; claim still allocated
		err := deleteAndAwaitRelease(ctx, client, ns, "p", []string{"c"})
		if err == nil || !strings.Contains(err.Error(), "ResourceClaim c not released") {
			t.Fatalf("expected the allocated claim to block cleanup, got %v", err)
		}
	})

	t.Run("claim already garbage-collected is fine", func(t *testing.T) {
		client := fake.NewClientset(podWithClaimStatus(ns, "p", "c"))
		if err := deleteAndAwaitRelease(context.Background(), client, ns, "p", []string{"c"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("post-deletion rescan finds a claim created after the pre-delete capture", func(t *testing.T) {
		// The resourceclaim controller can create the generated claim after
		// the caller captured claim names but before pod deletion; the final
		// owner scan after the pod is gone must still find it.
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		late := claim(ns, "p-late", true) // allocated, not in the passed claim names
		late.OwnerReferences = []metav1.OwnerReference{{Kind: "Pod", Name: "p", APIVersion: "v1"}}
		client := fake.NewClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: ns}}, late)
		err := deleteAndAwaitRelease(ctx, client, ns, "p", nil)
		if err == nil || !strings.Contains(err.Error(), "ResourceClaim p-late not released") {
			t.Fatalf("expected the rescan to find and wait on the late claim, got %v", err)
		}
	})

	t.Run("propagates pod delete API errors", func(t *testing.T) {
		client := fake.NewClientset(podWithClaimStatus(ns, "p", "c"))
		client.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("etcdserver: request timed out")
		})
		err := deleteAndAwaitRelease(context.Background(), client, ns, "p", nil)
		if err == nil || !strings.Contains(err.Error(), "failed to delete pod") {
			t.Fatalf("expected delete error to propagate, got %v", err)
		}
	})

	t.Run("retries transient pod delete API errors", func(t *testing.T) {
		client := fake.NewClientset(podWithClaimStatus(ns, "p", "c"))
		attempts := 0
		client.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			attempts++
			if attempts == 1 {
				return true, nil, apierrors.NewTooManyRequests("busy", 0)
			}
			return false, nil, nil
		})
		if err := deleteAndAwaitRelease(context.Background(), client, ns, "p", nil); err != nil {
			t.Fatalf("deleteAndAwaitRelease unexpected error: %v", err)
		}
		if attempts != 2 {
			t.Fatalf("delete attempts = %d, want 2", attempts)
		}
	})
}

func TestIsRetryableAPIError(t *testing.T) {
	if !isRetryableAPIError(apierrors.NewTooManyRequests("busy", 0)) {
		t.Fatal("TooManyRequests should be retryable")
	}
	if !isRetryableAPIError(&url.Error{Op: "Get", URL: "https://apiserver", Err: os.ErrDeadlineExceeded}) {
		t.Fatal("transport timeout should be retryable")
	}
	if isRetryableAPIError(apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "p", errors.New("denied"))) {
		t.Fatal("Forbidden should not be retryable")
	}
}

func TestDeletePodAndWait(t *testing.T) {
	client := fake.NewClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"}})
	getAttempts := 0
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		getAttempts++
		if getAttempts == 1 {
			return true, nil, errors.New("temporary read failure")
		}
		return false, nil, nil
	})
	if err := deletePodAndWait(context.Background(), client, "ns", "p", nil); err != nil {
		t.Fatalf("deletePodAndWait unexpected error after confirmed release: %v", err)
	}
}
