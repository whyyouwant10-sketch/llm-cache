package llmcache

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCanonicalKeyIgnoresOrderAuthAndNoise(t *testing.T) {
	h1 := http.Header{"Content-Type": {"application/json"}, "Authorization": {"Bearer one"}, "User-Agent": {"one"}}
	h2 := http.Header{"Content-Type": {" application/json; charset=utf-8 "}, "Authorization": {"Bearer two"}, "X-Request-ID": {"different"}}
	k1, err := NewKey("post", "/v1/chat/completions?b=two&a=one", h1, []byte(`{"model":"m","messages":[{"content":"x","role":"user"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	k2, err := NewKey("POST", "/v1/chat/completions?a=one&b=two", h2, []byte(`{ "messages": [ { "role": "user", "content": "x" } ], "model": "m" }`))
	if err != nil {
		t.Fatal(err)
	}
	if k1.CacheKey != k2.CacheKey {
		t.Fatalf("equivalent requests differ: %s != %s", k1.CacheKey, k2.CacheKey)
	}
	if k1.RequestSHA256 != k2.RequestSHA256 {
		t.Fatal("canonical body hashes differ")
	}
	if _, err := CanonicalizeJSON([]byte(`{"x":1,"x":2}`)); err == nil {
		t.Fatal("duplicate object member accepted")
	}
	if _, err := CanonicalizeJSON([]byte(`{"x":01}`)); err == nil {
		t.Fatal("invalid number accepted")
	}
}

func TestTransportHitTTLAndStats(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	store, err := Open(filepath.Join(dir, "cache.sqlite"), StoreConfig{Clock: func() time.Time { return now }, Pricing: map[string]Pricing{"m": {InputMicroUSD: 2, OutputMicroUSD: 3}}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	fixture, err := os.ReadFile(filepath.Join("fixtures", "chat-completion-200.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls int32
	mock := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		body, _ := io.ReadAll(req.Body)
		if !strings.Contains(string(body), `"model"`) {
			t.Errorf("upstream got unexpected body: %s", body)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}, "X-Upstream": {"ignored"}}, Body: io.NopCloser(strings.NewReader(string(fixture))), Request: req}, nil
	})
	transport := &Transport{Next: mock, Store: store, TTL: time.Hour}
	request := func() *http.Request {
		r, _ := http.NewRequest(http.MethodPost, "http://provider.test/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"hi"}],"model":"m"}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer secret")
		return r
	}
	first, err := transport.RoundTrip(request())
	if err != nil {
		t.Fatal(err)
	}
	firstBody, _ := io.ReadAll(first.Body)
	first.Body.Close()
	second, err := transport.RoundTrip(request())
	if err != nil {
		t.Fatal(err)
	}
	secondBody, _ := io.ReadAll(second.Body)
	second.Body.Close()
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("upstream calls = %d, want 1", calls)
	}
	if string(firstBody) != string(secondBody) {
		t.Fatal("hit body changed")
	}
	if second.Header.Get("X-Upstream") != "" {
		t.Fatal("unsafe response header replayed")
	}
	stats, err := store.Stats(context.Background(), StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Lookups != 2 || stats.Hits != 1 || stats.Misses != 1 || stats.TokensSaved != 7 || stats.CostSavedMicroUSD != 16 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	now = now.Add(2 * time.Hour)
	third, err := transport.RoundTrip(request())
	if err != nil {
		t.Fatal(err)
	}
	third.Body.Close()
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("expired entry was served, calls = %d", calls)
	}

	noWrite := &Transport{Next: mock, Store: store, TTL: 0}
	request2 := request()
	request2.Body = io.NopCloser(strings.NewReader(`{"messages":[{"role":"user","content":"different"}],"model":"m"}`))
	response, err := noWrite.RoundTrip(request2)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	key2, err := NewKey(request2.Method, request2.URL.RequestURI(), request2.Header, []byte(`{"messages":[{"role":"user","content":"different"}],"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, hit, err := store.Lookup(context.Background(), key2); err != nil {
		t.Fatal(err)
	} else if hit {
		t.Fatal("TTL zero wrote an entry")
	}
}

func TestMaintenanceDryRunDoesNotDelete(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	store, err := Open(filepath.Join(dir, "cache.sqlite"), StoreConfig{Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key, err := NewKey("POST", "/v1/embeddings", http.Header{"Content-Type": {"application/json"}}, []byte(`{"model":"e","input":["x"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), key, Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: []byte(`{"data":[]}`)}, time.Hour); err != nil {
		t.Fatal(err)
	}
	plan, err := store.PlanClear(context.Background(), Filter{Model: "e"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Count != 1 {
		t.Fatalf("planned %d rows", plan.Count)
	}
	if _, hit, err := store.Lookup(context.Background(), key); err != nil {
		t.Fatal(err)
	} else if !hit {
		t.Fatal("dry-run planning changed the entry")
	}
	if err := store.ApplyClear(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := store.Lookup(context.Background(), key); err != nil {
		t.Fatal(err)
	} else if hit {
		t.Fatal("applied plan did not delete entry")
	}
}

func TestBypassesAndErrorsAreRecorded(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "cache.sqlite"), StoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var calls int32
	mock := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return &http.Response{StatusCode: 429, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":"busy"}`)), Request: req}, nil
	})
	transport := &Transport{Next: mock, Store: store, TTL: time.Hour}
	stream, _ := http.NewRequest(http.MethodPost, "http://provider.test/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true}`))
	stream.Header.Set("Content-Type", "application/json")
	response, err := transport.RoundTrip(stream)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	errorRequest, _ := http.NewRequest(http.MethodPost, "http://provider.test/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	errorRequest.Header.Set("Content-Type", "application/json")
	response, err = transport.RoundTrip(errorRequest)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	stats, err := store.Stats(context.Background(), StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || stats.Bypasses != 1 || stats.Misses != 1 || stats.UpstreamErrors != 1 {
		t.Fatalf("unexpected bypass/error stats: calls=%d stats=%+v", calls, stats)
	}
	items, err := store.Inspect(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("error response was cached: %+v", items)
	}
}

func mustKey(t *testing.T, req *http.Request) Key {
	t.Helper()
	body, _ := io.ReadAll(req.Body)
	req.Body = io.NopCloser(strings.NewReader(string(body)))
	key, err := NewKey(req.Method, req.URL.RequestURI(), req.Header, body)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
