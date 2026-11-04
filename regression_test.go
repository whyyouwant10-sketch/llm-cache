package llmcache

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCanonicalHashingAndAuthorizationPrivacy(t *testing.T) {
	headers1 := http.Header{
		"Content-Type":  {"application/json; charset=utf-8"},
		"Accept":        {"application/json"},
		"Authorization": {"Bearer first-secret-token"},
		"User-Agent":    {"first-client"},
		"X-Request-ID":  {"request-one"},
	}
	headers2 := http.Header{
		"Content-Type":  {" application/json "},
		"Accept":        {" application/json "},
		"Authorization": {"Bearer second-secret-token"},
		"User-Agent":    {"second-client"},
		"X-Request-ID":  {"request-two"},
	}
	body1 := []byte(`{"model":"m","messages":[{"role":"user","content":"hello"}],"metadata":{"b":2,"a":1}}`)
	body2 := []byte(`{ "metadata": { "a": 1, "b": 2 }, "messages": [ { "content": "hello", "role": "user" } ], "model": "m" }`)
	key1, err := NewKey("post", "/v1/chat/completions?b=two&a=one&a=two#ignored", headers1, body1)
	if err != nil {
		t.Fatal(err)
	}
	key2, err := NewKey("POST", "/v1/chat/completions?a=one&b=two&a=two", headers2, body2)
	if err != nil {
		t.Fatal(err)
	}
	if key1.CacheKey != key2.CacheKey || key1.RequestSHA256 != key2.RequestSHA256 {
		t.Fatalf("equivalent requests differ: keys %s/%s body hashes %s/%s", key1.CacheKey, key2.CacheKey, key1.RequestSHA256, key2.RequestSHA256)
	}
	if len(key1.CacheKey) != 64 || strings.ToLower(key1.CacheKey) != key1.CacheKey {
		t.Fatalf("cache key is not lowercase SHA-256 hex: %q", key1.CacheKey)
	}

	arrayKey, err := NewKey("POST", "/v1/chat/completions?a=one&b=two&a=two", headers1, []byte(`{"model":"m","messages":[{"role":"user","content":"different"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if arrayKey.CacheKey == key1.CacheKey {
		t.Fatal("meaningful body change reused the cache key")
	}
	acceptKey, err := NewKey("POST", "/v1/chat/completions?a=one&b=two&a=two", http.Header{"Content-Type": {"application/json"}, "Accept": {"text/event-stream"}}, body1)
	if err != nil {
		t.Fatal(err)
	}
	if acceptKey.CacheKey == key1.CacheKey {
		t.Fatal("semantic Accept header change reused the cache key")
	}

	dir := t.TempDir()
	store, err := Open(filepath.Join(dir, "cache.sqlite"), StoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put(context.Background(), key1, Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: []byte(`{"ok":true}`)}, time.Hour); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(dir, "cache.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var requestBody []byte
	var requestHash, responseHeaders string
	if err := db.QueryRow("SELECT request_body, request_sha256, response_headers_json FROM cache_entries").Scan(&requestBody, &requestHash, &responseHeaders); err != nil {
		t.Fatal(err)
	}
	if requestBody != nil {
		t.Fatalf("raw request body was persisted: %q", requestBody)
	}
	if requestHash != key1.RequestSHA256 || strings.Contains(requestHash+responseHeaders, "secret-token") {
		t.Fatalf("authorization material leaked into stored metadata: hash=%q headers=%q", requestHash, responseHeaders)
	}
	for _, suffix := range []string{"", "-wal"} {
		data, readErr := os.ReadFile(filepath.Join(dir, "cache.sqlite") + suffix)
		if readErr == nil && bytes.Contains(data, []byte("secret-token")) {
			t.Fatalf("authorization material leaked into SQLite file %s", suffix)
		}
	}
}

func TestEmbeddingsGetSetMissHitAndArrayOrder(t *testing.T) {
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "cache.sqlite"), StoreConfig{
		Clock:   func() time.Time { return now },
		Pricing: map[string]Pricing{"text-embedding-fixture": {InputMicroUSD: 5}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	fixture, err := os.ReadFile(filepath.Join("fixtures", "embeddings-200.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls int32
	mock := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}, "X-Upstream": {"must-not-replay"}},
			Body:       io.NopCloser(bytes.NewReader(fixture)),
			Request:    req,
		}, nil
	})
	transport := &Transport{Next: mock, Store: store, TTL: time.Hour}
	request := func(body string) *http.Request {
		req, requestErr := http.NewRequest(http.MethodPost, "http://provider.test/v1/embeddings?z=last&a=first", strings.NewReader(body))
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		req.Header.Set("Content-Type", "application/json")
		return req
	}
	first, err := transport.RoundTrip(request(`{"input":["one","two"],"model":"text-embedding-fixture"}`))
	if err != nil {
		t.Fatal(err)
	}
	firstBody, _ := io.ReadAll(first.Body)
	first.Body.Close()
	second, err := transport.RoundTrip(request(`{ "model": "text-embedding-fixture", "input": ["one", "two"] }`))
	if err != nil {
		t.Fatal(err)
	}
	secondBody, _ := io.ReadAll(second.Body)
	second.Body.Close()
	if atomic.LoadInt32(&calls) != 1 || !bytes.Equal(firstBody, secondBody) {
		t.Fatalf("canonical equivalent request was not a byte-identical hit: calls=%d", calls)
	}
	if second.Header.Get("X-Upstream") != "" {
		t.Fatal("unsafe response header replayed on hit")
	}
	third, err := transport.RoundTrip(request(`{"input":["two","one"],"model":"text-embedding-fixture"}`))
	if err != nil {
		t.Fatal(err)
	}
	third.Body.Close()
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("array order did not change cache identity: calls=%d", calls)
	}
	stats, err := store.Stats(context.Background(), StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Lookups != 3 || stats.Hits != 1 || stats.Misses != 2 || stats.TokensSaved != 3 || stats.CostSavedMicroUSD != 15 {
		t.Fatalf("unexpected embeddings stats: %+v", stats)
	}
}

func TestBoundedResponseAndDatabaseStorageFailOpen(t *testing.T) {
	responseBody := []byte(`{"long":"0123456789abcdef"}`)
	for _, testCase := range []struct {
		name    string
		config  StoreConfig
		respMax int64
	}{
		{name: "response-limit", config: StoreConfig{MaxResponseBytes: 8}, respMax: 8},
		{name: "database-limit", config: StoreConfig{MaxDatabaseBytes: 1}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "cache.sqlite"), testCase.config)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			var calls int32
			mock := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				atomic.AddInt32(&calls, 1)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(responseBody)), Request: req}, nil
			})
			transport := &Transport{Next: mock, Store: store, TTL: time.Hour, MaxResponseBytes: testCase.respMax}
			makeRequest := func() *http.Request {
				req, requestErr := http.NewRequest(http.MethodPost, "http://provider.test/v1/chat/completions", bytes.NewReader([]byte(`{"model":"m"}`)))
				if requestErr != nil {
					t.Fatal(requestErr)
				}
				req.Header.Set("Content-Type", "application/json")
				return req
			}
			for i := 0; i < 2; i++ {
				resp, roundTripErr := transport.RoundTrip(makeRequest())
				if roundTripErr != nil {
					t.Fatal(roundTripErr)
				}
				body, readErr := io.ReadAll(resp.Body)
				resp.Body.Close()
				if readErr != nil || !bytes.Equal(body, responseBody) {
					t.Fatalf("pass-through response changed: err=%v body=%q", readErr, body)
				}
			}
			if atomic.LoadInt32(&calls) != 2 {
				t.Fatalf("bounded storage unexpectedly served a hit: calls=%d", calls)
			}
			items, inspectErr := store.Inspect(context.Background(), Filter{})
			if inspectErr != nil {
				t.Fatal(inspectErr)
			}
			if len(items) != 0 {
				t.Fatalf("bounded storage created entries: %+v", items)
			}
		})
	}
}

func TestMalformedOversizedAndUnsupportedRequestsBypass(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "cache.sqlite"), StoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var calls int32
	var received []string
	mock := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		body, readErr := io.ReadAll(req.Body)
		if readErr != nil {
			t.Errorf("upstream read failed: %v", readErr)
		}
		received = append(received, string(body))
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"ok":true}`)), Request: req}, nil
	})
	transport := &Transport{Next: mock, Store: store, TTL: time.Hour, MaxRequestBytes: 32}
	makeRequest := func(path, contentType, body string) *http.Request {
		req, requestErr := http.NewRequest(http.MethodPost, "http://provider.test"+path, strings.NewReader(body))
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		return req
	}
	cases := []struct {
		path, contentType, body string
	}{
		{path: "/v1/chat/completions", contentType: "application/json", body: `{"model":`},
		{path: "/v1/chat/completions", contentType: "application/json", body: `{"model":"m","stream":true}`},
		{path: "/v1/chat/completions", contentType: "text/plain", body: `{"model":"m"}`},
		{path: "/v1/chat/completions", contentType: "application/json", body: strings.Repeat("x", 64)},
		{path: "/v1/other", contentType: "application/json", body: `{"model":"m"}`},
	}
	for _, testCase := range cases {
		resp, roundTripErr := transport.RoundTrip(makeRequest(testCase.path, testCase.contentType, testCase.body))
		if roundTripErr != nil {
			t.Fatal(roundTripErr)
		}
		resp.Body.Close()
	}
	if atomic.LoadInt32(&calls) != int32(len(cases)) {
		t.Fatalf("bypass requests did not reach upstream exactly once: calls=%d", calls)
	}
	if len(received) != len(cases) || received[0] != cases[0].body || received[3] != cases[3].body {
		t.Fatalf("upstream did not receive malformed/oversized bodies unchanged: %#v", received)
	}
	stats, err := store.Stats(context.Background(), StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Bypasses != int64(len(cases)) || stats.Lookups != 0 || stats.Misses != 0 {
		t.Fatalf("bypass stats incorrect: %+v", stats)
	}
	items, err := store.Inspect(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("bypass requests were cached: %+v", items)
	}
}

func TestInspectAndDryRunVersusApplyMaintenance(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "cache.sqlite"), StoreConfig{Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	makeKey := func(model, endpoint string) Key {
		key, keyErr := NewKey(http.MethodPost, endpoint, http.Header{"Content-Type": {"application/json"}}, []byte(`{"model":"`+model+`","input":"`+model+`"}`))
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		return key
	}
	put := func(key Key, body string, ttl time.Duration) {
		t.Helper()
		if putErr := store.Put(context.Background(), key, Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: []byte(body)}, ttl); putErr != nil {
			t.Fatal(putErr)
		}
	}
	modelA := makeKey("a", "/v1/chat/completions")
	modelB := makeKey("b", "/v1/embeddings")
	put(modelA, `{"ok":"a"}`, time.Hour)
	put(modelB, `{"ok":"b"}`, 24*time.Hour)
	if _, hit, lookupErr := store.Lookup(context.Background(), modelA); lookupErr != nil || !hit {
		t.Fatalf("expected model A hit before inspection: hit=%v err=%v", hit, lookupErr)
	}
	items, err := store.Inspect(context.Background(), Filter{Model: "a", Endpoint: "/v1/chat/completions"})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].CacheKey != modelA.CacheKey || items[0].HitCount != 1 {
		t.Fatalf("inspect filter/result incorrect: %+v", items)
	}
	statsBefore, err := store.Stats(context.Background(), StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	clearPlan, err := store.PlanClear(context.Background(), Filter{Model: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if clearPlan.Count != 1 || clearPlan.Bytes == 0 {
		t.Fatalf("unexpected clear plan: %+v", clearPlan)
	}
	statsAfterPlan, err := store.Stats(context.Background(), StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if statsAfterPlan != statsBefore {
		t.Fatalf("clear dry run changed stats: before=%+v after=%+v", statsBefore, statsAfterPlan)
	}
	items, err = store.Inspect(context.Background(), Filter{Model: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].CacheKey != modelA.CacheKey {
		t.Fatalf("clear dry run changed entries: %+v", items)
	}
	if err := store.ApplyClear(context.Background(), clearPlan); err != nil {
		t.Fatal(err)
	}
	items, err = store.Inspect(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].CacheKey != modelB.CacheKey {
		t.Fatalf("applied clear deleted the wrong entries: %+v", items)
	}
	statsAfterClear, err := store.Stats(context.Background(), StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if statsAfterClear != statsBefore {
		t.Fatalf("clear changed stats unexpectedly: before=%+v after=%+v", statsBefore, statsAfterClear)
	}

	now = now.Add(2 * time.Hour)
	expired := makeKey("expired", "/v1/chat/completions")
	live := makeKey("live", "/v1/chat/completions")
	put(expired, `{"expired":true}`, time.Second)
	put(live, `{"live":true}`, time.Hour)
	now = now.Add(2 * time.Second)
	prunePlan, err := store.PlanPrune(context.Background(), PrunePolicy{Expired: true})
	if err != nil {
		t.Fatal(err)
	}
	if prunePlan.Count != 1 || prunePlan.Keys[0] != expired.CacheKey {
		t.Fatalf("unexpected expired prune plan: %+v", prunePlan)
	}
	statsBeforePrune, err := store.Stats(context.Background(), StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	items, err = store.Inspect(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("prune dry run changed entries: %+v", items)
	}
	statsAfterPlan, err = store.Stats(context.Background(), StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if statsAfterPlan != statsBeforePrune {
		t.Fatalf("prune dry run changed stats: before=%+v after=%+v", statsBeforePrune, statsAfterPlan)
	}
	if err := store.ApplyPrune(context.Background(), prunePlan); err != nil {
		t.Fatal(err)
	}
	items, err = store.Inspect(context.Background(), Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("applied prune deleted the wrong entries: %+v", items)
	}
	for _, item := range items {
		if item.CacheKey == expired.CacheKey {
			t.Fatalf("applied prune retained expired entry: %+v", items)
		}
	}
	statsAfterPrune, err := store.Stats(context.Background(), StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if statsAfterPrune != statsBeforePrune {
		t.Fatalf("prune changed stats unexpectedly: before=%+v after=%+v", statsBeforePrune, statsAfterPrune)
	}
}
