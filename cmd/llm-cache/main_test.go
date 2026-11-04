package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	llmcache "github.com/whyyouwant10-sketch/llm-cache"
)

func TestProxyHandlerForwardsAndCachesRequests(t *testing.T) {
	var calls int32
	var receivedAuth, receivedPath, receivedBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		receivedAuth = r.Header.Get("Authorization")
		receivedPath = r.URL.RequestURI()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("upstream body read failed: %v", err)
		}
		receivedBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream", "do-not-replay")
		w.Header().Set("Set-Cookie", "session=private")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"provider": "keeps formatting", "ok": true}`)
	}))
	defer upstream.Close()
	base, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	store, err := llmcache.Open(filepath.Join(t.TempDir(), "cache.sqlite"), llmcache.StoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	proxy := httptest.NewServer(proxyHandler(base, store, time.Hour))
	defer proxy.Close()

	request := func() *http.Request {
		req, requestErr := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions?b=two&a=one", strings.NewReader(`{"model":"m","messages":[]}`))
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer proxy-secret")
		return req
	}
	client := &http.Client{}
	first, err := client.Do(request())
	if err != nil {
		t.Fatal(err)
	}
	firstBody, _ := io.ReadAll(first.Body)
	first.Body.Close()
	second, err := client.Do(request())
	if err != nil {
		t.Fatal(err)
	}
	secondBody, _ := io.ReadAll(second.Body)
	second.Body.Close()
	if first.StatusCode != http.StatusCreated || second.StatusCode != http.StatusCreated {
		t.Fatalf("proxy status changed: first=%d second=%d", first.StatusCode, second.StatusCode)
	}
	if !bytes.Equal(firstBody, secondBody) || string(secondBody) != `{"provider": "keeps formatting", "ok": true}` {
		t.Fatalf("proxy hit body was not byte-identical: %q / %q", firstBody, secondBody)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("proxy did not cache the second request: upstream calls=%d", calls)
	}
	if receivedAuth != "Bearer proxy-secret" || receivedPath != "/v1/chat/completions?b=two&a=one" || receivedBody != `{"model":"m","messages":[]}` {
		t.Fatalf("proxy did not forward request unchanged: auth=%q path=%q body=%q", receivedAuth, receivedPath, receivedBody)
	}
	if second.Header.Get("X-Upstream") != "" || second.Header.Get("Set-Cookie") != "" {
		t.Fatalf("unsafe upstream headers replayed on hit: headers=%v", second.Header)
	}
}

func TestCLIInspectClearAndPruneDryRunApply(t *testing.T) {
	binary := buildCLI(t)
	dbPath := filepath.Join(t.TempDir(), "cache.sqlite")
	now := time.Now().UTC().Truncate(time.Second)
	store, err := llmcache.Open(dbPath, llmcache.StoreConfig{Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	key, err := llmcache.NewKey(http.MethodPost, "/v1/chat/completions", http.Header{"Content-Type": {"application/json"}}, []byte(`{"model":"cli-model"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), key, llmcache.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: []byte(`{"ok":true}`)}, time.Hour); err != nil {
		t.Fatal(err)
	}
	statsBefore, err := store.Stats(context.Background(), llmcache.StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	store.Close()

	inspectOutput := runCLI(t, binary, "inspect", "--db", dbPath, "--json")
	var inspected []llmcache.Inspection
	if err := json.Unmarshal(inspectOutput, &inspected); err != nil {
		t.Fatalf("inspect did not return JSON: %v; output=%s", err, inspectOutput)
	}
	if len(inspected) != 1 || inspected[0].CacheKey != key.CacheKey {
		t.Fatalf("inspect returned unexpected entries: %+v", inspected)
	}
	dryClear := runCLI(t, binary, "clear", "--db", dbPath, "--model", "cli-model")
	if !strings.Contains(string(dryClear), "clear: 1 entries") {
		t.Fatalf("clear dry-run output missing plan: %s", dryClear)
	}
	store, err = llmcache.Open(dbPath, llmcache.StoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	items, err := store.Inspect(context.Background(), llmcache.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	statsAfterDryClear, err := store.Stats(context.Background(), llmcache.StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || statsAfterDryClear != statsBefore {
		t.Fatalf("clear dry-run mutated database: items=%+v stats=%+v before=%+v", items, statsAfterDryClear, statsBefore)
	}
	store.Close()
	applyClear := runCLI(t, binary, "clear", "--db", dbPath, "--model", "cli-model", "--apply")
	if !strings.Contains(string(applyClear), "applied") {
		t.Fatalf("clear apply output missing confirmation: %s", applyClear)
	}
	store, err = llmcache.Open(dbPath, llmcache.StoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	items, err = store.Inspect(context.Background(), llmcache.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("clear apply retained entries: %+v", items)
	}
	statsAfterClear, err := store.Stats(context.Background(), llmcache.StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if statsAfterClear != statsBefore {
		t.Fatalf("clear apply changed stats: before=%+v after=%+v", statsBefore, statsAfterClear)
	}
	store.Close()

	oldStore, err := llmcache.Open(dbPath, llmcache.StoreConfig{Clock: func() time.Time { return now.Add(-2 * time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	oldKey, err := llmcache.NewKey(http.MethodPost, "/v1/chat/completions", http.Header{"Content-Type": {"application/json"}}, []byte(`{"model":"expired"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := oldStore.Put(context.Background(), oldKey, llmcache.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: []byte(`{"expired":true}`)}, time.Second); err != nil {
		t.Fatal(err)
	}
	oldStore.Close()
	liveStore, err := llmcache.Open(dbPath, llmcache.StoreConfig{Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	liveKey, err := llmcache.NewKey(http.MethodPost, "/v1/chat/completions", http.Header{"Content-Type": {"application/json"}}, []byte(`{"model":"live"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := liveStore.Put(context.Background(), liveKey, llmcache.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: []byte(`{"live":true}`)}, time.Hour); err != nil {
		t.Fatal(err)
	}
	statsBeforePrune, err := liveStore.Stats(context.Background(), llmcache.StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	liveStore.Close()
	dryPrune := runCLI(t, binary, "prune", "--db", dbPath, "--expired")
	if !strings.Contains(string(dryPrune), "prune: 1 entries") {
		t.Fatalf("prune dry-run output missing plan: %s", dryPrune)
	}
	checkStore, err := llmcache.Open(dbPath, llmcache.StoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	items, err = checkStore.Inspect(context.Background(), llmcache.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	statsAfterDryPrune, err := checkStore.Stats(context.Background(), llmcache.StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || statsAfterDryPrune != statsBeforePrune {
		t.Fatalf("prune dry-run mutated database: items=%+v stats=%+v before=%+v", items, statsAfterDryPrune, statsBeforePrune)
	}
	checkStore.Close()
	applyPrune := runCLI(t, binary, "prune", "--db", dbPath, "--expired", "--apply")
	if !strings.Contains(string(applyPrune), "applied") {
		t.Fatalf("prune apply output missing confirmation: %s", applyPrune)
	}
	checkStore, err = llmcache.Open(dbPath, llmcache.StoreConfig{})
	if err != nil {
		t.Fatal(err)
	}
	items, err = checkStore.Inspect(context.Background(), llmcache.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].CacheKey != liveKey.CacheKey {
		t.Fatalf("prune apply retained or deleted wrong entries: %+v", items)
	}
	statsAfterPrune, err := checkStore.Stats(context.Background(), llmcache.StatsFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if statsAfterPrune != statsBeforePrune {
		t.Fatalf("prune apply changed stats: before=%+v after=%+v", statsBeforePrune, statsAfterPrune)
	}
	checkStore.Close()
}

func buildCLI(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "llm-cache")
	cmd := exec.Command("go", "build", "-o", path, ".")
	cmd.Dir, _ = os.Getwd()
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, output)
	}
	return path
}

func runCLI(t *testing.T, binary string, args ...string) []byte {
	t.Helper()
	output, err := exec.Command(binary, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s failed: %v\n%s", binary, strings.Join(args, " "), err, output)
	}
	return output
}
