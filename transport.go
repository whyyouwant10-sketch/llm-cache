package llmcache

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Transport struct {
	Next             http.RoundTripper
	Store            Store
	TTL              time.Duration
	MaxRequestBytes  int64
	MaxResponseBytes int64
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.Next == nil {
		t.Next = http.DefaultTransport
	}
	if t.Store == nil {
		return t.Next.RoundTrip(req)
	}
	eligible, body, key, err := t.prepare(req)
	if !eligible {
		_ = t.record(req.Context(), "bypass")
		return t.Next.RoundTrip(req)
	}
	entry, hit, err := t.Store.Lookup(req.Context(), key)
	if err == nil && hit {
		return replay(req, entry), nil
	}
	upstreamReq := req
	if body != nil {
		upstreamReq = req.Clone(req.Context())
		upstreamReq.Body = io.NopCloser(bytes.NewReader(body))
		upstreamReq.ContentLength = int64(len(body))
	}
	resp, upstreamErr := t.Next.RoundTrip(upstreamReq)
	if upstreamErr != nil {
		_ = t.record(req.Context(), "error")
		return resp, upstreamErr
	}
	if resp == nil {
		_ = t.record(req.Context(), "error")
		return resp, nil
	}
	responseBody, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	resp.Body = io.NopCloser(bytes.NewReader(responseBody))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = t.record(req.Context(), "error")
		return resp, nil
	}
	if int64(len(responseBody)) <= t.responseLimit() && strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "application/json") {
		if err := t.Store.Put(req.Context(), key, Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: responseBody}, t.TTL); err == nil && t.TTL != 0 {
			return resp, nil
		}
	}
	_ = body
	_ = t.record(req.Context(), "miss")
	return resp, nil
}

func (t *Transport) prepare(req *http.Request) (bool, []byte, Key, error) {
	if req.Method != http.MethodPost || req.Body == nil || req.URL == nil {
		return false, nil, Key{}, nil
	}
	if req.URL.Path != "/v1/chat/completions" && req.URL.Path != "/v1/embeddings" {
		return false, nil, Key{}, nil
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(req.Header.Get("Content-Type"), ";")[0]))
	if contentType != "application/json" {
		return false, nil, Key{}, nil
	}
	limit := t.MaxRequestBytes
	if limit == 0 {
		limit = DefaultMaxRequest
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return false, nil, Key{}, err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	if int64(len(body)) > limit {
		return false, body, Key{}, nil
	}
	var probe struct {
		Stream bool `json:"stream"`
	}
	if json.Unmarshal(body, &probe) != nil || probe.Stream {
		return false, body, Key{}, nil
	}
	key, err := NewKey(req.Method, req.URL.RequestURI(), req.Header, body)
	if err != nil {
		return false, body, Key{}, nil
	}
	return true, body, key, nil
}
func (t *Transport) responseLimit() int64 {
	if t.MaxResponseBytes > 0 {
		return t.MaxResponseBytes
	}
	return DefaultMaxResponse
}
func (t *Transport) record(ctx context.Context, event string) error {
	if recorder, ok := t.Store.(eventRecorder); ok {
		return recorder.Record(ctx, event)
	}
	return nil
}
func replay(req *http.Request, entry Entry) *http.Response {
	h := entry.Header.Clone()
	return &http.Response{StatusCode: entry.StatusCode, Status: strconv.Itoa(entry.StatusCode) + " " + http.StatusText(entry.StatusCode), Header: h, Body: io.NopCloser(bytes.NewReader(append([]byte(nil), entry.Body...))), ContentLength: int64(len(entry.Body)), Request: req}
}
