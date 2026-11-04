# llm-cache v0.1 specification

## 1. Purpose

`llm-cache` is a local Go library and HTTP proxy for caching deterministic,
non-streaming requests to OpenAI-compatible APIs. It stores successful JSON
responses in SQLite, serves equivalent requests without contacting the
upstream, and reports cache effectiveness and estimated provider cost saved.

The cache is an optimization, not a source of truth. A caller must be able to
disable it or bypass it without changing the upstream API contract.

### Design goals

- Be wire-compatible with common OpenAI-compatible clients through a local
  HTTP endpoint.
- Make cache keys safe and reproducible across Go processes and machines.
- Keep authentication material out of keys, logs, and the SQLite database.
- Make cache administration observable and safe to preview.
- Keep v0.1 small enough to implement and test with a mock transport.

### Non-goals for v0.1

- A distributed cache or cross-process cache invalidation service.
- Semantic similarity, prompt rewriting, response ranking, or model fallback.
- Provider account management, billing, rate-limit handling, or retries.
- Caching streaming responses, audio, image, multipart, or binary payloads.
- Encryption-at-rest supplied by the library.

## 2. Product boundary

### 2.1 Go library boundary

The reusable package owns cache policy and persistence. A minimal public API is:

```go
type Store interface {
    Lookup(ctx context.Context, key Key) (Entry, bool, error)
    Put(ctx context.Context, key Key, response Response, ttl time.Duration) error
    PlanClear(ctx context.Context, filter Filter) (Plan, error)
    ApplyClear(ctx context.Context, plan Plan) error
    PlanPrune(ctx context.Context, policy PrunePolicy) (Plan, error)
    ApplyPrune(ctx context.Context, plan Plan) error
    Stats(ctx context.Context, filter StatsFilter) (Stats, error)
}

type Transport struct { // implements http.RoundTripper
    Next  http.RoundTripper
    Store Store
    TTL   time.Duration
}
```

Names may change during implementation, but the separation is required:

- canonicalization and key creation are pure functions;
- SQLite access is behind `Store`;
- the transport decides hit/miss/bypass and forwards misses to `Next`;
- the CLI calls the same store planning and application functions as library
  consumers.

The transport must preserve the upstream status code and response body on a
hit, and must not mutate a caller's request or response objects. It must
clone request/response headers when crossing the cache boundary.

### 2.2 Proxy boundary

The CLI proxy exposes an ordinary HTTP server. A typical setup is:

```text
client -> http://127.0.0.1:8787/v1/chat/completions
                     llm-cache -> https://api.example.test/v1/chat/completions
```

The v0.1 command surface is:

```text
llm-cache proxy --listen 127.0.0.1:8787 --upstream https://api.example.test \
  --db ~/.cache/llm-cache/cache.sqlite --ttl 24h
llm-cache stats [--json] [--since TIME] [--until TIME]
llm-cache clear [FILTERS] [--dry-run|--apply]
llm-cache prune [POLICY] [--dry-run|--apply]
```

Proxy behavior:

1. Accept `POST` with `Content-Type: application/json` for the configured
   upstream base URL.
2. Support `/v1/chat/completions` and `/v1/embeddings` request/response
   shapes. Other paths are forwarded but are never cached unless explicitly
   enabled by a future endpoint registry.
3. Forward the request method, path, query string, body, and all relevant
   caller headers. The proxy sets no provider-specific authentication header.
4. Read `Authorization` from the incoming request and forward it unchanged;
   never persist it or include it in the cache key.
5. Cache only successful, complete, non-streaming JSON responses. On a miss,
   the response is returned exactly as received and is stored only after the
   complete body has been read.
6. For `stream: true`, unsupported content types, oversized bodies, malformed
   JSON, or an explicitly bypassed request, forward without lookup/write and
   record a bypass.
7. Never cache upstream 4xx/5xx responses in v0.1. Return those responses to
   the caller and record an upstream error.

The proxy does not terminate TLS for the client, inspect or refresh tokens,
choose models, or retry failed upstream requests. The local listener should
default to loopback; binding a non-loopback address requires an explicit
`--listen` value and is the operator's responsibility.

### 2.3 CLI safety boundary

`clear` and `prune` are two-phase operations:

- Without `--apply`, the command is a read-only dry run and prints the number
  of matching entries, bytes, and (where applicable) the planned policy.
- `--dry-run` is an explicit alias for the default behavior.
- `--apply` is required for deletion and cannot be combined with
  `--dry-run`. It executes the previously computed predicate in a transaction.
- Cache statistics are preserved by `clear` and `prune` unless the operator
  supplies `clear --include-stats --apply`.

The library has the same boundary: `PlanClear`/`PlanPrune` do not write, and
only `ApplyClear`/`ApplyPrune` may delete rows. A dry run must not update hit
counters, timestamps, SQLite metadata, or the WAL.

Useful filters and policies are:

- `--model MODEL`, `--endpoint PATH`, and `--key-prefix HEX` for `clear`;
- `--expired` (the default `prune` policy);
- `--older-than DURATION`;
- `--max-bytes BYTES`, evicting least-recently-used entries first;
- `--events-older-than DURATION` to remove old diagnostic events, if event
  retention is enabled.

## 3. Request lifecycle and cache policy

For each request, the transport performs the following sequence:

1. Validate that the request is eligible (method, content type, JSON, size,
   endpoint, and `stream` value).
2. Build the versioned canonical key without retaining the raw authorization
   header.
3. Look up the key and consider it a hit only when `expires_at > now`.
4. On a hit, return the stored status, safe response headers, and body; update
   `hit_count`, `last_hit_at`, and the daily statistics in one short write
   transaction.
5. On a miss, call the configured upstream transport once.
6. Read the complete upstream response. Return it to the caller even if it is
   too large or otherwise not cacheable.
7. Store an eligible 2xx JSON response with `expires_at = stored_at + TTL`.
8. Record a miss, bypass, or error in the statistics appropriate to the
   outcome. A miss is a cache lookup miss that attempted upstream; a bypass is
   a request intentionally excluded before lookup.

The default TTL is 24 hours. `0` disables writes while retaining lookup and
statistics behavior. Configured TTLs must be between 1 second and 30 days when
enabled. An implementation may offer a longer-TTL override, but it is outside
the v0.1 compatibility contract.

Cache entries are immutable response snapshots. A new response for the same
key replaces the old entry and resets its expiry and hit count. Concurrent
misses for the same key may both reach upstream in v0.1; request coalescing is
reserved for a later version.

## 4. Canonicalization and hashing

The cache key is a SHA-256 digest rendered as lowercase hexadecimal. Its input
is a versioned UTF-8 record with length-delimited fields:

```text
llm-cache-key-v1\0
METHOD\0
normalized-path-and-query\0
canonical-semantic-headers\0
canonical-json-body
```

The exact framing must prevent concatenation ambiguity; implementations should
use a length prefix rather than rely only on the illustrative separators.

### 4.1 Included request data

- HTTP method, currently `POST`.
- URL path with one leading slash and no dot-segment ambiguity.
- Query parameters with percent-decoding followed by RFC 3986 normalization,
  sorting by parameter name and then value, and preservation of repeated
  values. Fragments are never sent to the server and are excluded.
- The canonical JSON body.
- A small allowlist of semantic headers, currently normalized media type for
  `Content-Type` and `Accept` when present. Header names are lowercase and
  optional whitespace is trimmed.

`Authorization`, cookies, user-agent, tracing/request IDs, dates, connection
headers, host, and all other non-semantic headers are excluded. They must not
cause separate entries or be stored as key material.

### 4.2 Canonical JSON rules

The v0.1 canonicalizer follows RFC 8785-style JSON canonicalization:

- Parse exactly one JSON value and reject malformed input, duplicate object
  member names, `NaN`, and infinities.
- Object member names are sorted by their UTF-16 code-unit ordering.
- Array order is preserved, including repeated values.
- Strings use the canonical JSON escaping rules; no Unicode normalization is
  performed.
- Numbers use one canonical representation, with no insignificant leading
  zeroes or exponent variation. Values outside the implementation's exact
  supported range are rejected rather than rounded.
- Whitespace is removed.
- Unknown request fields are retained. The canonicalizer must not drop fields
  merely because an older OpenAI schema does not know them.

The implementation must not silently treat an omitted field as equal to a
field set to `null`, `false`, `0`, or an empty array. Provider defaults can
change behavior, so those are distinct requests unless a future schema-aware
version explicitly documents a semantic default. `stream: true` is excluded
from caching rather than normalized into a cacheable request.

The key version is part of the digest input and is stored with each entry. Any
change to canonicalization, included headers, or endpoint semantics increments
the key version and causes old entries to miss safely; it must never reuse an
old key under new rules.

### 4.3 Response eligibility and metadata

The response body is stored byte-for-byte. JSON is validated before storage,
but it is not re-serialized, so a hit preserves provider formatting and field
order. Store only a safe response-header allowlist (`Content-Type`, selected
cache-independent metadata, and an optional request ID generated by the proxy
if one is explicitly configured). Do not replay upstream `Set-Cookie`,
`Authorization`, hop-by-hop, or transport headers.

When present, extract `usage.prompt_tokens`, `usage.completion_tokens`, and
`usage.total_tokens` from the JSON response. Missing or malformed usage is
allowed and produces zero token savings rather than making the response
uncacheable.

## 5. SQLite storage

The database is a single SQLite file opened with `WAL`, foreign keys enabled,
and a bounded busy timeout. The implementation should use a parameterized
driver and transactions for all writes. Times are stored as UTC Unix seconds;
monetary values are integer micro-USD to avoid floating-point accumulation.

`PRAGMA user_version` is the schema version. v0.1 is version `1`.

### 5.1 Schema

```sql
CREATE TABLE IF NOT EXISTS cache_entries (
    cache_key              TEXT PRIMARY KEY,       -- 64 lowercase hex chars
    key_version            INTEGER NOT NULL,
    method                 TEXT NOT NULL,
    path                   TEXT NOT NULL,
    model                  TEXT,
    request_sha256         TEXT NOT NULL,
    request_body           BLOB,                  -- NULL unless opt-in storage
    status_code            INTEGER NOT NULL,
    response_headers_json  TEXT NOT NULL,
    response_body          BLOB NOT NULL,
    response_bytes         INTEGER NOT NULL,
    stored_at              INTEGER NOT NULL,
    expires_at             INTEGER NOT NULL,
    last_hit_at            INTEGER,
    hit_count              INTEGER NOT NULL DEFAULT 0,
    prompt_tokens          INTEGER,
    completion_tokens      INTEGER,
    total_tokens           INTEGER,
    estimated_cost_microusd INTEGER NOT NULL DEFAULT 0,
    CHECK (length(cache_key) = 64),
    CHECK (response_bytes = length(response_body)),
    CHECK (expires_at >= stored_at),
    CHECK (hit_count >= 0)
);

CREATE INDEX IF NOT EXISTS cache_entries_expiry
    ON cache_entries (expires_at);
CREATE INDEX IF NOT EXISTS cache_entries_lru
    ON cache_entries (last_hit_at, stored_at);
CREATE INDEX IF NOT EXISTS cache_entries_model
    ON cache_entries (model);

CREATE TABLE IF NOT EXISTS stats_daily (
    day_utc                TEXT PRIMARY KEY,       -- YYYY-MM-DD
    lookups                INTEGER NOT NULL DEFAULT 0,
    hits                   INTEGER NOT NULL DEFAULT 0,
    misses                 INTEGER NOT NULL DEFAULT 0,
    bypasses               INTEGER NOT NULL DEFAULT 0,
    upstream_errors        INTEGER NOT NULL DEFAULT 0,
    tokens_saved           INTEGER NOT NULL DEFAULT 0,
    cost_saved_microusd    INTEGER NOT NULL DEFAULT 0,
    CHECK (lookups >= 0 AND hits >= 0 AND misses >= 0),
    CHECK (bypasses >= 0 AND upstream_errors >= 0),
    CHECK (tokens_saved >= 0 AND cost_saved_microusd >= 0)
);

CREATE TABLE IF NOT EXISTS meta (
    name                   TEXT PRIMARY KEY,
    value                  TEXT NOT NULL
);
```

`request_body` is nullable and must be NULL by default. It is useful for local
debugging only and is enabled by an explicit privacy-sensitive configuration
option. `request_sha256` is the digest of the canonical body, not a copy of
the authorization-bearing HTTP request.

The `model` column is a convenience field extracted from the request for
filtering and reporting. It is not authoritative for key equality; the full
canonical body remains the source of identity. `estimated_cost_microusd` is
the estimated provider cost represented by this response's usage, in micro-USD.

### 5.2 Statistics

`stats_daily` is updated in the same transaction as the hit counter or cache
write. `lookups` is always `hits + misses`; an upstream error after a cache
miss increments both `misses` and `upstream_errors`, while a bypass increments
only `bypasses`. The CLI reports:

```text
hit_rate = hits / (hits + misses)
```

Bypasses and upstream errors are shown separately and excluded from the hit
rate denominator. A hit contributes the cached response's known token count to
`tokens_saved`. It contributes `cost_saved_microusd` only when a matching model
pricing configuration is available. Unknown pricing yields zero estimated
cost saved and must be labeled as unknown rather than presented as zero actual
provider spend.

The v0.1 pricing input is a local configuration mapping model name to input and
output micro-USD per token. The estimate is:

```text
(prompt_tokens * input_micro_usd_per_token) +
(completion_tokens * output_micro_usd_per_token)
```

Pricing is never fetched from a provider and is never inferred from an API
key. Statistics are estimates of avoided equivalent calls, not a billing
statement.

## 6. Privacy and security

- The database directory is created with mode `0700`; the SQLite file and WAL
  files are created with mode `0600` where supported.
- API keys, bearer tokens, cookies, and arbitrary incoming headers are never
  persisted, logged, included in the key, or returned by `stats`.
- Request bodies can contain prompts, personal data, and tool arguments. The
  default database stores only `request_sha256`, endpoint/model metadata, and
  the response needed for replay. Raw request storage requires an explicit
  opt-in and must be called out in `config`/startup output.
- Cached response bodies can also contain sensitive data. Operators must treat
  the database as sensitive local data and may disable caching for a request or
  use `clear --apply` when retention is no longer appropriate.
- No request or response body is written to normal logs. Error logs include
  endpoint and status, never body or credentials.
- The proxy must not follow redirects for an upstream request automatically;
  redirects can change the authority receiving credentials. Redirect responses
  are not cacheable.
- v0.1 does not claim encryption-at-rest. A future encrypted store must keep
  the same key and replay semantics without silently changing privacy defaults.

## 7. Limits and resource behavior

The following are v0.1 defaults and hard upper bounds unless an explicit future
version changes them:

| Resource | Default | Hard limit / behavior |
| --- | ---: | --- |
| Request JSON body | 1 MiB | Reject caching and forward upstream when exceeded |
| Cached response body | 10 MiB | Return upstream response but do not store when exceeded |
| SQLite database | 1 GiB | Refuse new writes and continue pass-through |
| Entry TTL | 24 hours | Enabled range is 1 second–30 days; `0` disables writes |
| Concurrent SQLite writers | Driver-controlled | WAL, busy timeout, short write transactions |
| Stats retention | Indefinite aggregates | Optional diagnostic-event retention is bounded by policy |
| Header value retained | 8 KiB each | Drop non-allowlisted values; never truncate key material |

On limit or SQLite failure, the proxy fails open for normal upstream traffic:
it returns the upstream result when available and reports a cache warning to
metrics/logging without exposing database details to the API caller. A cache
lookup that cannot safely complete is treated as a miss only if it is safe to
contact upstream; it must never serve a partially read or expired entry.

## 8. Mock transport and fixtures

Tests should use a deterministic `http.RoundTripper` or `httptest.Server`, not
the network. The mock records call count, received path/query, selected
headers, and canonical request body, and returns fixture responses with a
controllable clock. Fixtures should be small JSON files or Go values with no
real credentials.

Required fixture matrix:

| Fixture | Setup | Expected result |
| --- | --- | --- |
| `chat-completion-200` | Same non-streaming chat request twice | One upstream call; second response is a byte-identical hit |
| `embeddings-200` | Same embeddings request twice | One upstream call; hit and usage metadata recorded |
| `object-key-order` | Reorder JSON object members only | Same key and second call is a hit |
| `array-order` | Reorder messages or embedding input array | Different keys; both reach upstream |
| `auth-is-not-key` | Same body with two bearer tokens | One shared key; neither token is persisted or logged |
| `header-noise` | Change user-agent/request ID/date | Same key; second call is a hit |
| `semantic-header` | Change accepted media type | Different key or bypass according to allowlist |
| `ttl-expired` | Advance fake clock beyond expiry | Expired entry is a miss and is replaced after 200 response |
| `stream-bypass` | Send `{"stream":true}` | Upstream is called; no lookup/write; bypass increments |
| `error-not-cached` | Return 429 and 500 | Each call reaches upstream; no entry is created |
| `oversize` | Exceed request/response limit | Request passes through or response is returned; no entry is created |
| `cost-known` | Include usage and configured model pricing | Hit increments tokens and estimated micro-USD saved |
| `cost-unknown` | Include usage without pricing | Tokens saved increments; cost is reported unknown/zero estimate |
| `dry-run-clear` | Create entries, run `clear` without `--apply` | Plan is printed; rows, stats, timestamps, and WAL are unchanged |
| `dry-run-prune` | Create expired and live entries | Only expired count is planned; no deletion occurs |
| `apply-prune` | Apply expired/LRU policy | Correct rows are deleted atomically; stats remain |
| `malformed-json` | Send invalid JSON | Upstream behavior is preserved; no cache key or entry is written |

Fixture assertions must compare the body served on a hit, not only status
codes. They must also verify that `Authorization` is absent from SQLite text
and that a dry run does not mutate `cache_entries` or `stats_daily`.

## 9. Acceptance criteria

An implementation satisfies v0.1 when all of the following are true:

1. A supported non-streaming OpenAI-compatible JSON request misses once,
   forwards unchanged, and later hits from SQLite without an upstream call.
2. The hit response preserves the stored status, body bytes, and safe headers.
3. Object key ordering and insignificant JSON formatting do not affect a key;
   array order, meaningful values, path/query, and allowlisted semantic
   headers do affect it.
4. Authorization and non-semantic request headers do not affect a key and are
   absent from the database and ordinary logs.
5. Expired entries are never served and TTL `0` never writes entries.
6. Streaming, malformed, oversized, unsupported, and non-2xx cases bypass or
   pass through according to this document and are not cached.
7. `stats` reports hits, misses, bypasses, errors, hit rate, tokens saved, and
   estimated cost saved with an explicit unknown-pricing state.
8. `clear` and `prune` are read-only by default; only `--apply` deletes, and
   dry-run tests show no row, counter, timestamp, or WAL mutation.
9. Apply operations are transactional, deterministic, and safe to rerun.
10. The mock transport fixture matrix passes with no external network access.
11. Schema initialization is idempotent, uses parameterized SQL, and leaves
    the database recoverable after a process restart.

## 10. v0.1 delivery scope

### Included

- Go package for canonical keys, SQLite store, caching `RoundTripper`, stats,
  and plan/apply maintenance operations.
- CLI proxy for the two supported endpoint shapes and pass-through behavior.
- SQLite schema version 1 with migrations initialized through `user_version`.
- Configurable TTL, database path, upstream URL, size limits, and local model
  pricing.
- JSON and human-readable stats output.
- Deterministic mock transport, fake clock, and the fixture matrix above.
- Documentation of privacy-sensitive raw request storage as an explicit opt-in.

### Explicitly deferred

- Streaming chunk capture/replay and SSE cache entries.
- Request coalescing, distributed locking, remote stores, and replication.
- Automatic semantic normalization of provider defaults.
- Encrypted SQLite, multi-tenant namespaces, and authenticated remote proxy
  access.
- Automatic pricing discovery, billing reconciliation, and provider retries.
- Cache invalidation webhooks and a browser/UI administration surface.
