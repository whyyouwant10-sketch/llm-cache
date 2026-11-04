# llm-cache

`llm-cache` is a local SQLite cache and HTTP proxy for deterministic,
non-streaming OpenAI-compatible JSON requests. A cache miss is forwarded to the
configured upstream; a later equivalent request is served from SQLite without
another upstream call.

The project is in early development and implements the v0.1 contract. Read the
full behavior and privacy contract in [`docs/SPEC.md`](docs/SPEC.md).

## What it does

- Caches successful JSON responses for `/v1/chat/completions` and
  `/v1/embeddings`.
- Builds deterministic, versioned SHA-256 keys from the semantic request data.
- Ignores `Authorization` and non-semantic request headers in cache keys.
- Records hits, misses, bypasses, upstream errors, tokens saved, and estimated
  cost saved.
- Uses SQLite WAL mode with a local, two-phase maintenance API.
- Defaults to a loopback listener and a 24-hour TTL.

## Requirements

- Go 1.19 or newer.
- A C compiler and CGO, required by the `github.com/mattn/go-sqlite3` driver.

## Build and SQLite setup

The command is built from this repository. No separate SQLite server is needed;
the database is a single SQLite file. The application creates the database's
parent directory with mode `0700` and the database file with mode `0600`.

```sh
mkdir -p ./bin
go build -o ./bin/llm-cache ./cmd/llm-cache

DB="${XDG_CACHE_HOME:-$HOME/.cache}/llm-cache/cache.sqlite"
./bin/llm-cache stats --db "$DB"
```

Opening the database initializes the schema. The default path used by the CLI
is `~/.cache/llm-cache/cache.sqlite` when the home directory is available.
`--db` can point to another local path.

## Start the proxy

Choose an upstream base URL and a local database. The proxy appends the
incoming request path and query string to the upstream URL.

```sh
DB="${XDG_CACHE_HOME:-$HOME/.cache}/llm-cache/cache.sqlite"

./bin/llm-cache proxy \
  --listen 127.0.0.1:8787 \
  --upstream https://api.example.test \
  --db "$DB" \
  --ttl 24h
```

The local endpoint mirrors the upstream path, for example:

```text
client -> http://127.0.0.1:8787/v1/chat/completions
           llm-cache -> https://api.example.test/v1/chat/completions
```

### Request example

The client sends its normal OpenAI-compatible request to the local listener.
Send the same request twice to observe one miss followed by one cache hit.

```sh
export OPENAI_API_KEY="replace-with-your-key"

curl http://127.0.0.1:8787/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${OPENAI_API_KEY}" \
  --data '{"model":"your-model","messages":[{"role":"user","content":"Say hello"}]}'

curl http://127.0.0.1:8787/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${OPENAI_API_KEY}" \
  --data '{"model":"your-model","messages":[{"role":"user","content":"Say hello"}]}'
```

The second response has the stored status, body bytes, and safe response
headers. The proxy does not add a cache marker to the response; use `stats` or
`inspect` to observe the result.

## Stats and estimated cost saved

Pricing is optional and is never fetched from a provider. Create a local JSON
mapping for the model names you use, with prices in micro-USD per token. Replace
the example model and rates with values appropriate for your provider:

```sh
cat > pricing.json <<'JSON'
{
  "your-model": {
    "input_micro_usd_per_token": 2,
    "output_micro_usd_per_token": 3
  }
}
JSON
```

Pass the same pricing file when running the proxy and when reading stats:

```sh
./bin/llm-cache proxy \
  --listen 127.0.0.1:8787 \
  --upstream https://api.example.test \
  --db "$DB" \
  --pricing pricing.json

./bin/llm-cache stats --db "$DB" --pricing pricing.json
```

For two identical requests with one hit, five prompt tokens, two completion
tokens, and the example rates, the human-readable output is:

```text
lookups: 2
hits: 1
misses: 1
bypasses: 0
upstream errors: 0
hit rate: 50.00%
tokens saved: 7
cost saved (micro-USD): 16
pricing known: true
```

The estimate is `(prompt_tokens * input rate) + (completion_tokens * output
rate)`. It represents avoided equivalent calls, not a billing statement. If a
model has no pricing entry, token savings can still be reported but estimated
cost is unknown.

Machine-readable stats are available with `--json`:

```sh
./bin/llm-cache stats --db "$DB" --pricing pricing.json --json
```

Use `--since` and `--until` with an RFC3339 timestamp or `YYYY-MM-DD` date to
limit the aggregate.

## Safe maintenance

`clear` and `prune` plan deletions first. Without `--apply`, they are read-only
dry runs; `--dry-run` is an explicit alias. A dry run prints the matching entry
count and bytes and does not delete rows, update statistics, change timestamps,
or write SQLite metadata/WAL state.

```sh
# Preview only (the default).
./bin/llm-cache clear --db "$DB" --model your-model
# clear: 3 entries, 12345 bytes

./bin/llm-cache prune --db "$DB" --expired --dry-run
# prune: 1 entries, 4096 bytes

# Delete only after reviewing the plan.
./bin/llm-cache prune --db "$DB" --expired --apply
./bin/llm-cache clear --db "$DB" --model your-model --apply
```

`--apply` cannot be combined with `--dry-run`. Use `clear --include-stats
--apply` only when aggregate statistics should also be removed. Other useful
filters are `clear --endpoint PATH`, `clear --key-prefix HEX`,
`prune --older-than DURATION`, and `prune --max-bytes BYTES`.

## API-key and privacy boundary

`llm-cache` does not read `OPENAI_API_KEY` (or any other provider key) and has
no provider-key flag. The client owns the environment variable and constructs
the `Authorization` header, as in the `curl` example. The proxy forwards that
header unchanged to the configured upstream.

The authorization value, cookies, request IDs, user agents, and other
non-semantic request headers are excluded from cache keys and are not stored in
SQLite or normal logs. By default, raw request bodies are not stored; the
database stores a canonical request digest, metadata, and the response needed
for replay. Cached response bodies may contain sensitive data, so treat the
SQLite file and its WAL files as sensitive local data. The proxy has no
authentication layer for non-loopback listeners; binding beyond loopback is an
operator decision.

## Inspect and clear

List cached metadata without printing request or response bodies:

```sh
./bin/llm-cache inspect --db "$DB"
./bin/llm-cache inspect --db "$DB" --json
```

## Early-development limitations

This is an early v0.1 implementation. It intentionally does not provide:

- Streaming/SSE, audio, image, multipart, or binary response caching.
- Request coalescing, distributed locking, remote stores, replication, or a
  web administration UI.
- Provider retries, rate-limit handling, billing reconciliation, or automatic
  pricing discovery.
- Encryption at rest, multi-tenant namespaces, or authenticated remote proxy
  access.

Streaming, malformed, oversized, unsupported, and non-2xx requests pass
through without being cached. The cache is an optimization and fails open to
normal upstream traffic when a response cannot be safely stored. See the
specification for resource limits and the complete v0.1 behavior.

## Development checks

Run the same checks used by CI:

```sh
test -z "$(gofmt -l $(find . -name '*.go' -type f))"
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Contribution instructions are in [`CONTRIBUTING.md`](CONTRIBUTING.md).
