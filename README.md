# llm-cache

Local SQLite cache and HTTP proxy for deterministic, non-streaming OpenAI-compatible JSON requests.

```sh
go test ./...
go build ./...

llm-cache proxy --listen 127.0.0.1:8787 \
  --upstream https://api.example.test \
  --db ~/.cache/llm-cache/cache.sqlite --ttl 24h
llm-cache stats --db ~/.cache/llm-cache/cache.sqlite --json
llm-cache inspect --db ~/.cache/llm-cache/cache.sqlite
llm-cache clear --db ~/.cache/llm-cache/cache.sqlite       # dry run
llm-cache clear --db ~/.cache/llm-cache/cache.sqlite --apply
llm-cache prune --db ~/.cache/llm-cache/cache.sqlite --expired --apply
```

The cache excludes authorization and non-semantic request headers from keys and persistence. `clear` and `prune` are read-only unless `--apply` is supplied. See [`docs/SPEC.md`](docs/SPEC.md) for the v0.1 contract.
