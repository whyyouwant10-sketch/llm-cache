# Contributing to llm-cache

Thanks for helping improve `llm-cache`. The project is in early development, so
small, focused changes are easiest to review.

## Before you start

- Use Go 1.19 or newer.
- Use a C compiler with CGO enabled for the SQLite driver.
- Read [`docs/SPEC.md`](docs/SPEC.md) before changing cache behavior or privacy
  boundaries.

## Local workflow

From the repository root:

```sh
gofmt -w $(find . -name '*.go' -type f)
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

The GitHub Actions workflow runs the same formatting, test, race, vet, and
build checks. Tests should use deterministic mock transports or `httptest`
servers; do not require an external provider or real credentials.

## Change guidelines

- Keep cache keys deterministic and versioned when canonicalization changes.
- Never put API keys, bearer tokens, cookies, or arbitrary request headers in
  keys, logs, fixtures, or committed files.
- Preserve the dry-run boundary: planning must not mutate rows, counters,
  timestamps, or SQLite metadata; only `--apply` may delete.
- Keep response fixtures small and free of real credentials or personal data.
- Update the specification or README when a user-visible contract changes.
- Keep changes focused and include meaningful regression coverage for behavior
  changes.

## Pull requests

Describe the user-visible behavior, the files changed, and the checks you ran.
Before opening a pull request, confirm that the full local workflow passes and
that no SQLite files, binaries, credentials, or local pricing files are staged.
