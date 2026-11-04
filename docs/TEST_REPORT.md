# Test report

Commit under test: `1bdc83b`

The regression coverage added in `regression_test.go` and
`cmd/llm-cache/main_test.go` covers canonical JSON/key hashing, authorization
privacy, embeddings get/set and miss/hit behavior, TTL expiry, bounded response
and database storage, statistics and cost-saved accounting, proxy forwarding and
replay, malformed/oversized/unsupported requests, `inspect`, and dry-run versus
`--apply` `clear`/`prune` behavior.

## Checks

All commands were run from `/home/_a_h/projects_git/llm-cache`.

| Command | Result |
| --- | --- |
| `gofmt -w regression_test.go cmd/llm-cache/main.go cmd/llm-cache/main_test.go` | PASS |
| `go test ./...` | PASS — package and CLI tests passed |
| `go test -race ./...` | PASS — package and CLI tests passed under the race detector |
| `go vet ./...` | PASS |
| `go build ./...` | PASS |

The exact formatting command was run after the final test edits; the test,
race, vet, and build commands were run afterward against the formatted sources.

## E2E smoke

Command executed:

```sh
tmp=$(mktemp -d)
go build -o "$tmp/llm-cache" ./cmd/llm-cache
TMPDIR_FOR_E2E="$tmp" "$tmp/llm-cache" proxy --listen 127.0.0.1:18787 --upstream http://127.0.0.1:18786 --db "$tmp/cache.sqlite" --ttl 1h >"$tmp/proxy.log" 2>&1 &
proxy_pid=$!
E2E_PROXY_PID="$proxy_pid" E2E_TMP="$tmp" python3 - <<'PY'
import http.client
import http.server
import threading
import time

calls = 0
body = b'{"e2e": true, "value": 7}'
class Upstream(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        global calls
        calls += 1
        length = int(self.headers.get('Content-Length', '0'))
        received = self.rfile.read(length)
        assert self.path == '/v1/chat/completions?b=two&a=one'
        assert received == b'{"model":"e2e"}'
        assert self.headers.get('Authorization') == 'Bearer e2e-secret'
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('X-Upstream', 'private')
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *_):
        pass

server = http.server.HTTPServer(('127.0.0.1', 18786), Upstream)
threading.Thread(target=server.serve_forever, daemon=True).start()
for _ in range(100):
    try:
        conn = http.client.HTTPConnection('127.0.0.1', 18787, timeout=0.2)
        conn.request('GET', '/health')
        conn.getresponse().read()
        conn.close()
        break
    except OSError:
        time.sleep(0.05)
else:
    raise RuntimeError('proxy did not start')

def call():
    conn = http.client.HTTPConnection('127.0.0.1', 18787, timeout=2)
    conn.request('POST', '/v1/chat/completions?b=two&a=one', b'{"model":"e2e"}', {'Content-Type': 'application/json', 'Authorization': 'Bearer e2e-secret'})
    response = conn.getresponse()
    data = response.read()
    headers = dict(response.getheaders())
    conn.close()
    return response.status, data, headers
first = call()
second = call()
assert first[0] == second[0] == 200
assert first[1] == second[1] == body
assert calls == 1, calls
assert 'x-upstream' not in {key.lower() for key in second[2]}
print('E2E smoke passed: one upstream call, byte-identical hit, auth forwarded, unsafe header not replayed')
server.shutdown()
PY
kill "$proxy_pid"
wait "$proxy_pid" || true
cat "$tmp/proxy.log"
```

Result:

```text
E2E smoke passed: one upstream call, byte-identical hit, auth forwarded, unsafe header not replayed
2026/11/04 00:32:10 listening on 127.0.0.1:18787
```

The smoke used only loopback HTTP servers and made no external network calls.
