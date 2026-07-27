# meter

A local proxy that caps and attributes your LLM API spend. Point your
Anthropic or OpenAI base URL at it and get per-project cost tracking, a
hard daily limit that actually stops requests, and a ranked list of your
most expensive calls, without touching a single call site.

```
meter watch - 14:32:07 (refreshing every tick, Ctrl-C to quit)

PROJECT         TODAY    CAP     $/HR     TIME TO CAP
cognitiveradar  $1.8400  $2.00   $6.2000  1m32s
infra-mind      $0.3100  $2.00   $0.4000  4h13m0s
pr-pilot        $0.0000  $2.00   $0.0000  -
GLOBAL          $2.1500  $5.00   $6.6000  25m54s
```

## The problem

Running LLM calls from several places (a terminal agent, a couple of side
projects, ad-hoc scripts) means they all bill to the same provider account.
The provider dashboard shows one number for the day; it cannot tell you
that one project burned 70% of it. Provider limits are monthly and
enforced after the fact by email. When a day costs 6x what it should,
there is no way to find out which single request did it.

meter sits between your SDK and the real API as a transparent reverse
proxy, so none of that changes except where the request goes.

## Install

```sh
go install github.com/vaish725/tokenmeter/cmd/meter@latest
```

Or build the Docker image locally:

```sh
docker build -t meter .
docker run -d \
  -p 8080:8080 -p 8081:8081 \
  -e METER_LISTEN_ADDR=0.0.0.0:8080 \
  -e METER_OPENAI_LISTEN_ADDR=0.0.0.0:8081 \
  meter
```

(A Homebrew tap and tagged releases are configured in `.goreleaser.yml`
but not yet published.)

## Quick start

```sh
meter                      # starts both listeners using configs/pricing.json and configs/caps.json
export ANTHROPIC_BASE_URL=http://127.0.0.1:8080
export OPENAI_BASE_URL=http://127.0.0.1:8081
```

Every SDK call now goes through meter unmodified. Tag a project explicitly
with a header if you want; otherwise requests are logged as `unattributed`:

```sh
curl $ANTHROPIC_BASE_URL/v1/messages \
  -H "X-Meter-Project: cognitiveradar" \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -d '{"model":"claude-sonnet-5","max_tokens":1024,"messages":[...]}'
```

## Commands

| Command | What it does |
|---|---|
| `meter` | Runs the proxy: an Anthropic-facing listener and an OpenAI-facing listener, on separate ports. |
| `meter top` | The costliest requests in a time window (`--since`, `--limit`). |
| `meter watch` | A live-refreshing dashboard: spend, burn rate, time to cap, per project. |
| `meter version` | Prints the build version. |

## How it works

- **Reserve then reconcile.** Checking "spend so far < cap" is racy under
  concurrency; N requests in flight can all pass the check before any of
  them finish. meter estimates a pessimistic cost before forwarding a
  request and reserves it against the budget atomically, then reconciles
  the estimate against the real token usage once the response is known.
- **Streaming without buffering.** Responses are relayed byte for byte as
  they arrive; usage is pulled out of the tail of the stream by a small
  incremental scanner, not by buffering the whole response first.
- **Hard caps by default.** A project or the whole account over its daily
  cap gets a structured `429` explaining which cap, what the limit is, and
  when it resets. An opt-in downshift policy (`configs/downshift.json`) can
  retry a cheaper model instead of blocking; without that file, behavior is
  unchanged.
- **Two ports, one process.** `ANTHROPIC_BASE_URL` and `OPENAI_BASE_URL`
  could point at the same port, but both APIs expose overlapping paths
  (`/v1/models`), so a single listener cannot tell an unmatched request's
  provider apart. Separate ports sidestep that.

## Configuration

All three files are plain JSON, live in `configs/`, and can be edited
without a rebuild:

- `pricing.json` - USD per million tokens, per model, plus an optional
  `default_price` fallback used for any model not explicitly listed. That
  fallback matters: without it, a model missing from this file costs $0 and
  never counts against your cap - a new model release or a typo in a model
  name would otherwise bypass the daily limit entirely. Reconciliation
  against real `usage` bounds the error even if a price goes stale.
- `caps.json` - a global daily cap and a default per-project cap, with
  optional per-project overrides. Resets at local midnight.
- `downshift.json` - optional, absent by default. Maps a model to a
  cheaper substitute to try before returning a `429`.

Paths and ports are all overridable by environment variable; see
`internal/config/config.go` for the full list and defaults.

## Performance

A local benchmark comparing a direct call to the same call through meter
(non-streaming, loopback, in-process fake upstream):

```
BenchmarkDirectRequest    30.1 µs/op
BenchmarkProxyRequest    280.3 µs/op
```

About 250µs of added overhead per request, well inside the PRD's 20ms p95
budget. Reproduce with `go test ./internal/proxy -bench . -benchmem`.

## Non-goals

Multi-tenant or multi-user support, prompt/response caching, being a
router or load balancer across providers, a hosted service, and storing
prompt bodies by default. This is a single binary for a single developer
on one machine, not a team gateway.

## Development

```sh
go build ./...
go vet ./...
go test ./... -race
go test ./internal/proxy -fuzz=FuzzFeedSSE -fuzztime=30s
```

CI runs gofmt, vet, build, and the full test suite under `-race` on every
push and pull request.

## License

MIT, see [LICENSE](LICENSE).
