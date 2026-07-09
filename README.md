# tiny-queue

tiny-queue is a small Go playground for background jobs. It keeps the moving parts in memory so you can run it locally, post a JSON job, and watch the worker update the status page without setting up Redis, Postgres, or a separate dashboard.

## Why this exists

This project exists to practice background job mechanics in a small, dependency-free Go codebase: enqueueing jobs, moving them through status transitions, retrying failures, dead-lettering exhausted work, and exposing a read-only status page.

## Quickstart

```bash
make test
make run
```

Open the status page in another tab:

http://localhost:8080/status

Then enqueue an echo job:

```bash
curl -X POST http://localhost:8080/jobs -H 'Content-Type: application/json' -d '{"type":"echo","payload":{"message":"hello"}}'
```

The sample server registers an `echo` handler, so this is enough to see a job move from queued to succeeded. If a handler returns an error, the queue waits before retrying; once attempts are exhausted, the job stays visible in the failed-jobs list.

## Known limitations

- Everything lives in process memory; restarting the server clears the queue.
- Job IDs are deterministic (`job-1`, `job-2`, ...) to keep local runs and tests easy to follow, not to act as production-safe identifiers.
- Retry handling assumes job handlers are safe to run more than once; the queue does not provide idempotency, deduplication, or durable audit records.
- The HTTP surface is meant for local exploration. It limits `POST /jobs` request bodies to 1 MiB, but does not include authentication, authorization, rate limiting, or production server shutdown behavior.
- `GET /status` is view-only. New work goes through `POST /jobs`.
