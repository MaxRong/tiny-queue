# tiny-queue

tiny-queue is a small Go playground for background jobs. It keeps the moving parts in memory so you can run it locally, post a JSON job, and watch the worker update the status page without setting up Redis, Postgres, or a separate dashboard.

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

## Notes

- Everything lives in process memory; restarting the server clears the queue.
- Job IDs are deterministic (`job-1`, `job-2`, ...) to keep local runs and tests easy to follow.
- `GET /status` is view-only. New work goes through `POST /jobs`.
