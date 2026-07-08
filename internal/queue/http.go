package queue

import (
	"encoding/json"
	"html/template"
	"net/http"
	"time"
)

var statusTemplate = template.Must(template.New("status").Funcs(template.FuncMap{
	"formatTime": formatStatusTime,
}).Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Tiny Queue status</title>
  <style>
    :root { color-scheme: light dark; font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    body { margin: 0; background: #f6f7f9; color: #1f2937; }
    header { background: #111827; color: white; padding: 2rem; }
    main { max-width: 1100px; margin: 0 auto; padding: 2rem; }
    section { margin-block: 2rem; }
    .counts { display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); }
    .card { background: white; border: 1px solid #e5e7eb; border-radius: .75rem; padding: 1rem; }
    .label { color: #6b7280; font-size: .875rem; text-transform: uppercase; letter-spacing: .08em; }
    .value { display: block; font-size: 2rem; font-weight: 700; margin-top: .25rem; }
    table { width: 100%; border-collapse: collapse; background: white; border: 1px solid #e5e7eb; border-radius: .75rem; overflow: hidden; }
    th, td { padding: .75rem; border-bottom: 1px solid #e5e7eb; text-align: left; vertical-align: top; }
    th { background: #f3f4f6; font-size: .8rem; text-transform: uppercase; letter-spacing: .06em; }
    tr:last-child td { border-bottom: 0; }
    .badge { border-radius: 999px; display: inline-block; font-size: .8rem; font-weight: 700; padding: .2rem .55rem; }
    .queued { background: #dbeafe; color: #1e40af; }
    .running { background: #fef3c7; color: #92400e; }
    .succeeded { background: #dcfce7; color: #166534; }
    .failed { background: #fee2e2; color: #991b1b; }
    .empty { background: white; border: 1px dashed #d1d5db; border-radius: .75rem; color: #6b7280; padding: 1rem; }
    code { white-space: pre-wrap; word-break: break-word; }
  </style>
</head>
<body>
<header>
  <h1>Tiny Queue</h1>
  <p>Read-only in-memory job status.</p>
</header>
<main>
  <section aria-labelledby="counts-heading">
    <h2 id="counts-heading">Counts</h2>
    <div class="counts">
      <article class="card"><span class="label">queued</span><span class="value">{{.Counts.Queued}}</span></article>
      <article class="card"><span class="label">running</span><span class="value">{{.Counts.Running}}</span></article>
      <article class="card"><span class="label">succeeded</span><span class="value">{{.Counts.Succeeded}}</span></article>
      <article class="card"><span class="label">failed</span><span class="value">{{.Counts.Failed}}</span></article>
    </div>
  </section>

  <section aria-labelledby="jobs-heading">
    <h2 id="jobs-heading">Recent jobs</h2>
    {{if .Jobs}}
    <table>
      <thead>
        <tr><th>ID</th><th>Type</th><th>Status</th><th>Attempts</th><th>Next Run</th><th>Last Error</th><th>Updated</th></tr>
      </thead>
      <tbody>
      {{range .Jobs}}
        <tr>
          <td><code>{{.ID}}</code></td>
          <td>{{.Type}}</td>
          <td><span class="badge {{.Status}}">{{.Status}}</span></td>
          <td>{{.Attempts}} / {{.MaxAttempts}}</td>
          <td>{{formatTime .NextRunAt}}</td>
          <td>{{if .LastError}}{{.LastError}}{{else}}—{{end}}</td>
          <td>{{formatTime .UpdatedAt}}</td>
        </tr>
      {{end}}
      </tbody>
    </table>
    {{else}}
    <p class="empty">No jobs have been enqueued.</p>
    {{end}}
  </section>

  <section aria-labelledby="dead-heading">
    <h2 id="dead-heading">Dead-letter queue</h2>
    {{if .DeadLetters}}
    <table>
      <thead>
        <tr><th>Job ID</th><th>Type</th><th>Attempts</th><th>Final Error</th><th>Failed At</th></tr>
      </thead>
      <tbody>
      {{range .DeadLetters}}
        <tr>
          <td><code>{{.JobID}}</code></td>
          <td>{{.Type}}</td>
          <td>{{.Attempts}}</td>
          <td>{{.FinalError}}</td>
          <td>{{formatTime .FailedAt}}</td>
        </tr>
      {{end}}
      </tbody>
    </table>
    {{else}}
    <p class="empty">No dead-lettered jobs.</p>
    {{end}}
  </section>
</main>
</body>
</html>`))

// Router returns the queue HTTP surface.
func Router(q *Queue) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		renderStatus(w, q)
	})
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		handlePostJob(w, r, q)
	})
	return mux
}

func renderStatus(w http.ResponseWriter, q *Queue) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := statusTemplate.Execute(w, q.Snapshot(0)); err != nil {
		return
	}
}

type enqueueRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type enqueueResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func handlePostJob(w http.ResponseWriter, r *http.Request, q *Queue) {
	var request enqueueRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	job, err := q.Enqueue(request.Type, []byte(request.Payload))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(enqueueResponse{ID: job.ID, Status: string(job.Status)})
}

func formatStatusTime(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.UTC().Format(time.RFC3339)
}
