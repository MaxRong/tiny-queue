package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStatusPageRendersReadOnlyDashboard(t *testing.T) {
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(base)
	q := New(Config{Now: clock.Now, MaxAttempts: 2, RetryDelay: 5 * time.Second})

	if err := q.Register("welcome", func(ctx context.Context, job Job) error {
		return nil
	}); err != nil {
		t.Fatalf("Register welcome returned error: %v", err)
	}
	if err := q.Register("retry-report", func(ctx context.Context, job Job) error {
		return testHTTPError("smtp temporary")
	}); err != nil {
		t.Fatalf("Register retry-report returned error: %v", err)
	}
	if err := q.Register("audit", func(ctx context.Context, job Job) error {
		return testHTTPError("audit sink unavailable")
	}); err != nil {
		t.Fatalf("Register audit returned error: %v", err)
	}

	if _, err := q.Enqueue("welcome", []byte(`{"to":"ada@example.test"}`)); err != nil {
		t.Fatalf("enqueue welcome job: %v", err)
	}
	if _, err := q.ProcessNext(context.Background()); err != nil {
		t.Fatalf("process welcome job: %v", err)
	}

	clock.Advance(time.Second)
	if _, err := q.Enqueue("audit", []byte(`{"event":"login"}`)); err != nil {
		t.Fatalf("enqueue audit job: %v", err)
	}
	if _, err := q.ProcessNext(context.Background()); err == nil || err.Error() != "audit sink unavailable" {
		t.Fatalf("first audit ProcessNext error = %v, want audit sink unavailable", err)
	}
	clock.Advance(5 * time.Second)
	failedJob, err := q.ProcessNext(context.Background())
	if err == nil || err.Error() != "audit sink unavailable" {
		t.Fatalf("second audit ProcessNext error = %v, want audit sink unavailable", err)
	}
	if failedJob.Status != StatusFailed || !failedJob.DeadLettered {
		t.Fatalf("failed audit job = %+v, want failed dead-lettered job", failedJob)
	}

	clock.Advance(time.Second)
	if _, err := q.Enqueue("retry-report", []byte(`{"report":"daily"}`)); err != nil {
		t.Fatalf("enqueue retry-report job: %v", err)
	}
	retryJob, err := q.ProcessNext(context.Background())
	if err == nil || err.Error() != "smtp temporary" {
		t.Fatalf("retry-report ProcessNext error = %v, want smtp temporary", err)
	}
	if retryJob.Status != StatusQueued || retryJob.NextRunAt.IsZero() {
		t.Fatalf("retry-report job = %+v, want queued retry with NextRunAt", retryJob)
	}

	queuedJob, err := q.Enqueue("email", []byte(`{"to":"grace@example.test"}`))
	if err != nil {
		t.Fatalf("enqueue queued email job: %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/status", nil)
	Router(q).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /status status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.Contains(contentType, "text/html; charset=utf-8") {
		t.Fatalf("GET /status Content-Type = %q, want text/html; charset=utf-8", contentType)
	}

	body := recorder.Body.String()
	assertBodyContains(t, body, "Tiny Queue")
	for _, want := range []string{"queued", "running", "succeeded", "failed"} {
		assertBodyContains(t, strings.ToLower(body), want)
	}
	for _, want := range []string{queuedJob.ID, queuedJob.Type, retryJob.ID, retryJob.Type, failedJob.ID, failedJob.Type, "smtp temporary", "audit sink unavailable"} {
		assertBodyContains(t, body, want)
	}
	assertBodyContainsAny(t, body, retryJob.NextRunAt,
		retryJob.NextRunAt.Format(time.RFC3339),
		retryJob.NextRunAt.Format("2006-01-02 15:04:05"),
		retryJob.NextRunAt.String(),
	)
	assertReadOnlyStatusPage(t, body)
}

func TestStatusPageShowsRunningJobWithoutSleeps(t *testing.T) {
	q := New(Config{})
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	if err := q.Register("slow-email", func(ctx context.Context, job Job) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	queued, err := q.Enqueue("slow-email", []byte(`{"to":"ada@example.test"}`))
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	go func() {
		_, err := q.ProcessNext(context.Background())
		done <- err
	}()

	select {
	case <-started:
	case err := <-done:
		t.Fatalf("ProcessNext finished before handler blocked: %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/status", nil)
	Router(q).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /status status = %d, want %d; body: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	body := recorder.Body.String()
	assertBodyContains(t, strings.ToLower(body), "running")
	assertBodyContains(t, body, queued.ID)
	assertBodyContains(t, body, queued.Type)
	assertReadOnlyStatusPage(t, body)

	close(release)
	released = true
	if err := <-done; err != nil {
		t.Fatalf("ProcessNext after release returned error: %v", err)
	}
}

func TestStatusPageRejectsMutationMethods(t *testing.T) {
	q := New(Config{})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/status", nil)

	Router(q).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /status status = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
}

func TestPostJobsEnqueuesRawPayload(t *testing.T) {
	q := New(Config{})
	payload := []byte(`{"to":"ada@example.test","priority":1}`)
	requestBody := `{"type":"email","payload":` + string(payload) + `}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(requestBody))
	request.Header.Set("Content-Type", "application/json")

	Router(q).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("POST /jobs status = %d, want %d; body: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.Contains(contentType, "application/json") {
		t.Fatalf("POST /jobs Content-Type = %q, want application/json", contentType)
	}

	var response map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode POST /jobs response: %v; body: %s", err, recorder.Body.String())
	}
	if len(response) != 2 {
		t.Fatalf("POST /jobs response fields = %v, want exactly id and status", response)
	}
	if response["id"] != "job-1" {
		t.Fatalf("POST /jobs response id = %q, want job-1", response["id"])
	}
	if response["status"] != string(StatusQueued) {
		t.Fatalf("POST /jobs response status = %q, want %q", response["status"], StatusQueued)
	}

	snapshot := q.Snapshot(0)
	if len(snapshot.Jobs) != 1 {
		t.Fatalf("Snapshot jobs = %d, want 1", len(snapshot.Jobs))
	}
	if !bytes.Equal(snapshot.Jobs[0].Payload, payload) {
		t.Fatalf("stored payload = %q, want raw payload bytes %q", snapshot.Jobs[0].Payload, payload)
	}
}

func TestPostJobsValidation(t *testing.T) {
	t.Run("malformed JSON", func(t *testing.T) {
		q := New(Config{})
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":`))
		request.Header.Set("Content-Type", "application/json")

		Router(q).ServeHTTP(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("POST /jobs malformed JSON status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})

	t.Run("oversized body", func(t *testing.T) {
		q := New(Config{})
		recorder := httptest.NewRecorder()
		requestBody := `{"type":"email","payload":"` + strings.Repeat("x", postJobRequestBodyLimitBytes) + `"}`
		request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(requestBody))
		request.Header.Set("Content-Type", "application/json")

		Router(q).ServeHTTP(recorder, request)

		if recorder.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("POST /jobs oversized body status = %d, want %d; body: %s", recorder.Code, http.StatusRequestEntityTooLarge, recorder.Body.String())
		}
		if jobs := q.Snapshot(0).Jobs; len(jobs) != 0 {
			t.Fatalf("Snapshot jobs after oversized body = %d, want 0", len(jobs))
		}
	})

	t.Run("oversized trailing body", func(t *testing.T) {
		q := New(Config{})
		recorder := httptest.NewRecorder()
		requestBody := `{"type":"email","payload":{}}` + strings.Repeat(" ", postJobRequestBodyLimitBytes)
		request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(requestBody))
		request.Header.Set("Content-Type", "application/json")

		Router(q).ServeHTTP(recorder, request)

		if recorder.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("POST /jobs oversized trailing body status = %d, want %d; body: %s", recorder.Code, http.StatusRequestEntityTooLarge, recorder.Body.String())
		}
		if jobs := q.Snapshot(0).Jobs; len(jobs) != 0 {
			t.Fatalf("Snapshot jobs after oversized trailing body = %d, want 0", len(jobs))
		}
	})

	for _, tt := range []struct {
		name    string
		jobType string
	}{
		{name: "empty type", jobType: ""},
		{name: "whitespace type", jobType: "   "},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q := New(Config{})
			recorder := httptest.NewRecorder()
			requestBody := `{"type":` + strconvQuote(tt.jobType) + `,"payload":{"ignored":true}}`
			request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(requestBody))
			request.Header.Set("Content-Type", "application/json")

			Router(q).ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("POST /jobs invalid type status = %d, want %d; body: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			assertBodyContains(t, recorder.Body.String(), ErrInvalidJobType.Error())
			if jobs := q.Snapshot(0).Jobs; len(jobs) != 0 {
				t.Fatalf("Snapshot jobs after invalid type = %d, want 0", len(jobs))
			}
		})
	}

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		t.Run(method+" /jobs", func(t *testing.T) {
			q := New(Config{})
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(method, "/jobs", nil)

			Router(q).ServeHTTP(recorder, request)

			if recorder.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s /jobs status = %d, want %d", method, recorder.Code, http.StatusMethodNotAllowed)
			}
		})
	}
}

type testHTTPError string

func (e testHTTPError) Error() string {
	return string(e)
}

func assertBodyContains(t *testing.T, body string, want string) {
	t.Helper()
	if !strings.Contains(body, want) {
		t.Fatalf("body missing %q; body: %s", want, body)
	}
}

func assertBodyContainsAny(t *testing.T, body string, value time.Time, candidates ...string) {
	t.Helper()
	for _, candidate := range candidates {
		if strings.Contains(body, candidate) {
			return
		}
	}
	t.Fatalf("body missing rendered time %s; checked formats %q; body: %s", value, candidates, body)
}

func assertReadOnlyStatusPage(t *testing.T, body string) {
	t.Helper()
	lowerBody := strings.ToLower(body)
	for _, forbidden := range []string{"<form", "<button", "method=", "href=\"/retry", "href=\"/requeue", "data-action=\"retry\"", "data-action=\"requeue\""} {
		if strings.Contains(lowerBody, forbidden) {
			t.Fatalf("status page contains forbidden operator control %q; body: %s", forbidden, body)
		}
	}
}

func strconvQuote(s string) string {
	encoded, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
