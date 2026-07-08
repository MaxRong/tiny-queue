package queue

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

type fakeClock struct {
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{now: t}
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Set(t time.Time) {
	c.now = t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func TestEnqueueRejectsEmptyType(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC))
	q := New(Config{Now: clock.Now})

	for _, tt := range []struct {
		name    string
		jobType string
	}{
		{name: "empty", jobType: ""},
		{name: "whitespace", jobType: "   "},
	} {
		t.Run(tt.name, func(t *testing.T) {
			job, err := q.Enqueue(tt.jobType, []byte(`{"ignored":true}`))
			if !errors.Is(err, ErrInvalidJobType) {
				t.Fatalf("Enqueue(%q) error = %v, want ErrInvalidJobType", tt.jobType, err)
			}
			if !reflect.DeepEqual(job, Job{}) {
				t.Fatalf("Enqueue(%q) job = %+v, want zero Job", tt.jobType, job)
			}

			snapshot := q.Snapshot(0)
			if len(snapshot.Jobs) != 0 {
				t.Fatalf("Snapshot jobs after rejected enqueue = %d, want 0", len(snapshot.Jobs))
			}
			if len(snapshot.DeadLetters) != 0 {
				t.Fatalf("Snapshot dead letters after rejected enqueue = %d, want 0", len(snapshot.DeadLetters))
			}
		})
	}
}

func TestEnqueueStoresQueuedJob(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC))
	q := New(Config{Now: clock.Now, MaxAttempts: 3, RetryDelay: 5 * time.Second})

	payload := []byte(`{"to":"ada@example.test"}`)
	job, err := q.Enqueue("email", payload)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	if job.ID != "job-1" {
		t.Fatalf("job ID = %q, want job-1", job.ID)
	}
	if job.Type != "email" {
		t.Fatalf("job Type = %q, want email", job.Type)
	}
	if job.Status != StatusQueued {
		t.Fatalf("job Status = %q, want %q", job.Status, StatusQueued)
	}
	if job.Attempts != 0 {
		t.Fatalf("job Attempts = %d, want 0", job.Attempts)
	}
	if job.MaxAttempts != 3 {
		t.Fatalf("job MaxAttempts = %d, want 3", job.MaxAttempts)
	}
	if !job.NextRunAt.IsZero() {
		t.Fatalf("job NextRunAt = %s, want zero time", job.NextRunAt)
	}
	if job.LastError != "" {
		t.Fatalf("job LastError = %q, want empty", job.LastError)
	}
	if job.DeadLettered {
		t.Fatalf("job DeadLettered = true, want false")
	}
	if !job.CreatedAt.Equal(clock.Now()) {
		t.Fatalf("job CreatedAt = %s, want %s", job.CreatedAt, clock.Now())
	}
	if !job.UpdatedAt.Equal(clock.Now()) {
		t.Fatalf("job UpdatedAt = %s, want %s", job.UpdatedAt, clock.Now())
	}
	if !job.CreatedAt.Equal(job.UpdatedAt) {
		t.Fatalf("job CreatedAt = %s, UpdatedAt = %s, want equal", job.CreatedAt, job.UpdatedAt)
	}
	if !bytes.Equal(job.Payload, []byte(`{"to":"ada@example.test"}`)) {
		t.Fatalf("job Payload = %q, want unchanged raw payload", job.Payload)
	}

	payload[0] = '['
	snapshot := q.Snapshot(0)
	if len(snapshot.Jobs) != 1 {
		t.Fatalf("Snapshot jobs = %d, want 1", len(snapshot.Jobs))
	}
	if !bytes.Equal(snapshot.Jobs[0].Payload, []byte(`{"to":"ada@example.test"}`)) {
		t.Fatalf("stored payload after source mutation = %q, want original payload bytes", snapshot.Jobs[0].Payload)
	}
}

func TestSnapshotCopiesAndOrdersJobs(t *testing.T) {
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(base)
	q := New(Config{Now: clock.Now, MaxAttempts: 3, RetryDelay: 5 * time.Second})

	first, err := q.Enqueue("email", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("enqueue first job: %v", err)
	}
	clock.Advance(time.Second)
	second, err := q.Enqueue("sms", []byte(`{"id":2}`))
	if err != nil {
		t.Fatalf("enqueue second job: %v", err)
	}
	third, err := q.Enqueue("push", []byte(`{"id":3}`))
	if err != nil {
		t.Fatalf("enqueue third job: %v", err)
	}

	snapshot := q.Snapshot(0)
	if len(snapshot.Jobs) != 3 {
		t.Fatalf("Snapshot jobs = %d, want 3", len(snapshot.Jobs))
	}
	assertJobOrder(t, snapshot.Jobs, []string{third.ID, second.ID, first.ID})

	snapshot.Jobs[0].Payload[0] = '['
	later := q.Snapshot(0)
	if len(later.Jobs) != 3 {
		t.Fatalf("later Snapshot jobs = %d, want 3", len(later.Jobs))
	}
	assertJobOrder(t, later.Jobs, []string{third.ID, second.ID, first.ID})
	if !bytes.Equal(later.Jobs[0].Payload, []byte(`{"id":3}`)) {
		t.Fatalf("stored payload after snapshot mutation = %q, want original payload bytes", later.Jobs[0].Payload)
	}
}

func assertJobOrder(t *testing.T, jobs []Job, wantIDs []string) {
	t.Helper()
	if len(jobs) != len(wantIDs) {
		t.Fatalf("job count = %d, want %d", len(jobs), len(wantIDs))
	}
	for i, wantID := range wantIDs {
		if jobs[i].ID != wantID {
			t.Fatalf("jobs[%d].ID = %q, want %q; full order: %v", i, jobs[i].ID, wantID, jobIDs(jobs))
		}
	}
}

func jobIDs(jobs []Job) []string {
	ids := make([]string, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
	}
	return ids
}
