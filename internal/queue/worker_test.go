package queue

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestProcessNextRunsOldestQueuedJob(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC))
	q := New(Config{Now: clock.Now, MaxAttempts: 3, RetryDelay: 5 * time.Second})

	var received []Job
	if err := q.Register("email", func(ctx context.Context, job Job) error {
		received = append(received, job)
		return nil
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}

	firstPayload := []byte(`{"to":"ada@example.test"}`)
	first, err := q.Enqueue("email", firstPayload)
	if err != nil {
		t.Fatalf("enqueue first job: %v", err)
	}
	second, err := q.Enqueue("email", []byte(`{"to":"grace@example.test"}`))
	if err != nil {
		t.Fatalf("enqueue second job: %v", err)
	}

	processed, err := q.ProcessNext(context.Background())
	if err != nil {
		t.Fatalf("ProcessNext returned error: %v", err)
	}
	if processed.ID != first.ID {
		t.Fatalf("processed job ID = %q, want oldest queued job %q", processed.ID, first.ID)
	}
	if processed.Status != StatusSucceeded {
		t.Fatalf("processed Status = %q, want %q", processed.Status, StatusSucceeded)
	}

	if len(received) != 1 {
		t.Fatalf("handler calls = %d, want 1", len(received))
	}
	handled := received[0]
	if handled.ID != first.ID {
		t.Fatalf("handler received job ID = %q, want first job %q", handled.ID, first.ID)
	}
	if handled.Status != StatusRunning {
		t.Fatalf("handler received Status = %q, want %q", handled.Status, StatusRunning)
	}
	if handled.Attempts != 1 {
		t.Fatalf("handler received Attempts = %d, want 1", handled.Attempts)
	}
	if !bytes.Equal(handled.Payload, firstPayload) {
		t.Fatalf("handler received Payload = %q, want %q", handled.Payload, firstPayload)
	}

	snapshot := q.Snapshot(0)
	storedFirst := requireSnapshotJob(t, snapshot, first.ID)
	if storedFirst.Status != StatusSucceeded {
		t.Fatalf("stored first job Status = %q, want %q", storedFirst.Status, StatusSucceeded)
	}
	if storedFirst.LastError != "" {
		t.Fatalf("stored first job LastError = %q, want empty", storedFirst.LastError)
	}
	if !storedFirst.NextRunAt.IsZero() {
		t.Fatalf("stored first job NextRunAt = %s, want zero time", storedFirst.NextRunAt)
	}

	storedSecond := requireSnapshotJob(t, snapshot, second.ID)
	if storedSecond.Status != StatusQueued {
		t.Fatalf("stored second job Status = %q, want %q", storedSecond.Status, StatusQueued)
	}
	if storedSecond.Attempts != 0 {
		t.Fatalf("stored second job Attempts = %d, want 0", storedSecond.Attempts)
	}
}

func TestProcessNextReturnsNoReadyJob(t *testing.T) {
	t.Run("empty queue", func(t *testing.T) {
		q := New(Config{})
		before := q.Snapshot(0)

		processed, err := q.ProcessNext(context.Background())
		if !errors.Is(err, ErrNoReadyJob) {
			t.Fatalf("ProcessNext error = %v, want ErrNoReadyJob", err)
		}
		if !reflect.DeepEqual(processed, Job{}) {
			t.Fatalf("ProcessNext job = %+v, want zero Job", processed)
		}

		after := q.Snapshot(0)
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("Snapshot changed after empty ProcessNext: before=%+v after=%+v", before, after)
		}
	})

	t.Run("retry waiting queue", func(t *testing.T) {
		base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
		clock := newFakeClock(base)
		q := New(Config{Now: clock.Now, MaxAttempts: 3, RetryDelay: 5 * time.Second})
		errTemporary := errors.New("smtp temporary")
		if err := q.Register("email", func(ctx context.Context, job Job) error {
			return errTemporary
		}); err != nil {
			t.Fatalf("Register returned error: %v", err)
		}
		if _, err := q.Enqueue("email", []byte(`{"to":"ada@example.test"}`)); err != nil {
			t.Fatalf("Enqueue returned error: %v", err)
		}

		firstFailureTime := base.Add(time.Second)
		clock.Set(firstFailureTime)
		if _, err := q.ProcessNext(context.Background()); !errors.Is(err, errTemporary) {
			t.Fatalf("initial ProcessNext error = %v, want %v", err, errTemporary)
		}

		clock.Set(firstFailureTime.Add(5*time.Second - time.Nanosecond))
		before := q.Snapshot(0)
		processed, err := q.ProcessNext(context.Background())
		if !errors.Is(err, ErrNoReadyJob) {
			t.Fatalf("ProcessNext before retry delay error = %v, want ErrNoReadyJob", err)
		}
		if !reflect.DeepEqual(processed, Job{}) {
			t.Fatalf("ProcessNext before retry delay job = %+v, want zero Job", processed)
		}
		after := q.Snapshot(0)
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("Snapshot changed while retry was waiting: before=%+v after=%+v", before, after)
		}
	})
}

func TestSucceededJobsAreNeverRetried(t *testing.T) {
	q := New(Config{})

	calls := 0
	if err := q.Register("email", func(ctx context.Context, job Job) error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	queued, err := q.Enqueue("email", []byte(`{"to":"ada@example.test"}`))
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	processed, err := q.ProcessNext(context.Background())
	if err != nil {
		t.Fatalf("first ProcessNext returned error: %v", err)
	}
	if processed.ID != queued.ID {
		t.Fatalf("first ProcessNext job ID = %q, want %q", processed.ID, queued.ID)
	}
	if processed.Status != StatusSucceeded {
		t.Fatalf("first ProcessNext Status = %q, want %q", processed.Status, StatusSucceeded)
	}

	for i := range 2 {
		later, err := q.ProcessNext(context.Background())
		if !errors.Is(err, ErrNoReadyJob) {
			t.Fatalf("later ProcessNext %d error = %v, want ErrNoReadyJob", i+1, err)
		}
		if !reflect.DeepEqual(later, Job{}) {
			t.Fatalf("later ProcessNext %d job = %+v, want zero Job", i+1, later)
		}
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
}

func TestMissingHandlerDeadLettersImmediately(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC))
	q := New(Config{Now: clock.Now, MaxAttempts: 3, RetryDelay: 5 * time.Second})

	payload := []byte(`{"task":"orphaned"}`)
	queued, err := q.Enqueue("unknown", payload)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	processed, err := q.ProcessNext(context.Background())
	if err == nil {
		t.Fatalf("ProcessNext returned nil error, want missing handler error")
	}
	wantErr := `no handler registered for job type "unknown"`
	if err.Error() != wantErr {
		t.Fatalf("ProcessNext error = %q, want %q", err.Error(), wantErr)
	}
	if processed.ID != queued.ID {
		t.Fatalf("processed job ID = %q, want %q", processed.ID, queued.ID)
	}
	if processed.Status != StatusFailed {
		t.Fatalf("processed Status = %q, want %q", processed.Status, StatusFailed)
	}
	if !processed.DeadLettered {
		t.Fatalf("processed DeadLettered = false, want true")
	}
	if processed.Attempts != 0 {
		t.Fatalf("processed Attempts = %d, want 0", processed.Attempts)
	}
	if processed.LastError != err.Error() {
		t.Fatalf("processed LastError = %q, want %q", processed.LastError, err.Error())
	}

	snapshot := q.Snapshot(0)
	if len(snapshot.DeadLetters) != 1 {
		t.Fatalf("DeadLetters = %d, want 1", len(snapshot.DeadLetters))
	}
	deadLetter := snapshot.DeadLetters[0]
	if deadLetter.JobID != queued.ID {
		t.Fatalf("DeadLetter JobID = %q, want %q", deadLetter.JobID, queued.ID)
	}
	if deadLetter.Type != "unknown" {
		t.Fatalf("DeadLetter Type = %q, want unknown", deadLetter.Type)
	}
	if !bytes.Equal(deadLetter.Payload, payload) {
		t.Fatalf("DeadLetter Payload = %q, want %q", deadLetter.Payload, payload)
	}
	if deadLetter.Attempts != 0 {
		t.Fatalf("DeadLetter Attempts = %d, want 0", deadLetter.Attempts)
	}
	if deadLetter.FinalError != err.Error() {
		t.Fatalf("DeadLetter FinalError = %q, want %q", deadLetter.FinalError, err.Error())
	}
	if !deadLetter.CreatedAt.Equal(queued.CreatedAt) {
		t.Fatalf("DeadLetter CreatedAt = %s, want %s", deadLetter.CreatedAt, queued.CreatedAt)
	}
	if !deadLetter.FailedAt.Equal(clock.Now()) {
		t.Fatalf("DeadLetter FailedAt = %s, want %s", deadLetter.FailedAt, clock.Now())
	}

	later, err := q.ProcessNext(context.Background())
	if !errors.Is(err, ErrNoReadyJob) {
		t.Fatalf("later ProcessNext error = %v, want ErrNoReadyJob", err)
	}
	if !reflect.DeepEqual(later, Job{}) {
		t.Fatalf("later ProcessNext job = %+v, want zero Job", later)
	}
}

func TestRetryAfterDelayThenSucceeds(t *testing.T) {
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(base)
	q := New(Config{Now: clock.Now, MaxAttempts: 3, RetryDelay: 5 * time.Second})
	errTemporary := errors.New("smtp temporary")

	calls := 0
	if err := q.Register("email", func(ctx context.Context, job Job) error {
		calls++
		if calls == 1 {
			return errTemporary
		}
		return nil
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	queued, err := q.Enqueue("email", []byte(`{"to":"ada@example.test"}`))
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	firstFailureTime := base.Add(time.Second)
	clock.Set(firstFailureTime)
	retryJob, err := q.ProcessNext(context.Background())
	if !errors.Is(err, errTemporary) {
		t.Fatalf("first ProcessNext error = %v, want %v", err, errTemporary)
	}
	if retryJob.ID != queued.ID {
		t.Fatalf("retry job ID = %q, want %q", retryJob.ID, queued.ID)
	}
	if retryJob.Status != StatusQueued {
		t.Fatalf("retry job Status = %q, want %q", retryJob.Status, StatusQueued)
	}
	if retryJob.Attempts != 1 {
		t.Fatalf("retry job Attempts = %d, want 1", retryJob.Attempts)
	}
	if retryJob.LastError != errTemporary.Error() {
		t.Fatalf("retry job LastError = %q, want %q", retryJob.LastError, errTemporary.Error())
	}
	wantNextRunAt := firstFailureTime.Add(5 * time.Second)
	if !retryJob.NextRunAt.Equal(wantNextRunAt) {
		t.Fatalf("retry job NextRunAt = %s, want %s", retryJob.NextRunAt, wantNextRunAt)
	}

	clock.Set(wantNextRunAt.Add(-time.Nanosecond))
	tooEarly, err := q.ProcessNext(context.Background())
	if !errors.Is(err, ErrNoReadyJob) {
		t.Fatalf("ProcessNext before NextRunAt error = %v, want ErrNoReadyJob", err)
	}
	if !reflect.DeepEqual(tooEarly, Job{}) {
		t.Fatalf("ProcessNext before NextRunAt job = %+v, want zero Job", tooEarly)
	}

	clock.Set(wantNextRunAt)
	succeeded, err := q.ProcessNext(context.Background())
	if err != nil {
		t.Fatalf("second ProcessNext returned error: %v", err)
	}
	if succeeded.ID != queued.ID {
		t.Fatalf("succeeded job ID = %q, want %q", succeeded.ID, queued.ID)
	}
	if succeeded.Attempts != 2 {
		t.Fatalf("succeeded job Attempts = %d, want 2", succeeded.Attempts)
	}
	if succeeded.Status != StatusSucceeded {
		t.Fatalf("succeeded job Status = %q, want %q", succeeded.Status, StatusSucceeded)
	}
	if succeeded.LastError != "" {
		t.Fatalf("succeeded job LastError = %q, want empty", succeeded.LastError)
	}
	if !succeeded.NextRunAt.IsZero() {
		t.Fatalf("succeeded job NextRunAt = %s, want zero time", succeeded.NextRunAt)
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
	if deadLetters := q.Snapshot(0).DeadLetters; len(deadLetters) != 0 {
		t.Fatalf("DeadLetters = %d, want 0", len(deadLetters))
	}
}

func TestDeadLetterExhaustedJobLeavesPolling(t *testing.T) {
	base := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(base)
	q := New(Config{Now: clock.Now, MaxAttempts: 2, RetryDelay: 5 * time.Second})
	errBoom := errors.New("boom")

	if err := q.Register("email", func(ctx context.Context, job Job) error {
		return errBoom
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	payload := []byte(`{"to":"ada@example.test"}`)
	queued, err := q.Enqueue("email", payload)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	firstFailureTime := base.Add(time.Second)
	clock.Set(firstFailureTime)
	retryJob, err := q.ProcessNext(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("first ProcessNext error = %v, want %v", err, errBoom)
	}
	if retryJob.ID != queued.ID {
		t.Fatalf("retry job ID = %q, want %q", retryJob.ID, queued.ID)
	}
	if retryJob.Status != StatusQueued {
		t.Fatalf("retry job Status = %q, want %q", retryJob.Status, StatusQueued)
	}
	if retryJob.Attempts != 1 {
		t.Fatalf("retry job Attempts = %d, want 1", retryJob.Attempts)
	}

	secondFailureTime := firstFailureTime.Add(5 * time.Second)
	clock.Set(secondFailureTime)
	failedJob, err := q.ProcessNext(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("second ProcessNext error = %v, want %v", err, errBoom)
	}
	if failedJob.ID != queued.ID {
		t.Fatalf("failed job ID = %q, want %q", failedJob.ID, queued.ID)
	}
	if failedJob.Status != StatusFailed {
		t.Fatalf("failed job Status = %q, want %q", failedJob.Status, StatusFailed)
	}
	if !failedJob.DeadLettered {
		t.Fatalf("failed job DeadLettered = false, want true")
	}
	if failedJob.Attempts != 2 {
		t.Fatalf("failed job Attempts = %d, want 2", failedJob.Attempts)
	}
	if failedJob.LastError != errBoom.Error() {
		t.Fatalf("failed job LastError = %q, want %q", failedJob.LastError, errBoom.Error())
	}
	if !failedJob.NextRunAt.IsZero() {
		t.Fatalf("failed job NextRunAt = %s, want zero time", failedJob.NextRunAt)
	}

	snapshot := q.Snapshot(0)
	if len(snapshot.DeadLetters) != 1 {
		t.Fatalf("DeadLetters = %d, want 1", len(snapshot.DeadLetters))
	}
	deadLetter := snapshot.DeadLetters[0]
	if deadLetter.JobID != queued.ID {
		t.Fatalf("DeadLetter JobID = %q, want %q", deadLetter.JobID, queued.ID)
	}
	if deadLetter.Type != "email" {
		t.Fatalf("DeadLetter Type = %q, want email", deadLetter.Type)
	}
	if !bytes.Equal(deadLetter.Payload, payload) {
		t.Fatalf("DeadLetter Payload = %q, want %q", deadLetter.Payload, payload)
	}
	if deadLetter.Attempts != 2 {
		t.Fatalf("DeadLetter Attempts = %d, want 2", deadLetter.Attempts)
	}
	if deadLetter.FinalError != errBoom.Error() {
		t.Fatalf("DeadLetter FinalError = %q, want %q", deadLetter.FinalError, errBoom.Error())
	}
	if !deadLetter.CreatedAt.Equal(queued.CreatedAt) {
		t.Fatalf("DeadLetter CreatedAt = %s, want %s", deadLetter.CreatedAt, queued.CreatedAt)
	}
	if !deadLetter.FailedAt.Equal(secondFailureTime) {
		t.Fatalf("DeadLetter FailedAt = %s, want %s", deadLetter.FailedAt, secondFailureTime)
	}

	later, err := q.ProcessNext(context.Background())
	if !errors.Is(err, ErrNoReadyJob) {
		t.Fatalf("later ProcessNext error = %v, want ErrNoReadyJob", err)
	}
	if !reflect.DeepEqual(later, Job{}) {
		t.Fatalf("later ProcessNext job = %+v, want zero Job", later)
	}
}

func requireSnapshotJob(t *testing.T, snapshot Snapshot, id string) Job {
	t.Helper()
	for _, job := range snapshot.Jobs {
		if job.ID == id {
			return job
		}
	}
	t.Fatalf("Snapshot missing job %q; jobs=%v", id, jobIDs(snapshot.Jobs))
	return Job{}
}
