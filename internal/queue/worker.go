package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Handler processes a single running job.
type Handler func(context.Context, Job) error

// Register associates a job type with a handler.
func (q *Queue) Register(jobType string, handler Handler) error {
	jobType = strings.TrimSpace(jobType)
	if jobType == "" {
		return ErrInvalidJobType
	}
	if handler == nil {
		return ErrNilHandler
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[jobType] = handler
	return nil
}

// ProcessNext runs one ready queued job, if any.
func (q *Queue) ProcessNext(ctx context.Context) (Job, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}

	q.mu.Lock()
	index, ok := q.nextReadyJobLocked(q.now())
	if !ok {
		q.mu.Unlock()
		return Job{}, ErrNoReadyJob
	}

	record := &q.jobs[index]
	handler, registered := q.handlers[record.job.Type]
	if !registered {
		err := errors.New(fmt.Sprintf("no handler registered for job type %q", record.job.Type))
		now := q.now()
		record.job.Status = StatusFailed
		record.job.DeadLettered = true
		record.job.LastError = err.Error()
		record.job.NextRunAt = time.Time{}
		record.job.UpdatedAt = now
		q.appendDeadLetterLocked(record.job, err.Error(), now)
		processed := copyJob(record.job)
		q.mu.Unlock()
		return processed, err
	}

	now := q.now()
	record.job.Status = StatusRunning
	record.job.Attempts++
	record.job.UpdatedAt = now
	runningJob := copyJob(record.job)
	q.mu.Unlock()

	err := handler(ctx, runningJob)

	q.mu.Lock()
	defer q.mu.Unlock()
	record = &q.jobs[index]
	now = q.now()
	if err == nil {
		record.job.Status = StatusSucceeded
		record.job.LastError = ""
		record.job.NextRunAt = time.Time{}
		record.job.UpdatedAt = now
		return copyJob(record.job), nil
	}

	record.job.LastError = err.Error()
	record.job.UpdatedAt = now
	if record.job.Attempts < record.job.MaxAttempts {
		record.job.Status = StatusQueued
		record.job.NextRunAt = now.Add(q.retryDelay)
		return copyJob(record.job), err
	}

	record.job.Status = StatusFailed
	record.job.DeadLettered = true
	record.job.NextRunAt = time.Time{}
	q.appendDeadLetterLocked(record.job, err.Error(), now)
	return copyJob(record.job), err
}

// Run repeatedly processes ready work until the context is canceled.
func (q *Queue) Run(ctx context.Context, idleDelay time.Duration) {
	if idleDelay <= 0 {
		idleDelay = 100 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, err := q.ProcessNext(ctx)
		if !errors.Is(err, ErrNoReadyJob) {
			continue
		}

		timer := time.NewTimer(idleDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (q *Queue) nextReadyJobLocked(now time.Time) (int, bool) {
	for i := range q.jobs {
		job := q.jobs[i].job
		if job.Status != StatusQueued || job.DeadLettered {
			continue
		}
		if !job.NextRunAt.IsZero() && job.NextRunAt.After(now) {
			continue
		}
		return i, true
	}
	return 0, false
}

func (q *Queue) appendDeadLetterLocked(job Job, finalError string, failedAt time.Time) {
	q.deadLetterSequence++
	q.deadLetters = append(q.deadLetters, deadLetterRecord{
		deadLetter: DeadLetter{
			JobID:      job.ID,
			Type:       job.Type,
			Payload:    copyBytes(job.Payload),
			Attempts:   job.Attempts,
			FinalError: finalError,
			CreatedAt:  job.CreatedAt,
			FailedAt:   failedAt,
		},
		sequence: q.deadLetterSequence,
	})
}
