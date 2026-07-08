package queue

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultSnapshotLimit = 25

type jobRecord struct {
	job      Job
	sequence uint64
}

type deadLetterRecord struct {
	deadLetter DeadLetter
	sequence   uint64
}

// Queue stores jobs and dead letters in memory.
type Queue struct {
	mu                 sync.Mutex
	maxAttempts        int
	retryDelay         time.Duration
	now                func() time.Time
	sequence           uint64
	deadLetterSequence uint64
	jobs               []jobRecord
	deadLetters        []deadLetterRecord
	handlers           map[string]Handler
}

// New constructs an empty in-memory queue with conservative defaults.
func New(config Config) *Queue {
	maxAttempts := config.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	retryDelay := config.RetryDelay
	if retryDelay <= 0 {
		retryDelay = time.Second
	}

	now := config.Now
	if now == nil {
		now = time.Now
	}

	return &Queue{
		maxAttempts: maxAttempts,
		retryDelay:  retryDelay,
		now:         now,
		handlers:    make(map[string]Handler),
	}
}

// Enqueue appends a job to the FIFO queue.
func (q *Queue) Enqueue(jobType string, payload []byte) (Job, error) {
	jobType = strings.TrimSpace(jobType)
	if jobType == "" {
		return Job{}, ErrInvalidJobType
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	q.sequence++
	now := q.now()
	job := Job{
		ID:          fmt.Sprintf("job-%d", q.sequence),
		Type:        jobType,
		Payload:     copyBytes(payload),
		Status:      StatusQueued,
		Attempts:    0,
		MaxAttempts: q.maxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	q.jobs = append(q.jobs, jobRecord{job: job, sequence: q.sequence})

	return copyJob(job), nil
}

// Snapshot returns defensive copies of recent jobs and all dead letters.
func (q *Queue) Snapshot(limit int) Snapshot {
	if limit <= 0 {
		limit = defaultSnapshotLimit
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	snapshot := Snapshot{}
	records := make([]jobRecord, len(q.jobs))
	copy(records, q.jobs)

	for _, record := range records {
		switch record.job.Status {
		case StatusQueued:
			snapshot.Counts.Queued++
		case StatusRunning:
			snapshot.Counts.Running++
		case StatusSucceeded:
			snapshot.Counts.Succeeded++
		case StatusFailed:
			snapshot.Counts.Failed++
		}
	}

	sort.SliceStable(records, func(i, j int) bool {
		left := records[i]
		right := records[j]
		if !left.job.UpdatedAt.Equal(right.job.UpdatedAt) {
			return left.job.UpdatedAt.After(right.job.UpdatedAt)
		}
		return left.sequence > right.sequence
	})

	if len(records) > limit {
		records = records[:limit]
	}
	snapshot.Jobs = make([]Job, len(records))
	for i, record := range records {
		snapshot.Jobs[i] = copyJob(record.job)
	}

	deadLetters := make([]deadLetterRecord, len(q.deadLetters))
	copy(deadLetters, q.deadLetters)
	sort.SliceStable(deadLetters, func(i, j int) bool {
		left := deadLetters[i]
		right := deadLetters[j]
		if !left.deadLetter.FailedAt.Equal(right.deadLetter.FailedAt) {
			return left.deadLetter.FailedAt.After(right.deadLetter.FailedAt)
		}
		return left.sequence > right.sequence
	})
	snapshot.DeadLetters = make([]DeadLetter, len(deadLetters))
	for i, record := range deadLetters {
		snapshot.DeadLetters[i] = copyDeadLetter(record.deadLetter)
	}

	return snapshot
}

func copyJob(job Job) Job {
	job.Payload = copyBytes(job.Payload)
	return job
}

func copyDeadLetter(deadLetter DeadLetter) DeadLetter {
	deadLetter.Payload = copyBytes(deadLetter.Payload)
	return deadLetter
}

func copyBytes(payload []byte) []byte {
	if payload == nil {
		return nil
	}
	copied := make([]byte, len(payload))
	copy(copied, payload)
	return copied
}
