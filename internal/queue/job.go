package queue

import (
	"errors"
	"time"
)

// Status describes the current lifecycle state of a job.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Job is the retained in-memory record for a queued unit of work.
type Job struct {
	ID           string
	Type         string
	Payload      []byte
	Status       Status
	Attempts     int
	MaxAttempts  int
	NextRunAt    time.Time
	LastError    string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	DeadLettered bool
}

// DeadLetter records jobs that are no longer eligible for worker polling.
type DeadLetter struct {
	JobID      string
	Type       string
	Payload    []byte
	Attempts   int
	FinalError string
	CreatedAt  time.Time
	FailedAt   time.Time
}

// Counts summarizes retained jobs by status.
type Counts struct {
	Queued    int
	Running   int
	Succeeded int
	Failed    int
}

// Snapshot is a defensive, point-in-time view of queue state.
type Snapshot struct {
	Counts      Counts
	Jobs        []Job
	DeadLetters []DeadLetter
}

// Config controls retry behavior and clock injection.
type Config struct {
	MaxAttempts int
	RetryDelay  time.Duration
	Now         func() time.Time
}

var (
	ErrInvalidJobType = errors.New("job type must not be empty")
	ErrNilHandler     = errors.New("handler must not be nil")
	ErrNoReadyJob     = errors.New("no queued job is ready")
)
