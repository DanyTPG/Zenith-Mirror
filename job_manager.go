package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type JobType string

const (
	JobTypeMirror JobType = "MIRROR"
	JobTypeLeech  JobType = "LEECH"
)

type Job struct {
	ID          string
	Type        JobType
	FileName    string
	Size        int64
	ReadBytes   int64
	Speed       float64
	ETA         time.Duration
	Status      string
	UserID      int64
	Ctx         context.Context
	Cancel      context.CancelFunc
	StartTime   time.Time
}

type JobManager struct {
	mu             sync.RWMutex
	jobs           map[string]*Job
	maxConcurrency int
	semaphore      chan struct{}
	seq            uint64
}

func NewJobManager(maxConcurrency int) *JobManager {
	if maxConcurrency <= 0 {
		maxConcurrency = 3
	}
	return &JobManager{
		jobs:           make(map[string]*Job),
		maxConcurrency: maxConcurrency,
		semaphore:      make(chan struct{}, maxConcurrency),
	}
}

func (jm *JobManager) CreateJob(parentCtx context.Context, jobType JobType, name string, size int64, userID int64) (*Job, error) {
	select {
	case jm.semaphore <- struct{}{}:
	default:
		return nil, errors.New("max concurrent jobs reached, please try again later")
	}

	idNum := atomic.AddUint64(&jm.seq, 1)
	jobID := fmt.Sprintf("job-%d", idNum)

	ctx, cancel := context.WithCancel(parentCtx)

	job := &Job{
		ID:        jobID,
		Type:      jobType,
		FileName:  name,
		Size:      size,
		Status:    "Starting",
		UserID:    userID,
		Ctx:       ctx,
		Cancel:    cancel,
		StartTime: time.Now(),
	}

	jm.mu.Lock()
	jm.jobs[jobID] = job
	jm.mu.Unlock()

	return job, nil
}

func (jm *JobManager) FinishJob(jobID string) {
	jm.mu.Lock()
	if job, ok := jm.jobs[jobID]; ok {
		job.Cancel()
		delete(jm.jobs, jobID)
		<-jm.semaphore
	}
	jm.mu.Unlock()
}

func (jm *JobManager) CancelJob(jobID string) bool {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	job, ok := jm.jobs[jobID]
	if !ok {
		return false
	}
	job.Cancel()
	return true
}

func (jm *JobManager) GetActiveJobs() []*Job {
	jm.mu.RLock()
	defer jm.mu.RUnlock()
	list := make([]*Job, 0, len(jm.jobs))
	for _, j := range jm.jobs {
		list = append(list, j)
	}
	return list
}
