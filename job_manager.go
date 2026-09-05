package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type JobType string

const (
	JobTypeMirror JobType = "MIRROR"
	JobTypeLeech  JobType = "LEECH"
)

type JobPhase string

const (
	PhaseDownloading JobPhase = "Downloading"
	PhaseUploading   JobPhase = "Uploading"
)

type JobState string

const (
	StateQueued    JobState = "QUEUED"
	StateRunning   JobState = "RUNNING"
	StateCancelled JobState = "CANCELLED"
	StateCompleted JobState = "COMPLETED"
)

type Job struct {
	ID        string
	Type      JobType
	FileName  string
	Size      int64
	ReadBytes int64
	Speed     float64
	ETA       time.Duration
	Phase     JobPhase
	State     JobState
	Status    string
	UserID    int64
	Order     int
	Ctx       context.Context
	Cancel    context.CancelFunc
	Execute   func()
	// Torrent-specific
	IsTorrent   bool
	Seeds       int
	Peers       int
	TorrentHash string
}

type JobManager struct {
	mu             sync.Mutex
	active         map[string]*Job
	queue          []*Job
	maxConcurrency int
	jobCounter     uint64
}

func NewJobManager(maxConcurrency int) *JobManager {
	if maxConcurrency <= 0 {
		maxConcurrency = 3
	}
	return &JobManager{
		active:         make(map[string]*Job),
		queue:          make([]*Job, 0),
		maxConcurrency: maxConcurrency,
	}
}

func (jm *JobManager) CreateJob(ctx context.Context, jobType JobType, fileName string, size int64, userID int64, execute func()) (*Job, error) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	jm.jobCounter++
	id := fmt.Sprintf("job-%d", jm.jobCounter)

	jobCtx, cancel := context.WithCancel(ctx)

	job := &Job{
		ID:        id,
		Type:      jobType,
		FileName:  fileName,
		Size:      size,
		Phase:     PhaseDownloading,
		State:     StateQueued,
		Status:    "Queued",
		UserID:    userID,
		Order:     int(jm.jobCounter),
		Ctx:       jobCtx,
		Cancel:    cancel,
		Execute:   execute,
	}

	jm.active[id] = job

	runningCount := 0
	for _, j := range jm.active {
		if j.State == StateRunning {
			runningCount++
		}
	}

	if runningCount < jm.maxConcurrency {
		job.State = StateRunning
		job.Status = "Running"
		go job.Execute()
	} else {
		jm.queue = append(jm.queue, job)
	}

	return job, nil
}

func (jm *JobManager) FinishJob(id string) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if job, ok := jm.active[id]; ok {
		job.State = StateCompleted
		delete(jm.active, id)
		job.Cancel()
	}

	for i, qJob := range jm.queue {
		if qJob.ID == id {
			jm.queue = append(jm.queue[:i], jm.queue[i+1:]...)
			break
		}
	}

	runningCount := 0
	for _, j := range jm.active {
		if j.State == StateRunning {
			runningCount++
		}
	}

	if runningCount < jm.maxConcurrency && len(jm.queue) > 0 {
		nextJob := jm.queue[0]
		jm.queue = jm.queue[1:]
		nextJob.State = StateRunning
		nextJob.Status = "Running"
		go nextJob.Execute()
	}
}

func (jm *JobManager) CancelJob(id string) bool {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	job, ok := jm.active[id]
	if !ok {
		return false
	}

	job.State = StateCancelled
	job.Status = "Cancelled"
	job.Cancel()
	delete(jm.active, id)

	for i, qJob := range jm.queue {
		if qJob.ID == id {
			jm.queue = append(jm.queue[:i], jm.queue[i+1:]...)
			break
		}
	}

	runningCount := 0
	for _, j := range jm.active {
		if j.State == StateRunning {
			runningCount++
		}
	}
	if runningCount < jm.maxConcurrency && len(jm.queue) > 0 {
		nextJob := jm.queue[0]
		jm.queue = jm.queue[1:]
		nextJob.State = StateRunning
		nextJob.Status = "Running"
		go nextJob.Execute()
	}

	return true
}

func (jm *JobManager) CancelAllJobs() int {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	count := len(jm.active)
	for _, job := range jm.active {
		job.State = StateCancelled
		job.Status = "Cancelled"
		job.Cancel()
	}

	jm.active = make(map[string]*Job)
	jm.queue = make([]*Job, 0)
	return count
}

func (jm *JobManager) GetActiveJobs() []*Job {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	jobs := make([]*Job, 0, len(jm.active))
	for _, j := range jm.active {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(i, k int) bool {
		return jobs[i].Order < jobs[k].Order
	})
	return jobs
}

func (jm *JobManager) GetActiveJobCount() int {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	return len(jm.active)
}
