package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

type JobTarget struct {
	PeerType   string `json:"peer_type"` // "user", "chat", "channel"
	UserID     int64  `json:"user_id,omitempty"`
	ChatID     int64  `json:"chat_id,omitempty"`
	ChannelID  int64  `json:"channel_id,omitempty"`
	AccessHash int64  `json:"access_hash,omitempty"`
	ReplyMsgID int    `json:"reply_msg_id"`
}

type StoredLocation struct {
	Type          string `json:"type"` // "doc" or "photo"
	ID            int64  `json:"id"`
	AccessHash    int64  `json:"access_hash"`
	FileReference []byte `json:"file_reference"`
	ThumbSize     string `json:"thumb_size,omitempty"`
}

type PersistedJob struct {
	ID           string          `json:"id"`
	Type         JobType         `json:"type"`
	Kind         string          `json:"kind"` // "url_mirror", "url_leech", "tg_mirror", "torrent_mirror", "torrent_leech"
	FileName     string          `json:"file_name"`
	Size         int64           `json:"size"`
	UserID       int64           `json:"user_id"`
	Target       JobTarget       `json:"target"`
	RawURL       string          `json:"raw_url,omitempty"`
	MagnetURI    string          `json:"magnet_uri,omitempty"`
	TorrentBytes []byte          `json:"torrent_bytes,omitempty"`
	Location     *StoredLocation `json:"location,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
}

type Job struct {
	ID        string
	Type      JobType
	Kind      string
	FileName  string
	Size      int64
	ReadBytes int64
	Speed     float64
	ETA       time.Duration
	Phase     JobPhase
	State     JobState
	Status    string
	UserID    int64
	Target    JobTarget
	Order     int
	Ctx       context.Context
	Cancel    context.CancelFunc
	Execute   func()

	// Source metadata for restart resume
	RawURL       string
	MagnetURI    string
	TorrentBytes []byte
	Location     *StoredLocation

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
	stateFilePath  string
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

func (jm *JobManager) SetStateFile(path string) {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	jm.stateFilePath = path
}

func (jm *JobManager) saveStateLocked() {
	if jm.stateFilePath == "" {
		return
	}
	records := make([]PersistedJob, 0, len(jm.active)+len(jm.queue))
	for _, j := range jm.active {
		if j.State == StateRunning || j.State == StateQueued {
			records = append(records, PersistedJob{
				ID:           j.ID,
				Type:         j.Type,
				Kind:         j.Kind,
				FileName:     j.FileName,
				Size:         j.Size,
				UserID:       j.UserID,
				Target:       j.Target,
				RawURL:       j.RawURL,
				MagnetURI:    j.MagnetURI,
				TorrentBytes: j.TorrentBytes,
				Location:     j.Location,
				CreatedAt:    time.Now(),
			})
		}
	}
	data, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return
	}
	tmp := jm.stateFilePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err == nil {
		_ = os.Rename(tmp, jm.stateFilePath)
	}
}

func (jm *JobManager) SaveState() {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	jm.saveStateLocked()
}

func (jm *JobManager) LoadPersistedState() ([]PersistedJob, error) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	if jm.stateFilePath == "" {
		return nil, nil
	}
	data, err := os.ReadFile(jm.stateFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var records []PersistedJob
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	return records, nil
}

func (jm *JobManager) RemovePersistedJob(id string) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	delete(jm.active, id)
	for i, qJob := range jm.queue {
		if qJob.ID == id {
			jm.queue = append(jm.queue[:i], jm.queue[i+1:]...)
			break
		}
	}
	jm.saveStateLocked()
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

	jm.saveStateLocked()
	return job, nil
}

func (jm *JobManager) CreateRecoveredJob(ctx context.Context, pj PersistedJob, execute func()) (*Job, error) {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	var num uint64
	if _, err := fmt.Sscanf(pj.ID, "job-%d", &num); err == nil && num > jm.jobCounter {
		jm.jobCounter = num
	}

	jobCtx, cancel := context.WithCancel(ctx)
	job := &Job{
		ID:           pj.ID,
		Type:         pj.Type,
		Kind:         pj.Kind,
		FileName:     pj.FileName,
		Size:         pj.Size,
		Phase:        PhaseDownloading,
		State:        StateQueued,
		Status:       "Resuming after restart...",
		UserID:       pj.UserID,
		Target:       pj.Target,
		RawURL:       pj.RawURL,
		MagnetURI:    pj.MagnetURI,
		TorrentBytes: pj.TorrentBytes,
		Location:     pj.Location,
		Order:        int(jm.jobCounter),
		Ctx:          jobCtx,
		Cancel:       cancel,
		Execute:      execute,
	}

	jm.active[pj.ID] = job

	runningCount := 0
	for _, j := range jm.active {
		if j.State == StateRunning {
			runningCount++
		}
	}

	if runningCount < jm.maxConcurrency {
		job.State = StateRunning
		job.Status = "Resuming"
		go job.Execute()
	} else {
		jm.queue = append(jm.queue, job)
	}

	jm.saveStateLocked()
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

	jm.saveStateLocked()
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

	jm.saveStateLocked()
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
	jm.saveStateLocked()
	return count
}

func (jm *JobManager) Stop() {
	jm.mu.Lock()
	defer jm.mu.Unlock()

	for _, job := range jm.active {
		job.Cancel()
	}
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
