package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gotd/td/tg"
)

type DMUser struct {
	UserID     int64     `json:"user_id"`
	AccessHash int64     `json:"access_hash"`
	Username   string    `json:"username,omitempty"`
	FirstName  string    `json:"first_name,omitempty"`
	StartedAt  time.Time `json:"started_at"`
}

type DMUserManager struct {
	mu       sync.RWMutex
	filePath string
	users    map[int64]DMUser
}

func NewDMUserManager(filePath string) *DMUserManager {
	if filePath == "" {
		filePath = "dm_users.json"
	}
	mgr := &DMUserManager{
		filePath: filePath,
		users:    make(map[int64]DMUser),
	}
	_ = mgr.Load()
	return mgr
}

func (m *DMUserManager) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var list []DMUser
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}

	m.users = make(map[int64]DMUser, len(list))
	for _, u := range list {
		m.users[u.UserID] = u
	}
	return nil
}

func (m *DMUserManager) SaveLocked() error {
	list := make([]DMUser, 0, len(m.users))
	for _, u := range m.users {
		list = append(list, u)
	}

	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}

	tmp := fmt.Sprintf("%s.tmp.%d", m.filePath, time.Now().UnixNano())
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, m.filePath)
}

func (m *DMUserManager) Register(userID, accessHash int64, username, firstName string) {
	if userID == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.users[userID]
	startedAt := time.Now()
	if exists && !existing.StartedAt.IsZero() {
		startedAt = existing.StartedAt
	}
	if accessHash == 0 && exists {
		accessHash = existing.AccessHash
	}

	m.users[userID] = DMUser{
		UserID:     userID,
		AccessHash: accessHash,
		Username:   username,
		FirstName:  firstName,
		StartedAt:  startedAt,
	}
	_ = m.SaveLocked()
}

func (m *DMUserManager) Unregister(userID int64) {
	if userID == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.users[userID]; ok {
		delete(m.users, userID)
		_ = m.SaveLocked()
	}
}

func (m *DMUserManager) HasUser(userID int64) bool {
	if userID == 0 {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.users[userID]
	return ok && (u.AccessHash != 0 || u.UserID != 0)
}

func (m *DMUserManager) GetUser(userID int64) (DMUser, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.users[userID]
	return u, ok
}

func (m *DMUserManager) InputPeer(userID int64) (tg.InputPeerClass, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.users[userID]
	if !ok {
		return nil, false
	}
	return &tg.InputPeerUser{
		UserID:     u.UserID,
		AccessHash: u.AccessHash,
	}, true
}
