package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDMUserManager(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "test_dm_users.json")

	mgr := NewDMUserManager(storePath)
	if mgr.HasUser(12345) {
		t.Fatal("expected user not to exist")
	}

	mgr.Register(12345, 987654, "testuser", "Test")
	if !mgr.HasUser(12345) {
		t.Fatal("expected user to exist after registration")
	}

	u, ok := mgr.GetUser(12345)
	if !ok || u.Username != "testuser" || u.AccessHash != 987654 {
		t.Fatalf("unexpected user details: %+v", u)
	}

	peer, ok := mgr.InputPeer(12345)
	if !ok || peer == nil {
		t.Fatal("expected valid input peer")
	}

	// Reload from disk to verify persistence
	mgr2 := NewDMUserManager(storePath)
	if !mgr2.HasUser(12345) {
		t.Fatal("expected persisted user in new manager instance")
	}
	u2, _ := mgr2.GetUser(12345)
	if u2.Username != "testuser" || u2.AccessHash != 987654 {
		t.Fatalf("unexpected persisted user details: %+v", u2)
	}

	// Test unregister
	mgr2.Unregister(12345)
	if mgr2.HasUser(12345) {
		t.Fatal("expected user to be removed after unregister")
	}

	mgr3 := NewDMUserManager(storePath)
	if mgr3.HasUser(12345) {
		t.Fatal("expected user to remain removed in reloaded instance")
	}

	_ = os.Remove(storePath)
}
