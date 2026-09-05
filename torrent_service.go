package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
)

type TorrentService struct {
	client *torrent.Client
	cfg    *Config
}

func NewTorrentService(cfg *Config) (*TorrentService, error) {
	if err := os.MkdirAll(cfg.TorrentDownloadDir, 0755); err != nil {
		return nil, fmt.Errorf("create torrent dir: %w", err)
	}
	cc := torrent.NewDefaultClientConfig()
	cc.DataDir = cfg.TorrentDownloadDir
	cc.Seed = false
	cc.DisablePEX = false
	cc.NoUpload = false
	if cfg.TorrentListenPort != 0 {
		cc.ListenPort = cfg.TorrentListenPort
	}
	// reduce noise
	// cc.Debug = false
	client, err := torrent.NewClient(cc)
	if err != nil {
		return nil, fmt.Errorf("torrent client: %w", err)
	}
	slog.Info("torrent service ready", "data_dir", cfg.TorrentDownloadDir, "listen_port", cc.ListenPort)
	return &TorrentService{client: client, cfg: cfg}, nil
}

func (s *TorrentService) AddMagnet(magnetURI string) (*torrent.Torrent, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("torrent service disabled")
	}
	t, err := s.client.AddMagnet(magnetURI)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (s *TorrentService) AddTorrentBytes(data []byte) (*torrent.Torrent, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("torrent service disabled")
	}
	mi, err := metainfo.Load(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parse torrent: %w", err)
	}
	t, err := s.client.AddTorrent(mi)
	if err != nil {
		return nil, fmt.Errorf("add torrent: %w", err)
	}
	return t, nil
}

func (s *TorrentService) WaitForInfo(ctx context.Context, t *torrent.Torrent) error {
	select {
	case <-t.GotInfo():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *TorrentService) Drop(t *torrent.Torrent) {
	if t != nil {
		t.Drop()
	}
}

func (s *TorrentService) Close() {
	if s != nil && s.client != nil {
		s.client.Close()
	}
}
