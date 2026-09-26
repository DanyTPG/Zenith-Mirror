package main

import (
	"path/filepath"
	"testing"
)

func TestFeedFilterMatching(t *testing.T) {
	tests := []struct {
		title    string
		includes []string
		excludes []string
		expected bool
	}{
		{
			title:    "Dark.2024.S02E03.1080p.WEBRip.x265",
			includes: []string{"1080p,2160p", "WEBRip"},
			excludes: []string{"CAM,TeleSync"},
			expected: true,
		},
		{
			title:    "Dark.2024.S02E03.2160p.UHD.Remux",
			includes: []string{"1080p,2160p", "Remux"},
			excludes: []string{"CAM"},
			expected: true,
		},
		{
			title:    "Dark.2024.S02E03.720p.HDTV",
			includes: []string{"1080p,2160p"},
			excludes: []string{},
			expected: false,
		},
		{
			title:    "Dark.2024.S02E03.1080p.CAMRip",
			includes: []string{"1080p"},
			excludes: []string{"CAM,TeleSync"},
			expected: false,
		},
		{
			title:    "Anime.Episode.01.Subbed.mkv",
			includes: []string{},
			excludes: []string{"Dubbed"},
			expected: true,
		},
		{
			title:    "Anime.Episode.01.Dubbed.mkv",
			includes: []string{},
			excludes: []string{"Dubbed"},
			expected: false,
		},
	}

	for _, tt := range tests {
		got := matchesFilters(tt.title, tt.includes, tt.excludes)
		if got != tt.expected {
			t.Errorf("matchesFilters(%q, %v, %v) = %v; want %v", tt.title, tt.includes, tt.excludes, got, tt.expected)
		}
	}
}

func TestRSSXMLParsing(t *testing.T) {
	sampleRSS := `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Torrent Tracker</title>
    <link>https://tracker.example.com</link>
    <item>
      <title>Movie 1080p BluRay</title>
      <link>magnet:?xt=urn:btih:0123456789abcdef</link>
      <guid>movie-1080p-1</guid>
      <description>1080p release</description>
      <enclosure url="https://tracker.example.com/download.torrent" length="12345" type="application/x-bittorrent" />
    </item>
    <item>
      <title>Movie 720p WEB</title>
      <link>https://tracker.example.com/item/2</link>
      <guid>movie-720p-2</guid>
    </item>
  </channel>
</rss>`

	items, title, err := parseFeedXML([]byte(sampleRSS))
	if err != nil {
		t.Fatalf("failed parsing RSS: %v", err)
	}
	if title != "Torrent Tracker" {
		t.Errorf("expected channel title 'Torrent Tracker', got %q", title)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].Title != "Movie 1080p BluRay" {
		t.Errorf("unexpected title: %s", items[0].Title)
	}
	if items[0].Link != "magnet:?xt=urn:btih:0123456789abcdef" {
		t.Errorf("unexpected link: %s", items[0].Link)
	}
	if items[0].GUID != "movie-1080p-1" {
		t.Errorf("unexpected guid: %s", items[0].GUID)
	}
}

func TestAtomXMLParsing(t *testing.T) {
	sampleAtom := `<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Release Feed</title>
  <entry>
    <title>Series S01E01 1080p</title>
    <id>urn:uuid:entry-1</id>
    <link href="https://dl.example.com/s01e01.mkv" rel="enclosure"/>
    <summary>Episode 1 summary</summary>
  </entry>
</feed>`

	items, title, err := parseFeedXML([]byte(sampleAtom))
	if err != nil {
		t.Fatalf("failed parsing Atom: %v", err)
	}
	if title != "Release Feed" {
		t.Errorf("expected title 'Release Feed', got %q", title)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Title != "Series S01E01 1080p" {
		t.Errorf("unexpected title: %s", items[0].Title)
	}
	if items[0].Link != "https://dl.example.com/s01e01.mkv" {
		t.Errorf("unexpected link: %s", items[0].Link)
	}
	if items[0].GUID != "urn:uuid:entry-1" {
		t.Errorf("unexpected guid: %s", items[0].GUID)
	}
}

func TestFeedManagerPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "feeds_state.json")

	fm1 := NewFeedManager(filePath, nil)

	// Add feed directly to map to bypass network fetch in unit test
	f := &FeedSubscription{
		ID:        1,
		Name:      "test_feed",
		Mode:      FeedModeMirror,
		URL:       "https://example.com/rss",
		Includes:  []string{"1080p"},
		Excludes:  []string{"CAM"},
		UserID:    12345,
		LastGUID:  "guid-1",
		LastTitle: "Title 1",
	}
	fm1.feeds[1] = f
	fm1.feedCounter = 1
	fm1.saveStateLocked()

	// Load in second manager
	fm2 := NewFeedManager(filePath, nil)
	list := fm2.ListFeeds(12345, false)
	if len(list) != 1 {
		t.Fatalf("expected 1 feed loaded, got %d", len(list))
	}
	if list[0].Name != "test_feed" {
		t.Errorf("unexpected feed name: %s", list[0].Name)
	}
	if list[0].Mode != FeedModeMirror {
		t.Errorf("unexpected feed mode: %s", list[0].Mode)
	}

	// Test Pause
	_, err := fm2.PauseFeed(1, 12345, false)
	if err != nil {
		t.Fatalf("failed pausing feed: %v", err)
	}
	if !fm2.feeds[1].Paused {
		t.Errorf("expected feed to be paused")
	}

	// Test Delete
	_, err = fm2.DeleteFeed(1, 12345, false)
	if err != nil {
		t.Fatalf("failed deleting feed: %v", err)
	}
	if len(fm2.ListFeeds(12345, false)) != 0 {
		t.Errorf("expected 0 feeds after deletion")
	}
}
