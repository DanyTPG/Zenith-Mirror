package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FeedMode string

const (
	FeedModeMirror FeedMode = "mirror"
	FeedModeLeech  FeedMode = "leech"
	FeedModeNotify FeedMode = "notify"
)

type FeedItem struct {
	Title       string
	Link        string
	GUID        string
	Description string
	Published   string
}

type FeedSubscription struct {
	ID          int       `json:"id"`
	Name        string    `json:"name"`
	Mode        FeedMode  `json:"mode"`
	URL         string    `json:"url"`
	Includes    []string  `json:"includes"`
	Excludes    []string  `json:"excludes"`
	UserID      int64     `json:"user_id"`
	Target      JobTarget `json:"target"`
	LastGUID    string    `json:"last_guid"`
	LastTitle   string    `json:"last_title"`
	Paused      bool      `json:"paused"`
	LastChecked time.Time `json:"last_checked"`
	CreatedAt   time.Time `json:"created_at"`
}

// XML structures for RSS 2.0
type rssRoot struct {
	XMLName xml.Name `xml:"rss"`
	Channel struct {
		Title string    `xml:"title"`
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
}

type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	GUID        string `xml:"guid"`
	Description string `xml:"description"`
	PubDate     string `xml:"pubDate"`
	Enclosure   struct {
		URL    string `xml:"url,attr"`
		Length int64  `xml:"length,attr"`
		Type   string `xml:"type,attr"`
	} `xml:"enclosure"`
}

// XML structures for Atom 1.0
type atomRoot struct {
	XMLName xml.Name    `xml:"feed"`
	Title   string      `xml:"title"`
	Entries []atomEntry `xml:"entry"`
}

type atomEntry struct {
	Title   string `xml:"title"`
	ID      string `xml:"id"`
	Updated string `xml:"updated"`
	Summary string `xml:"summary"`
	Links   []struct {
		Href string `xml:"href,attr"`
		Rel  string `xml:"rel,attr"`
	} `xml:"link"`
}

// parseFeedXML attempts to unmarshal RSS 2.0 or Atom 1.0 XML data.
func parseFeedXML(data []byte) ([]FeedItem, string, error) {
	// 1. Try RSS 2.0
	var rss rssRoot
	if err := xml.Unmarshal(data, &rss); err == nil && (len(rss.Channel.Items) > 0 || rss.Channel.Title != "") {
		items := make([]FeedItem, 0, len(rss.Channel.Items))
		for _, it := range rss.Channel.Items {
			link := strings.TrimSpace(it.Link)
			if link == "" && it.Enclosure.URL != "" {
				link = strings.TrimSpace(it.Enclosure.URL)
			}
			guid := strings.TrimSpace(it.GUID)
			if guid == "" {
				guid = link
			}
			if guid == "" {
				guid = strings.TrimSpace(it.Title)
			}
			items = append(items, FeedItem{
				Title:       strings.TrimSpace(it.Title),
				Link:        link,
				GUID:        guid,
				Description: strings.TrimSpace(it.Description),
				Published:   strings.TrimSpace(it.PubDate),
			})
		}
		return items, rss.Channel.Title, nil
	}

	// 2. Try Atom 1.0
	var atom atomRoot
	if err := xml.Unmarshal(data, &atom); err == nil && (len(atom.Entries) > 0 || atom.Title != "") {
		items := make([]FeedItem, 0, len(atom.Entries))
		for _, en := range atom.Entries {
			link := ""
			for _, l := range en.Links {
				if l.Rel == "enclosure" && l.Href != "" {
					link = strings.TrimSpace(l.Href)
					break
				}
			}
			if link == "" && len(en.Links) > 0 {
				link = strings.TrimSpace(en.Links[0].Href)
			}
			guid := strings.TrimSpace(en.ID)
			if guid == "" {
				guid = link
			}
			if guid == "" {
				guid = strings.TrimSpace(en.Title)
			}
			items = append(items, FeedItem{
				Title:       strings.TrimSpace(en.Title),
				Link:        link,
				GUID:        guid,
				Description: strings.TrimSpace(en.Summary),
				Published:   strings.TrimSpace(en.Updated),
			})
		}
		return items, atom.Title, nil
	}

	return nil, "", errors.New("unrecognized feed format (neither RSS 2.0 nor Atom)")
}

// matchesFilters checks if item title matches include groups and doesn't match exclude groups.
// include group "+1080p,2160p" requires 1080p OR 2160p.
// exclude group "-CAM,TeleSync" rejects if CAM OR TeleSync present.
func matchesFilters(title string, includes []string, excludes []string) bool {
	titleLower := strings.ToLower(title)

	// All include groups must be satisfied
	for _, inc := range includes {
		parts := strings.Split(inc, ",")
		matchedGroup := false
		for _, p := range parts {
			clean := strings.TrimSpace(strings.ToLower(p))
			if clean != "" && strings.Contains(titleLower, clean) {
				matchedGroup = true
				break
			}
		}
		if !matchedGroup && len(parts) > 0 && strings.TrimSpace(parts[0]) != "" {
			return false
		}
	}

	// Any exclude hit rejects
	for _, exc := range excludes {
		parts := strings.Split(exc, ",")
		for _, p := range parts {
			clean := strings.TrimSpace(strings.ToLower(p))
			if clean != "" && strings.Contains(titleLower, clean) {
				return false
			}
		}
	}

	return true
}

type FeedManager struct {
	mu          sync.Mutex
	filePath    string
	feeds       map[int]*FeedSubscription
	feedCounter int
	client      *http.Client
	ts          *TelegramService
}

func NewFeedManager(filePath string, ts *TelegramService) *FeedManager {
	fm := &FeedManager{
		filePath: filePath,
		feeds:    make(map[int]*FeedSubscription),
		client: &http.Client{
			Timeout: 20 * time.Second,
		},
		ts: ts,
	}
	_ = fm.LoadState()
	return fm
}

func (fm *FeedManager) LoadState() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.filePath == "" {
		return nil
	}
	data, err := os.ReadFile(fm.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(data) == 0 {
		return nil
	}

	var list []*FeedSubscription
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}

	maxID := 0
	for _, f := range list {
		fm.feeds[f.ID] = f
		if f.ID > maxID {
			maxID = f.ID
		}
	}
	fm.feedCounter = maxID
	return nil
}

func (fm *FeedManager) SaveState() {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.saveStateLocked()
}

func (fm *FeedManager) saveStateLocked() {
	if fm.filePath == "" {
		return
	}
	list := make([]*FeedSubscription, 0, len(fm.feeds))
	for _, f := range fm.feeds {
		list = append(list, f)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].ID < list[j].ID
	})

	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return
	}
	tmp := fm.filePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err == nil {
		_ = os.Rename(tmp, fm.filePath)
	}
}

func (fm *FeedManager) FetchFeedItems(ctx context.Context, feedURL string) ([]FeedItem, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feedURL, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; Zenith-Mirror-Bot/1.0)")

	resp, err := fm.client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("HTTP status %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024))
	if err != nil {
		return nil, "", err
	}

	return parseFeedXML(body)
}

func (fm *FeedManager) AddFeed(ctx context.Context, mode FeedMode, name, rawURL string, includes, excludes []string, userID int64, target JobTarget) (*FeedSubscription, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	mode = FeedMode(strings.ToLower(string(mode)))
	if mode != FeedModeMirror && mode != FeedModeLeech && mode != FeedModeNotify {
		return nil, fmt.Errorf("invalid mode %q: choose mirror, leech, or notify", mode)
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("feed name cannot be empty")
	}

	if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
		return nil, errors.New("URL must start with http:// or https://")
	}

	for _, f := range fm.feeds {
		if f.UserID == userID && strings.EqualFold(f.Name, name) {
			return nil, fmt.Errorf("you already have a feed named %q", name)
		}
	}

	// Fetch to initialize baseline GUID so past items are not flood-downloaded
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	items, _, err := fm.FetchFeedItems(probeCtx, rawURL)
	if err != nil {
		return nil, fmt.Errorf("failed fetching feed URL: %w", err)
	}

	lastGUID := ""
	lastTitle := ""
	if len(items) > 0 {
		lastGUID = items[0].GUID
		lastTitle = items[0].Title
	}

	fm.feedCounter++
	feed := &FeedSubscription{
		ID:          fm.feedCounter,
		Name:        name,
		Mode:        mode,
		URL:         rawURL,
		Includes:    includes,
		Excludes:    excludes,
		UserID:      userID,
		Target:      target,
		LastGUID:    lastGUID,
		LastTitle:   lastTitle,
		Paused:      false,
		LastChecked: time.Now(),
		CreatedAt:   time.Now(),
	}

	fm.feeds[feed.ID] = feed
	fm.saveStateLocked()
	return feed, nil
}

func (fm *FeedManager) ListFeeds(userID int64, isOwner bool) []*FeedSubscription {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	var result []*FeedSubscription
	for _, f := range fm.feeds {
		if isOwner || f.UserID == userID {
			result = append(result, f)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result
}

func (fm *FeedManager) GetFeed(identifier string, userID int64, isOwner bool) *FeedSubscription {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	id, err := strconv.Atoi(identifier)
	for _, f := range fm.feeds {
		if !isOwner && f.UserID != userID {
			continue
		}
		if err == nil && f.ID == id {
			return f
		}
		if strings.EqualFold(f.Name, identifier) {
			return f
		}
	}
	return nil
}

func (fm *FeedManager) PauseFeed(id int, userID int64, isOwner bool) (*FeedSubscription, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, ok := fm.feeds[id]
	if !ok {
		return nil, fmt.Errorf("feed #%d not found", id)
	}
	if !isOwner && f.UserID != userID {
		return nil, errors.New("unauthorized to modify this feed")
	}
	f.Paused = true
	fm.saveStateLocked()
	return f, nil
}

func (fm *FeedManager) ResumeFeed(id int, userID int64, isOwner bool) (*FeedSubscription, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, ok := fm.feeds[id]
	if !ok {
		return nil, fmt.Errorf("feed #%d not found", id)
	}
	if !isOwner && f.UserID != userID {
		return nil, errors.New("unauthorized to modify this feed")
	}
	f.Paused = false
	fm.saveStateLocked()
	return f, nil
}

func (fm *FeedManager) DeleteFeed(id int, userID int64, isOwner bool) (*FeedSubscription, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, ok := fm.feeds[id]
	if !ok {
		return nil, fmt.Errorf("feed #%d not found", id)
	}
	if !isOwner && f.UserID != userID {
		return nil, errors.New("unauthorized to delete this feed")
	}
	delete(fm.feeds, id)
	fm.saveStateLocked()
	return f, nil
}

// CheckFeed fetches the feed and executes matches that appeared since LastGUID.
func (fm *FeedManager) CheckFeed(ctx context.Context, f *FeedSubscription) (int, error) {
	items, _, err := fm.FetchFeedItems(ctx, f.URL)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}

	// Find index of last seen item
	lastIdx := -1
	for i, item := range items {
		if (f.LastGUID != "" && item.GUID == f.LastGUID) || (f.LastTitle != "" && item.Title == f.LastTitle) {
			lastIdx = i
			break
		}
	}

	// New items are from 0 up to lastIdx (exclusive). If not found, all items are considered new if LastGUID was empty,
	// or only the top 5 to avoid unexpected flood if the feed completely rolled over.
	var newItems []FeedItem
	if lastIdx > 0 {
		newItems = items[:lastIdx]
	} else if lastIdx == -1 {
		if f.LastGUID == "" {
			newItems = items[:1] // brand new: process only latest
		} else {
			// Feed rolled over completely: process max 5 latest
			maxLimit := 5
			if len(items) < maxLimit {
				maxLimit = len(items)
			}
			newItems = items[:maxLimit]
		}
	}

	// Process new items in chronological order (oldest unread first)
	matchCount := 0
	for i := len(newItems) - 1; i >= 0; i-- {
		item := newItems[i]
		if !matchesFilters(item.Title, f.Includes, f.Excludes) {
			continue
		}

		matchCount++
		slog.Info("feed matched item", "feed_id", f.ID, "feed_name", f.Name, "title", item.Title, "mode", f.Mode)

		if fm.ts != nil {
			switch f.Mode {
			case FeedModeNotify:
				fm.ts.notifyFeedMatch(f, item)
			case FeedModeMirror:
				fm.ts.createFeedMirrorJob(ctx, f, item.Title, item.Link)
			case FeedModeLeech:
				fm.ts.createFeedLeechJob(ctx, f, item.Title, item.Link)
			}
		}
	}

	// Update state
	fm.mu.Lock()
	f.LastGUID = items[0].GUID
	f.LastTitle = items[0].Title
	f.LastChecked = time.Now()
	fm.saveStateLocked()
	fm.mu.Unlock()

	return matchCount, nil
}

func (fm *FeedManager) StartBackgroundPoller(ctx context.Context, interval time.Duration) {
	if interval < 30*time.Second {
		interval = 10 * time.Minute
	}
	slog.Info("starting feed background poller", "interval", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fm.mu.Lock()
			var activeFeeds []*FeedSubscription
			for _, f := range fm.feeds {
				if !f.Paused {
					activeFeeds = append(activeFeeds, f)
				}
			}
			fm.mu.Unlock()

			for _, f := range activeFeeds {
				checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				_, err := fm.CheckFeed(checkCtx, f)
				cancel()
				if err != nil {
					slog.Warn("feed check error", "feed_id", f.ID, "name", f.Name, "error", err)
				}
				// Small delay between feeds to prevent CPU/network spikes
				time.Sleep(2 * time.Second)
			}
		}
	}
}
