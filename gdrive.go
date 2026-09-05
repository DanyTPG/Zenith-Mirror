package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"path/filepath"
	"mime"
	"google.golang.org/api/option"
)

const DriveChunkSize = 1 * 1024 * 1024 // 1MB chunk alignment

type GDriveService struct {
	service  *drive.Service
	folderID string
}

func NewGDriveService(ctx context.Context, cfg *Config) (*GDriveService, error) {
	var client *http.Client
	var err error

	if cfg.GDriveCredentialsFile != "" {
		slog.Info("initializing Google Drive via OAuth2 User Authorization")
		client, err = getOAuthClient(ctx, cfg.GDriveCredentialsFile, cfg.GDriveTokenFile)
		if err != nil {
			return nil, fmt.Errorf("failed oauth client setup: %w", err)
		}
	} else if os.Getenv("SERVICE_ACCOUNT_FILE") != "" {
		saFile := os.Getenv("SERVICE_ACCOUNT_FILE")
		slog.Info("initializing Google Drive via Service Account JSON", "sa_file", saFile)
		b, err := os.ReadFile(saFile)
		if err != nil {
			return nil, fmt.Errorf("failed reading service account file: %w", err)
		}
		creds, err := google.CredentialsFromJSON(ctx, b, drive.DriveFileScope)
		if err != nil {
			return nil, fmt.Errorf("failed parsing service account json: %w", err)
		}
		client = oauth2.NewClient(ctx, creds.TokenSource)
	} else {
		return nil, fmt.Errorf("neither gdrive_credentials_file nor SERVICE_ACCOUNT_FILE configured")
	}

	srv, err := drive.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve Drive client: %w", err)
	}

	return &GDriveService{
		service:  srv,
		folderID: cfg.GDriveFolderID,
	}, nil
}

func getOAuthClient(ctx context.Context, credsFile, tokenFile string) (*http.Client, error) {
	b, err := os.ReadFile(credsFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read client secret file %s: %w", credsFile, err)
	}

	config, err := google.ConfigFromJSON(b, drive.DriveFileScope)
	if err != nil {
		return nil, fmt.Errorf("unable to parse client secret to config: %w", err)
	}

	tok, err := tokenFromFile(tokenFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		_ = saveToken(tokenFile, tok)
	}
	return config.Client(ctx, tok), nil
}

func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		slog.Error("unable to read authorization code", "error", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		slog.Error("unable to retrieve token from web", "error", err)
	}
	return tok
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

func saveToken(path string, token *oauth2.Token) error {
	slog.Info("saving credential file to path", "path", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("unable to cache oauth token: %w", err)
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(token)
}

func (s *GDriveService) UploadStream(ctx context.Context, name string, r io.Reader, size int64) (string, error) {
	return s.UploadStreamToFolder(ctx, name, r, size, s.folderID)
}

func (s *GDriveService) UploadStreamToFolder(ctx context.Context, name string, r io.Reader, size int64, parentFolderID string) (string, error) {
	f := &drive.File{
		Name: name,
	}
	if parentFolderID != "" {
		f.Parents = []string{parentFolderID}
	}

	mimeType := mime.TypeByExtension(filepath.Ext(name))
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	call := s.service.Files.Create(f).Media(r, googleapi.ChunkSize(DriveChunkSize), googleapi.ContentType(mimeType)).Context(ctx)
	res, err := call.Do()
	if err != nil {
		return "", fmt.Errorf("drive upload failed: %w", err)
	}

	permission := &drive.Permission{
		Type: "anyone",
		Role: "reader",
	}
	_, _ = s.service.Permissions.Create(res.Id, permission).Context(ctx).Do()

	webLink := fmt.Sprintf("https://drive.google.com/file/d/%s/view", res.Id)
	return webLink, nil
}

func (s *GDriveService) EnsureFolder(ctx context.Context, name string, parentFolderID string) (string, error) {
	query := fmt.Sprintf("name = '%s' and mimeType = 'application/vnd.google-apps.folder' and trashed = false", name)
	if parentFolderID != "" {
		query += fmt.Sprintf(" and '%s' in parents", parentFolderID)
	}
	r, err := s.service.Files.List().Q(query).Fields("files(id, name)").Context(ctx).Do()
	if err == nil && len(r.Files) > 0 {
		return r.Files[0].Id, nil
	}

	folder := &drive.File{
		Name:     name,
		MimeType: "application/vnd.google-apps.folder",
	}
	if parentFolderID != "" {
		folder.Parents = []string{parentFolderID}
	}
	created, err := s.service.Files.Create(folder).Fields("id").Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("create drive folder %q: %w", name, err)
	}

	permission := &drive.Permission{
		Type: "anyone",
		Role: "reader",
	}
	_, _ = s.service.Permissions.Create(created.Id, permission).Context(ctx).Do()

	return created.Id, nil
}
