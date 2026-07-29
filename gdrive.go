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
	"google.golang.org/api/option"
)

const DriveChunkSize = 1024 * 1024

type GDriveService struct {
	service  *drive.Service
	folderID string
}

func NewGDriveService(ctx context.Context, cfg *Config) (*GDriveService, error) {
	var client *http.Client
	var err error

	if cfg.GDriveCredentials != "" {
		slog.Info("initializing Google Drive via OAuth2 User Authorization")
		client, err = getOAuthClient(ctx, cfg.GDriveCredentials, cfg.GDriveTokenFile)
		if err != nil {
			return nil, fmt.Errorf("failed oauth authorization: %w", err)
		}
	} else {
		slog.Info("initializing Google Drive via Service Account")
		srv, err := drive.NewService(ctx, option.WithCredentialsFile(cfg.GDriveSAFile))
		if err != nil {
			return nil, fmt.Errorf("failed to create drive service: %w", err)
		}
		return &GDriveService{service: srv, folderID: cfg.GDriveFolderID}, nil
	}

	srv, err := drive.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("failed creating drive service from client: %w", err)
	}

	return &GDriveService{
		service:  srv,
		folderID: cfg.GDriveFolderID,
	}, nil
}

func getOAuthClient(ctx context.Context, credFile, tokenFile string) (*http.Client, error) {
	b, err := os.ReadFile(credFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read client secret file: %w", err)
	}

	config, err := google.ConfigFromJSON(b, drive.DriveFileScope)
	if err != nil {
		return nil, fmt.Errorf("unable to parse client secret to config: %w", err)
	}

	tok, err := tokenFromFile(tokenFile)
	if err != nil {
		slog.Info("no token file found, generating authorization URL")
		tok, err = getTokenFromWeb(config)
		if err != nil {
			return nil, err
		}
		_ = saveToken(tokenFile, tok)
	}

	return config.Client(ctx, tok), nil
}

func getTokenFromWeb(config *oauth2.Config) (*oauth2.Token, error) {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("\n=======================================================\n")
	fmt.Printf("Go to the following link in your browser:\n\n%v\n\n", authURL)
	fmt.Printf("Paste the authorization code here and press Enter: ")
	fmt.Printf("\n=======================================================\n\n")

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		return nil, fmt.Errorf("unable to read authorization code: %w", err)
	}

	tok, err := config.Exchange(context.TODO(), authCode)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve token from web: %w", err)
	}
	return tok, nil
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
	slog.Info("saving OAuth token to file", "path", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("unable to cache oauth token: %w", err)
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(token)
}

func (g *GDriveService) UploadStream(ctx context.Context, name string, reader io.Reader, size int64) (string, error) {
	fileMeta := &drive.File{
		Name:    name,
		Parents: []string{g.folderID},
	}

	call := g.service.Files.Create(fileMeta).Context(ctx)
	call.Media(reader, googleapi.ChunkSize(DriveChunkSize))

	res, err := call.Do()
	if err != nil {
		return "", fmt.Errorf("drive upload failed: %w", err)
	}

	slog.Info("gdrive upload complete", "file_id", res.Id, "name", name)
	return fmt.Sprintf("https://drive.google.com/file/d/%s/view", res.Id), nil
}
