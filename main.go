package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/joho/godotenv"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Get environment variables
	dbHost := os.Getenv("DB_HOST")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbName := os.Getenv("DB_NAME")
	driveFolderID := os.Getenv("DRIVE_FOLDER_ID")
	sevenZPassword := os.Getenv("SEVENZ_PASSWORD")
	updateQuery := os.Getenv("UPDATE_QUERY")
	serviceAccountFile := os.Getenv("SERVICE_ACCOUNT_FILE")

	if dbHost == "" || dbName == "" || driveFolderID == "" || sevenZPassword == "" || updateQuery == "" || serviceAccountFile == "" {
		log.Fatal("Missing required environment variables")
	}

	// Authenticate with Google Drive
	ctx := context.Background()
	srv, err := drive.NewService(ctx, option.WithCredentialsFile(serviceAccountFile))
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}

	// Get files from folder
	files, err := getFilesFromFolder(srv, driveFolderID)
	if err != nil {
		log.Fatalf("Unable to get files: %v", err)
	}

	// Process each file
	for _, file := range files {
		err := processFile(srv, file, dbHost, dbUser, dbPass, dbName, sevenZPassword, updateQuery)
		if err != nil {
			log.Printf("Error processing file %s: %v", file.Name, err)
			continue
		}
		log.Printf("Processed file %s successfully", file.Name)
	}
}

func getFilesFromFolder(srv *drive.Service, folderID string) ([]*drive.File, error) {
	query := fmt.Sprintf("'%s' in parents and trashed = false", folderID)
	fileList, err := srv.Files.List().Q(query).Fields("files(id, name)").Do()
	if err != nil {
		return nil, err
	}
	return fileList.Files, nil
}

func processFile(srv *drive.Service, file *drive.File, dbHost, dbUser, dbPass, dbName, password, updateQuery string) error {
	// Create temp dir
	tempDir, err := os.MkdirTemp("", "backup-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	// Download file
	downloadedFile := filepath.Join(tempDir, file.Name)
	err = downloadFile(srv, file.Id, downloadedFile)
	if err != nil {
		return err
	}

	// Extract 7z
	extractDir := filepath.Join(tempDir, "extracted")
	err = extract7z(downloadedFile, extractDir, password)
	if err != nil {
		return err
	}

	// Find .bak file
	bakFile, err := findBakFile(extractDir)
	if err != nil {
		return err
	}

	// Restore DB
	err = restoreDB(dbHost, dbUser, dbPass, dbName, bakFile)
	if err != nil {
		return err
	}

	// Run update query
	err = runUpdateQuery(dbHost, dbUser, dbPass, dbName, updateQuery)
	if err != nil {
		return err
	}

	// Delete Drive file
	err = srv.Files.Delete(file.Id).Do()
	if err != nil {
		return err
	}

	return nil
}

func downloadFile(srv *drive.Service, fileID, destPath string) error {
	resp, err := srv.Files.Get(fileID).Download()
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func extract7z(archivePath, destDir, password string) error {
	cmd := exec.Command("7z", "x", "-p"+password, archivePath, "-o"+destDir)
	return cmd.Run()
}

func findBakFile(dir string) (string, error) {
	var bakFile string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(info.Name(), ".bak") {
			bakFile = path
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if bakFile == "" {
		return "", fmt.Errorf("no .bak file found")
	}
	return bakFile, nil
}

func restoreDB(host, user, pass, dbName, bakPath string) error {
	var connString string
	if user == "" && pass == "" {
		connString = fmt.Sprintf("sqlserver://%s?database=master&integrated security=true", host)
	} else {
		connString = fmt.Sprintf("sqlserver://%s:%s@%s?database=master", user, pass, host)
	}
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return err
	}
	defer db.Close()

	query := fmt.Sprintf("RESTORE DATABASE %s FROM DISK = '%s' WITH REPLACE", dbName, bakPath)
	_, err = db.Exec(query)
	return err
}

func runUpdateQuery(host, user, pass, dbName, query string) error {
	var connString string
	if user == "" && pass == "" {
		connString = fmt.Sprintf("sqlserver://%s?database=%s&integrated security=true", host, dbName)
	} else {
		connString = fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", user, pass, host, dbName)
	}
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(query)
	return err
}
