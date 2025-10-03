package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

func main() {
	log.Println("Starting backup-otomatis application")

	// Load .env file
	log.Println("Loading .env file...")
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	log.Println(".env file loaded successfully")

	// Get environment variables
	log.Println("Reading environment variables...")
	dbHost := os.Getenv("DB_HOST")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbName := os.Getenv("DB_NAME")
	sevenZPassword := os.Getenv("SEVENZ_PASSWORD")
	updateQuery := os.Getenv("UPDATE_QUERY")
	serviceAccountFile := os.Getenv("SERVICE_ACCOUNT_FILE")

	log.Printf("DB_HOST: %s", dbHost)
	log.Printf("DB_USER: %s", dbUser)
	log.Printf("DB_PASS: %s", strings.Repeat("*", len(dbPass))) // Hide password
	log.Printf("DB_NAME: %s", dbName)
	log.Printf("SEVENZ_PASSWORD: %s", strings.Repeat("*", len(sevenZPassword)))
	// log.Printf("UPDATE_QUERY: %s", updateQuery)
	log.Printf("SERVICE_ACCOUNT_FILE: %s", serviceAccountFile)

	if dbHost == "" || dbName == "" || sevenZPassword == "" || updateQuery == "" || serviceAccountFile == "" {
		log.Fatal("Missing required environment variables")
	}
	log.Println("All required environment variables are set")

	// Authenticate with Google Drive
	log.Println("Authenticating with Google Drive...")
	ctx := context.Background()
	srv, err := drive.NewService(ctx, option.WithCredentialsFile(serviceAccountFile))
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}
	log.Println("Google Drive authentication successful")

	// Get files from folder
	log.Println("Retrieving files from Google Drive...")
	files, err := getFilesFromFolder(srv)
	if err != nil {
		log.Fatalf("Unable to get files: %v", err)
	}
	log.Printf("Found %d files to process", len(files))

	// Process each file
	for i, file := range files {
		log.Printf("Processing file %d/%d: %s (ID: %s)", i+1, len(files), file.Name, file.Id)
		err := processFile(srv, file, dbHost, dbUser, dbPass, dbName, sevenZPassword, updateQuery)
		if err != nil {
			log.Printf("Error processing file %s: %v", file.Name, err)
			continue
		}
		log.Printf("Successfully processed file %s", file.Name)
	}

	log.Println("Backup-otomatis application completed")
}

func getFilesFromFolder(srv *drive.Service) ([]*drive.File, error) {
	query := "trashed = false and mimeType != 'application/vnd.google-apps.folder' and name contains 'Susenas2025M'"
	log.Printf("Executing Drive query: %s", query)
	fileList, err := srv.Files.List().Q(query).Fields("files(id, name, createdTime)").OrderBy("createdTime").Do()
	if err != nil {
		return nil, fmt.Errorf("Drive API error: %v", err)
	}
	log.Printf("Drive API returned %d files", len(fileList.Files))
	return fileList.Files, nil
}

func processFile(srv *drive.Service, file *drive.File, dbHost, dbUser, dbPass, dbName, password, updateQuery string) error {
	log.Printf("Starting processing for file: %s", file.Name)

	// Create temp dir
	log.Println("Creating temporary directory...")
	tempDir, err := os.MkdirTemp("", "backup-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	log.Printf("Temporary directory created: %s", tempDir)

	// Download file
	downloadedFile := filepath.Join(tempDir, file.Name)
	log.Printf("Downloading file to: %s", downloadedFile)
	err = downloadFile(srv, file.Id, downloadedFile)
	if err != nil {
		return fmt.Errorf("failed to download file: %v", err)
	}
	log.Println("File downloaded successfully")

	// Extract 7z
	extractDir := filepath.Join(tempDir, "extracted")
	log.Printf("Extracting 7z archive to: %s", extractDir)
	err = extract7z(downloadedFile, extractDir, password)
	if err != nil {
		return fmt.Errorf("failed to extract 7z: %v", err)
	}
	log.Println("7z extraction completed")

	// Find .bak file
	log.Println("Searching for .bak file...")
	bakFile, err := findBakFile(extractDir)
	if err != nil {
		return fmt.Errorf("failed to find .bak file: %v", err)
	}
	log.Printf("Found .bak file: %s", bakFile)

	// Restore DB
	log.Printf("Restoring database %s from %s", dbName, bakFile)
	err = restoreDB(dbHost, dbUser, dbPass, dbName, bakFile)
	if err != nil {
		return fmt.Errorf("failed to restore database: %v", err)
	}
	log.Println("Database restore completed")

	// Run update query
	// log.Printf("Running update query: %s", updateQuery)
	err = runUpdateQuery(dbHost, dbUser, dbPass, dbName, updateQuery)
	if err != nil {
		return fmt.Errorf("failed to run update query: %v", err)
	}
	log.Println("Update query executed successfully")

	// Delete Drive file
	log.Printf("Deleting file from Google Drive: %s", file.Id)
	// err = srv.Files.Delete(file.Id).Do()
	// if err != nil {
	// 	return fmt.Errorf("failed to delete Drive file: %v", err)
	// }
	log.Println("File deleted from Google Drive")

	log.Printf("Processing completed for file: %s", file.Name)
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
	args := []string{"-S", host, "-d", "master"}
	if user == "" && pass == "" {
		args = append(args, "-E")
	} else {
		args = append(args, "-U", user, "-P", pass)
	}
	query := fmt.Sprintf("RESTORE DATABASE %s FROM DISK = '%s' WITH REPLACE", dbName, bakPath)
	args = append(args, "-Q", query)
	// log.Printf("Running sqlcmd with args: %v", args)
	cmd := exec.Command("sqlcmd", args...)
	output, err := cmd.CombinedOutput()
	log.Printf("sqlcmd output: %s", string(output))
	if err != nil {
		log.Printf("sqlcmd output: %s", string(output))
		return err
	}
	return nil
}

func runUpdateQuery(host, user, pass, dbName, query string) error {
	args := []string{"-S", host, "-d", dbName}
	if user == "" && pass == "" {
		args = append(args, "-E")
	} else {
		args = append(args, "-U", user, "-P", pass)
	}
	args = append(args, "-Q", query)
	// log.Printf("Running sqlcmd with args: %v", args)
	cmd := exec.Command("sqlcmd", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("sqlcmd output: %s", string(output))
		return err
	}
	return nil
}
