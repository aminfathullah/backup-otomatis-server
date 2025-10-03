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
	"time"

	"github.com/joho/godotenv"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
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

	// Authenticate with Google Drive and Sheets
	log.Println("Authenticating with Google Drive and Sheets...")
	ctx := context.Background()
	srv, err := drive.NewService(ctx, option.WithCredentialsFile(serviceAccountFile))
	if err != nil {
		log.Fatalf("Unable to retrieve Drive client: %v", err)
	}
	sheetsSrv, err := sheets.NewService(ctx, option.WithCredentialsFile(serviceAccountFile))
	if err != nil {
		log.Fatalf("Unable to retrieve Sheets client: %v", err)
	}
	log.Println("Google Drive and Sheets authentication successful")

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
		spreadsheetID := os.Getenv("SPREADSHEET_ID")
		if spreadsheetID == "" {
			log.Fatal("Missing SPREADSHEET_ID environment variable")
		}
		err := processFile(srv, sheetsSrv, spreadsheetID, file, dbHost, dbUser, dbPass, dbName, sevenZPassword, updateQuery)
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
	fileList, err := srv.Files.List().Q(query).Fields("files(id, name, createdTime, size, parents)").OrderBy("createdTime").Do()
	if err != nil {
		return nil, fmt.Errorf("Drive API error: %v", err)
	}
	log.Printf("Drive API returned %d files", len(fileList.Files))
	return fileList.Files, nil
}

func processFile(srv *drive.Service, sheetsSrv *sheets.Service, spreadsheetID string, file *drive.File, dbHost, dbUser, dbPass, dbName, password, updateQuery string) error {
	log.Printf("Starting processing for file: %s", file.Name)

	// Check file size
	if file.Size < 10*1024 {
		log.Printf("File %s is smaller than 10KB (%d bytes), deleting from Drive", file.Name, file.Size)
		// err := srv.Files.Delete(file.Id).Do()
		// if err != nil {
		// 	return fmt.Errorf("failed to delete small file: %v", err)
		// }
		log.Println("Small file deleted from Google Drive")
		return nil
	}

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
		if createdTime, err := time.Parse(time.RFC3339, file.CreatedTime); err != nil {
			log.Printf("Error parsing created time for %s: %v, proceeding with deletion", file.Name, err)
		} else if time.Since(createdTime) >= 10*time.Minute {
			// Delete Drive file
			log.Printf("Deleting file from Google Drive: %s", file.Id)
			// err = srv.Files.Delete(file.Id).Do()
			// if err != nil {
			// 	return fmt.Errorf("failed to delete Drive file: %v", err)
			// }
			log.Println("File deleted from Google Drive")

			// Update spreadsheet: find parent folder name and write created time to Susenas column (B) for the row matching Kab (A)
			parentName, pErr := getParentFolderName(srv, file)
			log.Printf("Parent folder name: %s", parentName)
			if pErr != nil {
				log.Printf("Warning: failed to get parent folder name: %v", pErr)
			} else {
				createdTime := file.CreatedTime
				if uErr := upsertSpreadsheetRow(sheetsSrv, spreadsheetID, parentName, createdTime); uErr != nil {
					log.Printf("Warning: failed to update spreadsheet: %v", uErr)
				} else {
					log.Printf("Spreadsheet updated for Kab=%s with Susenas=%s", parentName, createdTime)
				}
			}
		} else {
			log.Printf("File %s is less than 10 minutes old, skipping deletion", file.Name)
		}
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

	// Grant permissions to SQL Server service on the bak file and extracted folder
	log.Println("Granting permissions to SQL Server service on bak file and folder...")
	// derive instance name from host if present (e.g., SERVER\INSTANCE)
	serviceAcct := "NT SERVICE\\MSSQLSERVER"
	if strings.Contains(dbHost, "\\") {
		parts := strings.SplitN(dbHost, "\\", 2)
		instance := parts[1]
		serviceAcct = "NT SERVICE\\MSSQL$" + instance
	}
	// grant on the bak file
	cmd := exec.Command("icacls", bakFile, "/grant", serviceAcct+":F")
	err = cmd.Run()
	if err != nil {
		log.Printf("Failed to grant permissions on bak file: %v", err)
		// Continue anyway
	}
	// also grant on the extract folder (recursive)
	extractFolder := filepath.Dir(bakFile)
	cmd = exec.Command("icacls", extractFolder, "/grant", serviceAcct+":F", "/T")
	err = cmd.Run()
	if err != nil {
		log.Printf("Failed to grant permissions on extract folder: %v", err)
	}

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

	log.Printf("Deleting file from Google Drive: %s", file.Id)
	// err = srv.Files.Delete(file.Id).Do()
	// if err != nil {
	// 	return fmt.Errorf("failed to delete Drive file: %v", err)
	// }
	log.Println("File deleted from Google Drive")

	// Update spreadsheet: find parent folder name and write created time to Susenas column (B) for the row matching Kab (A)
	parentName, pErr := getParentFolderName(srv, file)
	log.Printf("Parent folder name: %s", parentName)
	if pErr != nil {
		log.Printf("Warning: failed to get parent folder name: %v", pErr)
	} else {
		createdTime := file.CreatedTime
		if uErr := upsertSpreadsheetRow(sheetsSrv, spreadsheetID, parentName, createdTime); uErr != nil {
			log.Printf("Warning: failed to update spreadsheet: %v", uErr)
		} else {
			log.Printf("Spreadsheet updated for Kab=%s with Susenas=%s", parentName, createdTime)
		}
	}

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

	// First, get logical file names from the backup using RESTORE FILELISTONLY
	argsList := append(args, "-h", "-1", "-W", "-s", "|", "-Q", fmt.Sprintf("SET NOCOUNT ON; RESTORE FILELISTONLY FROM DISK='%s'", bakPath))
	cmd := exec.Command("sqlcmd", argsList...)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to run RESTORE FILELISTONLY: %v", err)
	}
	listOut := strings.TrimSpace(string(out))
	var dataLogical, logLogical string
	if listOut != "" {
		lines := strings.Split(listOut, "\n")
		for _, l := range lines {
			l = strings.TrimSpace(l)
			if l == "" {
				continue
			}
			cols := strings.Split(l, "|")
			for i := range cols {
				cols[i] = strings.TrimSpace(cols[i])
			}
			if len(cols) < 3 {
				continue
			}
			typ := strings.ToUpper(cols[2])
			if strings.HasPrefix(typ, "L") {
				logLogical = cols[0]
			} else {
				// treat as data
				dataLogical = cols[0]
			}
		}
	}

	// Next, query the instance default data path. Use SET NOCOUNT ON and suppress headers/rowcounts.
	argsPath := append(args, "-h", "-1", "-W", "-Q", "SET NOCOUNT ON; SELECT SERVERPROPERTY('InstanceDefaultDataPath')")
	cmd = exec.Command("sqlcmd", argsPath...)
	out, err = cmd.Output()
	if err != nil {
		// If we can't get the instance path, fall back to the backup's directory
		log.Printf("warning: failed to get instance data path: %v", err)
	}
	dataPath := strings.TrimSpace(string(out))
	// sqlcmd may return the literal "NULL" when the property is not set.
	if dataPath == "" || strings.EqualFold(dataPath, "NULL") {
		// fallback to directory of the .bak file
		dataPath = filepath.Dir(bakPath)
		log.Printf("Data path empty or NULL, falling back to bak directory: %s", dataPath)
	} else {
		log.Printf("Data path: %s", dataPath)
	}

	// Use detected logical names or sensible defaults
	if dataLogical == "" {
		dataLogical = dbName
	}
	if logLogical == "" {
		logLogical = dbName + "_log"
	}

	// Set database to single user mode to close all other connections
	log.Println("Setting database to single user mode...")
	query := fmt.Sprintf("ALTER DATABASE %s SET SINGLE_USER WITH ROLLBACK IMMEDIATE;", dbName)
	argsSingle := append(args, "-Q", query)
	cmd = exec.Command("sqlcmd", argsSingle...)
	output, err := cmd.CombinedOutput()
	log.Printf("sqlcmd output: %s", string(output))
	if err != nil {
		return fmt.Errorf("failed to set database to single user: %v", err)
	}
	log.Println("Database set to single user mode")

	// Build RESTORE ... WITH MOVE statement
	mdfTarget := filepath.Join(dataPath, dbName+".mdf")
	ldfTarget := filepath.Join(dataPath, dbName+"_log.ldf")
	query = fmt.Sprintf("RESTORE DATABASE %s FROM DISK='%s' WITH REPLACE, MOVE '%s' TO '%s', MOVE '%s' TO '%s'", dbName, bakPath, dataLogical, mdfTarget, logLogical, ldfTarget)
	argsRestore := append(args, "-Q", query)
	cmd = exec.Command("sqlcmd", argsRestore...)
	output, err = cmd.CombinedOutput()
	log.Printf("sqlcmd output: %s", string(output))
	if err != nil {
		log.Printf("sqlcmd output: %s", string(output))
		return fmt.Errorf("restore failed: %v", err)
	}
	log.Println("Database restore completed")

	// Set database back to multi user mode
	log.Println("Setting database back to multi user mode...")
	query = fmt.Sprintf("ALTER DATABASE %s SET MULTI_USER;", dbName)
	argsMulti := append(args, "-Q", query)
	cmd = exec.Command("sqlcmd", argsMulti...)
	output, err = cmd.CombinedOutput()
	log.Printf("sqlcmd output: %s", string(output))
	if err != nil {
		log.Printf("Warning: failed to set database back to multi user: %v", err)
		// Don't return error, as restore succeeded
	} else {
		log.Println("Database set back to multi user mode")
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

// getParentFolderName returns the name of the first parent folder for the file (or empty string)
func getParentFolderName(srv *drive.Service, file *drive.File) (string, error) {
	if file.Parents != nil && len(file.Parents) > 0 {
		parentID := file.Parents[0]
		f, err := srv.Files.Get(parentID).Fields("id, name").Do()
		if err != nil {
			return "", err
		}
		return f.Name, nil
	}
	// fallback: try to retrieve parents via drive API
	fi, err := srv.Files.Get(file.Id).Fields("parents").Do()
	if err != nil {
		return "", err
	}
	if fi.Parents != nil && len(fi.Parents) > 0 {
		p, err := srv.Files.Get(fi.Parents[0]).Fields("name").Do()
		if err != nil {
			return "", err
		}
		return p.Name, nil
	}
	return "", nil
}

// upsertSpreadsheetRow finds a row where column A == kab and sets column B to createdTime (RFC3339 string).
// If no row matches, it appends a new row [kab, createdTime].
func upsertSpreadsheetRow(srv *sheets.Service, spreadsheetID, kab, createdTime string) error {
	// Read the sheet values (assume sheet1, columns A:B)
	readRange := "A:B"
	resp, err := srv.Spreadsheets.Values.Get(spreadsheetID, readRange).Do()
	if err != nil {
		return fmt.Errorf("failed to read spreadsheet: %v", err)
	}
	log.Printf("Spreadsheet returned %d rows", len(resp.Values))

	// Search for kab in column A
	rowIndex := -1
	if resp.Values != nil {
		for i, row := range resp.Values {
			if len(row) > 0 {
				if s, ok := row[0].(string); ok && strings.TrimSpace(s) == strings.TrimSpace(kab) {
					rowIndex = i // 0-based index in resp.Values
					break
				}
			}
		}
	}

	if rowIndex >= 0 {
		// Update cell in column B at rowIndex+1 (Sheets rows are 1-based)
		a1 := fmt.Sprintf("B%d", rowIndex+1)
		vr := &sheets.ValueRange{
			Range:  a1,
			Values: [][]interface{}{{createdTime}},
		}
		_, err = srv.Spreadsheets.Values.Update(spreadsheetID, a1, vr).ValueInputOption("RAW").Do()
		if err != nil {
			return fmt.Errorf("failed to update spreadsheet cell %s: %v", a1, err)
		}
		return nil
	}

	// Append new row
	vr := &sheets.ValueRange{
		Values: [][]interface{}{{kab, createdTime}},
	}
	_, err = srv.Spreadsheets.Values.Append(spreadsheetID, "A:B", vr).ValueInputOption("RAW").InsertDataOption("INSERT_ROWS").Do()
	if err != nil {
		return fmt.Errorf("failed to append row to spreadsheet: %v", err)
	}
	return nil
}
