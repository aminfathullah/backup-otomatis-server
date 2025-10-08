// Package main provides an automated backup restoration system.
//
// This application downloads database backup files from Google Drive,
// extracts them, restores to SQL Server, runs update queries, and cleans up.
// It integrates with Google Drive API and Google Sheets for tracking.
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

const (
	minFileSize = 10 * 1024
	// main is the entry point of the backup-otomatis application.
	//
	// It loads environment variables, authenticates with Google services,
	// retrieves files from Drive, processes each file by downloading, extracting,
	// restoring to database, running updates, and cleaning up.
	maxAgeForDeletion = 10 * time.Minute
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
	spreadsheetID := os.Getenv("SPREADSHEET_ID")

	log.Printf("DB_HOST: %s", dbHost)
	log.Printf("DB_USER: %s", dbUser)
	log.Printf("DB_PASS: %s", strings.Repeat("*", len(dbPass))) // Hide password
	log.Printf("DB_NAME: %s", dbName)
	log.Printf("SEVENZ_PASSWORD: %s", strings.Repeat("*", len(sevenZPassword)))
	// log.Printf("UPDATE_QUERY: %s", updateQuery)
	log.Printf("SERVICE_ACCOUNT_FILE: %s", serviceAccountFile)
	log.Printf("SPREADSHEET_ID: %s", spreadsheetID)

	if dbHost == "" || dbName == "" || sevenZPassword == "" || updateQuery == "" || serviceAccountFile == "" || spreadsheetID == "" {
		log.Fatal("Missing required environment variables")
	}
	log.Println("All required environment variables are set")

	// Ensure required external tools are available in PATH before proceeding.
	// This fails fast with a clear message so the operator can fix the environment.
	if _, err := exec.LookPath("7z"); err != nil {
		log.Fatalf("7z not found in PATH: %v. Please install 7-Zip and ensure '7z' is available in PATH.", err)
	}
	if _, err := exec.LookPath("sqlcmd"); err != nil {
		log.Fatalf("sqlcmd not found in PATH: %v. Please install SQL Server Command Line Utilities (sqlcmd) and ensure it's available in PATH.", err)
	}

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
		err := processFile(srv, sheetsSrv, spreadsheetID, file, dbHost, dbUser, dbPass, dbName, sevenZPassword, updateQuery)
		if err != nil {
			log.Printf("Error processing file %s: %v", file.Name, err)
			// getFilesFromFolder retrieves a list of files from the specified Google Drive folder.
			//
			// It queries Google Drive for files that are not trashed, not folders, and contain 'Susenas2025M'
			// in their name. Files are ordered by creation time.
			//
			// Parameters:
			//   - srv: authenticated Google Drive service client.
			//
			// Returns:
			//   - []*drive.File: slice of Google Drive file objects.
			//   - error: any error encountered during the API call.
			// processFile handles the complete processing workflow for a single Google Drive file.
			//
			// It checks file size, downloads and extracts if valid, grants permissions,
			// restores the database, runs update queries, and cleans up by deleting the file
			// and updating the spreadsheet.
			//
			// Parameters:
			//   - srv: Google Drive service client.
			//   - sheetsSrv: Google Sheets service client.
			//   - spreadsheetID: ID of the Google Sheet for tracking.
			//   - file: the Google Drive file to process.
			//   - dbHost: SQL Server host.
			//   - dbUser: database username.
			//   - dbPass: database password.
			//   - dbName: target database name.
			//   - password: 7z archive password.
			//   - updateQuery: SQL query to run after restore.
			//
			// Returns:
			//   - error: any error encountered during processing.
			continue
		}
		log.Printf("Successfully processed file %s", file.Name)
	}

	log.Println("Backup-otomatis application completed")
}

func getFilesFromFolder(srv *drive.Service) ([]*drive.File, error) {
	query := "trashed = false and mimeType != 'application/vnd.google-apps.folder' and name contains 'Susenas2025M'"
	log.Printf("Executing Drive query: %s", query)
	fileList, err := srv.Files.List().Q(query).PageSize(1000).Fields("nextPageToken, files(id, name, createdTime, size, parents)").OrderBy("createdTime").Do()
	if err != nil {
		return nil, fmt.Errorf("Drive API error: %v", err)
	}
	log.Printf("Drive API returned %d files", len(fileList.Files))
	return fileList.Files, nil
}

func processFile(srv *drive.Service, sheetsSrv *sheets.Service, spreadsheetID string, file *drive.File, dbHost, dbUser, dbPass, dbName, password, updateQuery string) error {
	log.Printf("Starting processing for file: %s", file.Name)

	if file.Size < minFileSize {
		return deleteSmallFile(srv, file)
	}

	tempDir, err := createTempDir()
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	bakFile, err := downloadAndExtract(srv, file, tempDir, password)
	// deleteSmallFile deletes a file from Google Drive if it is smaller than the minimum size.
	//
	// Parameters:
	//   - srv: Google Drive service client.
	//   - file: the file to delete.
	//
	// Returns:
	//   - error: any error encountered during deletion.
	if err != nil {
		if shouldDelete(file) {
			// createTempDir creates a temporary directory for file processing.
			//
			// Returns:
			//   - string: path to the created temporary directory.
			//   - error: any error encountered during creation.
			deleteFileAndUpdateSpreadsheet(srv, sheetsSrv, spreadsheetID, file)
		} else {
			log.Printf("File %s is less than 10 minutes old, skipping deletion", file.Name)
		}
		// downloadAndExtract downloads a file from Google Drive and extracts the 7z archive.
		//
		// It downloads the file to a temporary location, extracts it using the provided password,
		// and locates the .bak file within the extracted contents.
		//
		// Parameters:
		//   - srv: Google Drive service client.
		//   - file: the file to download.
		//   - tempDir: temporary directory for operations.
		//   - password: password for 7z extraction.
		//
		// Returns:
		//   - string: path to the extracted .bak file.
		//   - error: any error encountered during download or extraction.
		return err
	}

	grantPermissions(bakFile, dbHost)

	err = restoreDB(dbHost, dbUser, dbPass, dbName, bakFile)
	if err != nil {
		return err
	}

	err = runUpdateQuery(dbHost, dbUser, dbPass, dbName, updateQuery)
	if err != nil {
		// grantPermissions grants SQL Server service permissions on the backup file and its directory.
		//
		// It determines the appropriate service account based on the database host and uses icacls
		// to grant full control permissions.
		//
		// Parameters:
		//   - bakFile: path to the .bak file.
		//   - dbHost: SQL Server host, used to determine the service account.
		return err
	}

	// shouldDelete determines if a file should be deleted based on its age.
	//
	// Files older than maxAgeForDeletion (10 minutes) are eligible for deletion.
	//
	// Parameters:
	//   - file: the Google Drive file to check.
	//
	// Returns:
	//   - bool: true if the file should be deleted, false otherwise.
	// formatCreatedTime formats the file creation time according to the configured timezone.
	//
	// If SPREADSHEET_TIMEZONE is set, it uses that timezone; otherwise, uses local time.
	// Falls back to the original string if parsing fails.
	//
	// Parameters:
	//   - createdTimeStr: RFC3339 formatted creation time string.
	//
	// Returns:
	//   - string: formatted time string in "1/2/2006 15:04:05" format.
	err = deleteFileAndUpdateSpreadsheet(srv, sheetsSrv, spreadsheetID, file)
	if err != nil {
		return err
	}

	log.Printf("Processing completed for file: %s", file.Name)
	return nil
}
func deleteSmallFile(srv *drive.Service, file *drive.File) error {
	log.Printf("File %s is smaller than 10KB (%d bytes), deleting from Drive", file.Name, file.Size)
	err := srv.Files.Delete(file.Id).Do()
	// deleteFileAndUpdateSpreadsheet deletes a file from Google Drive and updates the tracking spreadsheet.
	//
	// It retrieves the parent folder name, formats the creation time, and either updates an existing
	// row in the spreadsheet or appends a new one.
	//
	// Parameters:
	//   - srv: Google Drive service client.
	//   - sheetsSrv: Google Sheets service client.
	//   - spreadsheetID: ID of the Google Sheet.
	//   - file: the file being processed.
	//
	// Returns:
	//   - error: any error encountered during deletion or spreadsheet update.
	if err != nil {
		return fmt.Errorf("failed to delete small file: %v", err)
	}
	log.Println("Small file deleted from Google Drive")
	return nil
}

func createTempDir() (string, error) {
	tempDir, err := os.MkdirTemp("", "backup-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %v", err)
	}
	log.Printf("Temporary directory created: %s", tempDir)
	return tempDir, nil
}

func downloadAndExtract(srv *drive.Service, file *drive.File, tempDir, password string) (string, error) {
	downloadedFile := filepath.Join(tempDir, file.Name)
	log.Printf("Downloading file to: %s", downloadedFile)
	err := downloadFile(srv, file.Id, downloadedFile)
	// downloadFile downloads a file from Google Drive to the specified destination path.
	//
	// Parameters:
	//   - srv: Google Drive service client.
	//   - fileID: ID of the file to download.
	//   - destPath: local path where the file will be saved.
	//
	// Returns:
	//   - error: any error encountered during download.
	if err != nil {
		return "", fmt.Errorf("failed to download file: %v", err)
	}
	log.Println("File downloaded successfully")

	extractDir := filepath.Join(tempDir, "extracted")
	log.Printf("Extracting 7z archive to: %s", extractDir)
	err = extract7z(downloadedFile, extractDir, password)
	// extract7z extracts a 7z archive to the specified directory using the provided password.
	//
	// Parameters:
	//   - archivePath: path to the 7z archive file.
	//   - destDir: destination directory for extraction.
	// findBakFile searches for a .bak file within the specified directory.
	//
	// It recursively walks the directory and returns the path of the first .bak file found.
	//
	// Parameters:
	//   - dir: directory to search in.
	//
	// Returns:
	//   - string: path to the .bak file.
	//   - error: error if no .bak file is found or if walking fails.
	//   - password: password for the archive.
	//
	// Returns:
	//   - error: any error encountered during extraction.
	if err != nil {
		return "", fmt.Errorf("failed to extract 7z: %v", err)
	}
	log.Println("7z extraction completed")

	log.Println("Searching for .bak file...")
	// restoreDB restores a SQL Server database from a .bak file.
	//
	// It performs a full restore with move operations, setting the database to single-user mode
	// during the process and back to multi-user afterward. It detects logical file names and
	// uses the instance's default data path.
	//
	// Parameters:
	//   - host: SQL Server host.
	//   - user: database username (empty for Windows auth).
	//   - pass: database password (empty for Windows auth).
	//   - dbName: name of the database to restore.
	//   - bakPath: path to the .bak file.
	//
	// Returns:
	//   - error: any error encountered during the restore process.
	bakFile, err := findBakFile(extractDir)
	if err != nil {
		return "", fmt.Errorf("failed to find .bak file: %v", err)
	}
	log.Printf("Found .bak file: %s", bakFile)
	return bakFile, nil
}

func grantPermissions(bakFile, dbHost string) {
	log.Println("Granting permissions to SQL Server service on bak file and folder...")
	serviceAcct := "NT SERVICE\\MSSQLSERVER"
	if strings.Contains(dbHost, "\\") {
		parts := strings.SplitN(dbHost, "\\", 2)
		instance := parts[1]
		serviceAcct = "NT SERVICE\\MSSQL$" + instance
	}
	cmd := exec.Command("icacls", bakFile, "/grant", serviceAcct+":F")
	err := cmd.Run()
	if err != nil {
		log.Printf("Failed to grant permissions on bak file: %v", err)
	}
	extractFolder := filepath.Dir(bakFile)
	cmd = exec.Command("icacls", extractFolder, "/grant", serviceAcct+":F", "/T")
	err = cmd.Run()
	if err != nil {
		log.Printf("Failed to grant permissions on extract folder: %v", err)
	}
}

func shouldDelete(file *drive.File) bool {
	createdTime, err := time.Parse(time.RFC3339, file.CreatedTime)
	if err != nil {
		log.Printf("Error parsing created time for %s: %v, proceeding without deletion", file.Name, err)
		return false
	}
	return time.Since(createdTime) >= maxAgeForDeletion
}

func formatCreatedTime(createdTimeStr string) string {
	t, err := time.Parse(time.RFC3339, createdTimeStr)
	if err != nil {
		return createdTimeStr
	}
	tz := os.Getenv("SPREADSHEET_TIMEZONE")
	var loc *time.Location
	if tz == "" || strings.EqualFold(tz, "Local") {
		loc = time.Local
	} else {
		l, lerr := time.LoadLocation(tz)
		if lerr != nil {
			log.Printf("warning: unable to load timezone %s: %v, using Local", tz, lerr)
			loc = time.Local
		} else {
			loc = l
		}
	}
	return t.In(loc).Format("1/2/2006 15:04:05")
}

func deleteFileAndUpdateSpreadsheet(srv *drive.Service, sheetsSrv *sheets.Service, spreadsheetID string, file *drive.File) error {
	log.Printf("Deleting file from Google Drive: %s", file.Id)
	err := srv.Files.Delete(file.Id).Do()
	if err != nil {
		return fmt.Errorf("failed to delete Drive file: %v", err)
	}
	log.Println("File deleted from Google Drive")

	parentName, pErr := getParentFolderName(srv, file)
	log.Printf("Parent folder name: %s", parentName)
	if pErr != nil {
		log.Printf("Warning: failed to get parent folder name: %v", pErr)
	} else {
		createdStr := formatCreatedTime(file.CreatedTime)
		if uErr := upsertSpreadsheetRow(sheetsSrv, spreadsheetID, parentName, createdStr); uErr != nil {
			log.Printf("Warning: failed to update spreadsheet: %v", uErr)
		} else {
			log.Printf("Spreadsheet updated for Kab=%s with Susenas=%s", parentName, createdStr)
		}
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
	// runUpdateQuery executes a SQL query on the specified database.
	//
	// Parameters:
	//   - host: SQL Server host.
	//   - user: database username (empty for Windows auth).
	//   - pass: database password (empty for Windows auth).
	//   - dbName: target database name.
	//   - query: SQL query to execute.
	//
	// Returns:
	//   - error: any error encountered during query execution.
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

// GetParentFolderName returns the name of the first parent folder for the file.
//
// It attempts to retrieve the parent folder name using the file's parents field.
// Falls back to querying the Drive API if necessary.
//
// Parameters:
//   - srv: Google Drive service client.
//   - file: the Google Drive file.
//
// Returns:
//   - string: name of the parent folder, or empty string if not found.
//   - error: any error encountered during the API calls.
func getParentFolderName(srv *drive.Service, file *drive.File) (string, error) {
	if len(file.Parents) > 0 {
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
	if len(fi.Parents) > 0 {
		p, err := srv.Files.Get(fi.Parents[0]).Fields("name").Do()
		if err != nil {
			return "", err
		}
		return p.Name, nil
	}
	return "", nil
}

// UpsertSpreadsheetRow finds or creates a row in the spreadsheet for the given kab and createdTime.
//
// It searches for an existing row where column A matches the kab value.
// If found, it updates column B with the createdTime. If not found, it appends a new row.
//
// Parameters:
//   - srv: Google Sheets service client.
//   - spreadsheetID: ID of the Google Sheet.
//   - kab: value for column A (e.g., parent folder name).
//   - createdTime: formatted time string for column B.
//
// Returns:
//   - error: any error encountered during read, update, or append operations.
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
		_, err = srv.Spreadsheets.Values.Update(spreadsheetID, a1, vr).ValueInputOption("USER_ENTERED").Do()
		if err != nil {
			return fmt.Errorf("failed to update spreadsheet cell %s: %v", a1, err)
		}
		return nil
	}

	// Append new row
	vr := &sheets.ValueRange{
		Values: [][]interface{}{{kab, createdTime}},
	}
	_, err = srv.Spreadsheets.Values.Append(spreadsheetID, "A:B", vr).ValueInputOption("USER_ENTERED").InsertDataOption("INSERT_ROWS").Do()
	if err != nil {
		return fmt.Errorf("failed to append row to spreadsheet: %v", err)
	}
	return nil
}
