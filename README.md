# Backup Otomatis

This Go application automates the process of downloading database backups from Google Drive, extracting them, restoring to SQL Server, running an update query, and cleaning up.

## Prerequisites

- Go 1.21 or later
- 7-Zip installed and available in PATH
- SQL Server instance
- Google Service Account with Drive API access

## Setup

1. Clone or download this project.
2. Copy `.env.example` to `.env` and fill in the required values.
3. Place your Google Service Account JSON file in the project directory or specify the path in `.env`.
4. Run `go mod tidy` to install dependencies.
5. Run `go build` to build the application.

## Usage

Run the compiled binary:

```bash
./backup-otomatis
```

The application will:
1. Connect to Google Drive using the service account.
2. List all files in the specified folder.
3. For each file:
   - Download the 7z archive.
   - Extract it using the provided password.
   - Restore the .bak file to the SQL Server database.
   - Run the specified update query.
   - Delete the local files and the file from Google Drive.

## Configuration

Edit the `.env` file with your settings:

- `DB_HOST`: SQL Server host
- `DB_USER`: Database username
- `DB_PASS`: Database password
- `DB_NAME`: Database name to restore to
- `DRIVE_FOLDER_ID`: Google Drive folder ID containing the backup files
- `SERVICE_ACCOUNT_FILE`: Path to service account JSON file
- `SEVENZ_PASSWORD`: Password for 7z archives
- `UPDATE_QUERY`: SQL query to run after restore

## Notes

- Ensure the service account has read/write access to the Drive folder.
- The application assumes each 7z file contains exactly one .bak file.
- Files are processed in the order returned by Google Drive API.
- Errors in processing one file will not stop the processing of others.