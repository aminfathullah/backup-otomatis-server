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

The application is configured via environment variables in a `.env` file. Below is a comprehensive list of all configuration variables:

| Variable | Description | Required |
|----------|-------------|----------|
| `DB_HOST` | SQL Server host (e.g., `localhost\SQLEXPRESS`) | Yes |
| `DB_USER` | Database username (leave empty for Windows Authentication) | Yes |
| `DB_PASS` | Database password (leave empty for Windows Authentication) | Yes |
| `DB_NAME` | Database name to restore to | Yes |
| `SEVENZ_PASSWORD` | Password for 7z archives | Yes |
| `UPDATE_QUERY` | SQL query to run after restore | Yes |
| `SERVICE_ACCOUNT_FILE` | Path to Google service account JSON file | Yes |
| `SPREADSHEET_ID` | Google Sheets ID for tracking processed files | Yes |
| `SPREADSHEET_TIMEZONE` | Timezone for formatting timestamps in spreadsheet (e.g., `Asia/Jakarta`) | No |

Note: DRIVE_FOLDER_ID is not used; files are queried by name containing 'Susenas2025M'.

## Logging Output

The application provides detailed logging throughout the process:

- Startup and configuration loading
- Google Drive and Sheets authentication
- File discovery and processing status
- Download, extraction, and database operations
- Errors and warnings

Logs are output to stdout/stderr and can be redirected for monitoring.

## Common Error Scenarios

- **Missing environment variables**: Ensure all required variables are set in `.env`.
- **Google API authentication failure**: Verify service account JSON file and permissions.
- **7z extraction failure**: Check password and archive integrity.
- **Database connection issues**: Confirm SQL Server is running and credentials are correct.
- **File not found in Drive**: Ensure files match the query criteria.

## Troubleshooting Steps

1. **Check logs**: Review output for specific error messages.
2. **Verify environment**: Ensure `.env` file is properly configured.
3. **Test connections**: Manually verify Google Drive access and SQL Server connectivity.
4. **Permissions**: Ensure service account has Drive access and SQL Server permissions.
5. **File integrity**: Confirm backup files are not corrupted.

## Notes

- Ensure the service account has read/write access to the Drive folder.
- The application assumes each 7z file contains exactly one .bak file.
- Files are processed in the order returned by Google Drive API.
- Errors in processing one file will not stop the processing of others.