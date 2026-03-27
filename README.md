# imapcrawler
a python tool to crawl an imap inbox and extract emails as structured records for other tools

# IMAP Email Inbox Crawler

A Python tool for crawling emails from an IMAP server, fetching raw email data, and cleaning/parsing the content for later analysis or use in applications like knowledge bases.

## Features

- Fetch emails from IMAP servers (including Gmail, Outlook, etc.)
- Support for querying by date ranges, months, or specific days
- Raw email extraction with metadata
- Email content cleaning and parsing (HTML to text, quote removal, etc.)
- Configurable file handling modes (merge, overwrite, raise)
- Persistent configuration storage
- Command-line interface for easy automation

## Installation

To install this package in development mode, run:
```bash
pip install imapcrawler
```

The package has the following optional dependencies which will improve quality of text extraction:
- `tqdm`
- `beautifulsoup4` 
- `python-dateutil`
- `mail-parser-reply`

the package can be installed with all optional dependencies like this

```bash
pip install imapcrawler[all]
```


## Usage

**NOTE**: if installed via pip you can either use `python imapcrawler.py ` or just `imapcrawler`.

### Basic Commands

1. **Set up configuration**:
   ```bash
   python imapcrawler.py config-set --server imap.example.com --email user@example.com
   ```

2. **Download raw emails**:
   ```bash
   python imapcrawler.py download --month 2023-06 --limit 100
   ```

3. **Clean downloaded emails**:
   ```bash
   python imapcrawler.py clean
   ```

### Command Reference

#### `config-set`
Set persistent configuration values interactively or via arguments.

#### `config-show`
Display current configuration.

#### `config-clear`
Clear all configuration values.

#### `config-default`
Reset configuration to factory defaults.

#### `download`
Fetch raw emails from IMAP server.

Options:
- `--date` - Specific date (YYYY-MM-DD)
- `--month` - Month to query (YYYY-MM)
- `--limit` - Limit number of emails (-1 for all)
- `--diff` - Skip already known emails
- `--filepath_raw` - Output file for raw emails
- `--filepath_clean` - Output file for cleaned emails

#### `clean`
Process raw emails and save cleaned version.

#### `peek-raw` / `peek-clean`
Show a random email from raw or cleaned files.

## Configuration File

Configuration is stored in `~/.imapcrawler_config.json` and includes:
- Server address
- Email address
- File paths for raw and cleaned emails
- File handling mode

## Output Files

- `emails_raw.jsonl`: Raw email data with full metadata
- `emails.jsonl`: Cleaned email data with processed content

## Example Workflow

```bash
# Configure once
python imapcrawler.py config-set --server imap.gmail.com --email user@gmail.com

# Download emails from June 2023
python imapcrawler.py download --month 2023-06 --limit 500

# Clean the downloaded emails
python imapcrawler.py clean

# View a sample
python imapcrawler.py peek-clean
```

## Notes

- Passwords are prompted securely when not provided via command line
- The tool handles large email volumes efficiently with progress bars
- Supports various IMAP servers including Gmail, Outlook, and custom servers
- Email cleaning removes HTML tags, quoted text, and signature blocks