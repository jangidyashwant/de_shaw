# DE SHAW Case Studies
# Web Scraper Case Studies

This repository contains three web scrapers designed to extract and store data including **PDF,text,and xlsx** files from different sources with varying complexities and anti-bot mechanisms.

---

## 📁 Case Study 1: `infopost_scrapper`

### Overview:
This script handles a session-based download process that requires tokens for navigation between pages.

### Key Features:
- Loads configuration from `config.yaml`
- Authenticates and fetches `view_state` and `event_validation_id` required for second-page access
- Iterates through a list of downloadable files
- Resets session after every 3 downloads to handle session expiry
- Files are saved in the `FILES/infopost` directory

### Stats:
- 🔄 Total Requests: **43**
- 📄 Files Downloaded: **25**

---

## 📁 Case Study 2: `pjmeis_scrapper`

### Overview:
This script scrapes tabular data by configuring pagination parameters and extracts it efficiently.

### Key Features:
- Loads configuration from `config.yaml`
- Retrieves `callable_state` for the session
- Sets page size to 200 using a dedicated function
- Iterates over pages and stores the parsed data in CSV format
- Output saved in the `DATA/pjm-eis.com` directory

### Stats:
- 🔄 Total Requests: **3**
- 📄 Output: CSV file with structured data

---

## 📁 Case Study 3: `ferc_scrapper`

### Overview:
This scraper fetches regulatory form metadata and logs checksums for validation purposes.

### Key Features:
- Loads configuration from `config.yaml`
- Retrieves `view_state` and `event_validation_id` from the base page
- Navigates to the Form 1 Viewer
- Parses SHA-256 checksum from the response
- Output saved in the `DATA/ferc.gov` directory

### Stats:
- 🔄 Total Requests: **2**
- 🧾 Output: Text file containing SHA-256 checksum

---

## ⚙️ Requirements

- Python 3.x
- Dependencies listed in `requirements.txt` (e.g., `requests`, `yaml`, `logging`)

---

## 📌 Notes

- All scripts implement session handling and error recovery.
- File paths and session timeouts are configurable via `config.yaml`.
- Ideal for use in data pipelines and regulatory data aggregation.

---

## 📂 Directory Structure

