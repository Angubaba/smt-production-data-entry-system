# SMT Production Data Entry System

A desktop application built for the SMT (Surface Mount Technology) department to manage rework registers and quality sheet data — replacing manual Excel-based tracking with a structured, searchable database system.

---

## Features

### Rework Register
- **Session-based data entry** — group entries into named sessions (e.g. by date or batch)
- **Flexible columns** — add, remove, rename, reorder columns via Manage Columns dialog; all changes persist to `columns.json`
- **Date-wise sorting** — entries auto-sort by date with configurable format (DD.MM.YY, YYYY-MM-DD, etc.)
- **Model filter** — click the ▼ arrow on the Model column heading to filter rows by model
- **Duplicate detection** — warns before adding an identical row
- **Row management** — reorder rows, insert rows between existing entries
- **Find & Replace** — search and replace values across the current session
- **Undo last entry**
- **Import from Excel** — maps Excel headers to columns; prompts to clear or append to current session
- **Export to Excel** — formatted export with chart data sheet including:
  - Daily qty / defects / DPMO per model
  - Top 5 defect → component breakdown
  - Top 5 component → defect breakdown
- **History & Search** — search across all sessions by date range, text, model, or combo filters
- **Open previous session as current** — columns automatically sync to the loaded session's schema
- **Rename / delete sessions**

### Quality Sheet
- **Monthly entry** — track production data per model per day per line
- **DPMO auto-calculation** — based on qty produced, defects, and solder joint count
- **Model search** — filter the 80+ model list by name
- **In-app graphs** — dual-axis line chart (Qty + DPMO) with F11 fullscreen toggle
- **Multi-month Excel export** — all models, all months, formatted with borders and conditional highlighting
- **Import from Excel** — load existing quality sheet data
- **History & Search** — filter by date range and model across all months

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| UI | Python 3.11 + Tkinter / ttk |
| Database | SQLite (via `sqlite3`) |
| Charts | Matplotlib |
| Excel I/O | openpyxl |
| Packaging | PyInstaller (`--onedir`) |

---

## Project Structure

```
smt_data_entry.py     # Entire application (single-file)
columns.json          # Rework register column configuration
smt_rework.db         # Rework entries database (not tracked)
smt_quality.db        # Quality sheet database (not tracked)
```

---

## Running the App

**From source:**
```bash
pip install matplotlib openpyxl
python smt_data_entry.py
```

**Packaged (standalone folder):**
```
dist_new/SMT_App/SMT_App.exe
```
Copy the entire `SMT_App/` folder to any PC — no Python installation required.

**Build from source:**
```bash
pip install pyinstaller openpyxl matplotlib
python -m PyInstaller -y --onedir --windowed --name "SMT_App" \
  --add-data "columns.json;." --collect-all openpyxl smt_data_entry.py
```

---

## Column Configuration

Columns are defined in `columns.json` as a list of objects:

```json
{
  "key": "pcba_no",
  "display": "PCBA NO.",
  "type": "entry",
  "options": [],
  "required": true,
  "default": ""
}
```

| Field | Description |
|-------|-------------|
| `key` | Internal identifier used in the database |
| `display` | Column header shown in the UI and Excel exports |
| `type` | `"entry"` (free text) or `"combo"` (dropdown) |
| `options` | Dropdown options for combo type |
| `required` | Shows ★ marker; blocks submission if empty |
| `default` | Pre-filled value when form loads |

---

## Database Schema

```sql
-- Rework (smt_rework.db)
sessions(session_id TEXT, label TEXT, created_at TEXT)
entries(id INTEGER, session_id TEXT, data TEXT, created_at TEXT)
-- data column stores entry as JSON: {"pcba_no": "12345", "fault": "SHIFT", ...}

-- Quality Sheet (smt_quality.db)
qs_months(id, year, month, created_at)
qs_models(id, month_id, sr_no, model, solder_joints, dpmo_threshold)
qs_entries(id, model_id, day, line, qty_produced, defects, dpmo)
```

---

## Notes

- Databases are excluded from version control (contain production data)
- The app auto-migrates old fixed-column databases to the JSON schema on first run
- Built during internship at Deltron Electronics, SMT Department
