# MiniFeed

An RSS ingest server for LLM agents. MiniFeed continuously fetches RSS feeds, extracts full page text via trafilatura, and stores everything in per-domain SQLite databases with FTS5 full-text search. A REST API and browser UI let you manage feeds, search content, and control how far back entries are kept.

![MiniFeed Web UI](progress/2026-04-06-WebUI.jpeg)

---

## Quickstart

### Prerequisites

- Python 3.11 or newer
- pip

### 1. Clone and enter the directory

```sh
git clone https://github.com/your-org/MiniFeed.git
cd MiniFeed
```

### 2. Create a virtual environment

```sh
python -m venv .venv
```

Activate it:

| Platform | Command |
|---|---|
| Windows (PowerShell) | `.venv\Scripts\Activate.ps1` |
| Windows (CMD) | `.venv\Scripts\activate.bat` |
| macOS / Linux | `source .venv/bin/activate` |

### 3. Install dependencies

```sh
pip install -r requirements.txt
```

---

## Starting the server

```sh
python main.py
```

The server starts on **http://localhost:8100** by default (configurable in `config/`).

On startup, the scheduler respects the last-fetched timestamp for every feed — feeds that were fetched recently are skipped until they are next due, so a restart never causes an unnecessary flood of re-ingests.

Press `Ctrl+C` to stop.

---

## Browser UI

| URL | Purpose |
|---|---|
| `http://localhost:8100/` | Home — manage domains, search across all feeds |
| `http://localhost:8100/web/{domain}` | Domain view — feeds, entries, management |
| `http://localhost:8100/web/{domain}/{id}` | Full entry including extracted page text |
| `http://localhost:8100/web/search?q=…` | Full-text search (optionally scoped to a domain) |
| `http://localhost:8100/api/docs` | Interactive Swagger API documentation |

### Domain view features

#### Feeds panel

| Column | Description |
|---|---|
| Name | Feed label |
| Rate | Update interval in minutes — click to edit inline |
| Entries | Current live entry count for that feed |
| Next | Minutes until the next scheduled poll |
| ↻ | Trigger an immediate re-ingest (bypasses the rate gate) |
| ✕ | Remove the feed |

The page self-refreshes every 30 seconds (DOM swap only — no scroll jump).

#### Manage Entries panel

Choose an **Entry Age Category** from the dropdown to control which entries are accepted and kept:

| Mode | Behaviour |
|---|---|
| **No limit** | All entries are stored regardless of publication date |
| **Days Previous** | Rolling window — skip (and optionally purge) entries older than N days. Good for fast-moving feeds like AI news where you only care about the last 30 days |
| **Calendar Period** | Fixed date range — only accept entries within a start/end date. Good for building reference archives like "World news 2025" |

Both modes apply as an **ingest gate** (new entries outside the window are silently skipped) and provide a **purge button** to remove existing entries that fall outside the configured range.

Entries can also be bulk-deleted by feed.

---

## REST API

Full interactive reference at `http://localhost:8100/api/docs`.

### Feeds

```http
GET    /api/feeds                        List all configured feeds
POST   /api/feeds                        Add a new feed
DELETE /api/feeds/{feed_id}              Remove a feed
PATCH  /api/feeds/{feed_id}/rate         Update a feed's polling interval (minutes)
```

**Add a feed**

```sh
curl -X POST http://localhost:8100/api/feeds \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "tech",
    "name": "TechCrunch",
    "url": "https://techcrunch.com/feed/",
    "update_rate": 30
  }'
```

**Change polling interval**

```sh
curl -X PATCH "http://localhost:8100/api/feeds/<feed_id>/rate?minutes=60"
```

### Domains

```http
GET    /api/domains                      List domains with entry counts
POST   /api/domains                      Create a new empty domain
DELETE /api/domains/{domain}             Delete a domain and all its data
POST   /api/domains/{domain}/rename      Rename a domain
```

### Content

```http
GET /api/domains/{domain}/entries                   Paginated entries (limit, offset)
GET /api/domains/{domain}/entries/{entry_id}        Single entry with full content
DELETE /api/domains/{domain}/entries/{entry_id}     Delete a single entry
DELETE /api/domains/{domain}/entries                Bulk-delete by feed_name or older_than_days
POST /api/domains/{domain}/entries/bulk-delete      Delete a list of entry IDs
```

**Get entries for a domain**

```sh
curl "http://localhost:8100/api/domains/tech/entries?limit=50&offset=0"
```

### Search

```http
GET /api/search?q={query}                      Search all domains (FTS5/BM25)
GET /api/search?q={query}&domain={domain}      Search within one domain
GET /api/search?q={query}&full=true            Include full page text in results
```

```sh
curl "http://localhost:8100/api/search?q=artificial+intelligence&domain=tech"
```

Search uses SQLite FTS5 with a `unicode61` tokenizer and BM25 ranking — results match whole words only, not substrings.

### Recent entries

```http
GET /api/recent                            Entries ingested in the last 24 h (all domains)
GET /api/recent?domain={domain}&hours=48   Scoped by domain or time window
```

---

## How ingestion works

1. **APScheduler** runs an interval job for each feed at its configured rate.
2. The job calls `_enqueue()` which reloads the feed from disk and checks `last_fetched_at`. If the feed was fetched recently (within its rate window), it is skipped with a log message.
3. A single background worker thread drains the queue one feed at a time via `feedparser`.
4. Each entry's publication date is checked against the domain's **Entry Age Category** gate — out-of-range entries are discarded before any HTTP request is made for the page.
5. Full page text is extracted with **trafilatura** and stored alongside the headline, URL, metadata, and published date.
6. Entries are deduplicated by URL (`INSERT OR IGNORE`).
7. On success, `last_fetched_at` is written to the feed JSON file — this gate survives server restarts.

---

## Data storage

| Path | Contents |
|---|---|
| `feeds/{domain}.json` | Feed inventory per domain (auto-created) |
| `data/{domain}.db` | SQLite database for each domain (auto-created) |
| `config/` | Server configuration (host, port, data/feeds dirs) |

Each domain database contains:

- `entries` table — id, feed\_name, headline, url, published, metadata (JSON), page\_text, ingested\_at, deleted flag
- `entries_fts` — FTS5 virtual table mirroring headline and page\_text for fast full-text search
- `domain_settings` — per-domain key/value store for the entry age gate configuration

Deletions are **soft deletes** — the URL is preserved for deduplication but content is blanked and the entry is excluded from all queries.

---

## Project layout

```
MiniFeed/
├── main.py                  Entry point — starts uvicorn
├── requirements.txt         Python dependencies
├── config/                  Server config (host, port, paths)
├── feeds/                   Per-domain feed JSON files (auto-created)
├── data/                    SQLite databases, one per domain (auto-created)
└── app/
    ├── api.py               FastAPI routes — REST API + web UI
    ├── database.py          SQLite helpers, FTS5 search, age-gate settings
    ├── feed_manager.py      Feed JSON CRUD, domain lifecycle
    ├── ingest.py            RSS fetching, page extraction, scheduler, rate gate
    ├── config.py            Config loader
    ├── version.py           Version string
    └── templates/           Jinja2 HTML templates
        ├── base.html        Layout, shared CSS
        ├── index.html       Home page
        ├── domain.html      Domain view (feeds + entries + management)
        ├── entry.html       Single entry view
        └── search.html      Search results
```


---

## Quickstart

### Prerequisites

- Python 3.11 or newer
- pip

### 1. Clone and enter the directory

```sh
git clone https://github.com/your-org/MiniFeed.git
cd MiniFeed
```

### 2. Create a virtual environment

```sh
python -m venv .venv
```

Activate it:

| Platform | Command |
|---|---|
| Windows (PowerShell) | `.venv\Scripts\Activate.ps1` |
| Windows (CMD) | `.venv\Scripts\activate.bat` |
| macOS / Linux | `source .venv/bin/activate` |

### 3. Install dependencies

```sh
pip install -r requirements.txt
```

---

## Starting the server

```sh
python main.py
```

The server starts on **http://localhost:8000**.

On first startup, all previously configured feeds are ingested immediately in the background. Each feed then runs on its own schedule.

To stop the server press `Ctrl+C`.

---

## Interacting with MiniFeed

### Browser UI

| URL | Purpose |
|---|---|
| `http://localhost:8000/` | Home — manage feeds, view all domains |
| `http://localhost:8000/web/{domain}` | Paginated list of entries for a domain |
| `http://localhost:8000/web/{domain}/{id}` | Full entry including extracted page text |
| `http://localhost:8000/web/search?q=your+query` | Search across all feeds |
| `http://localhost:8000/api/docs` | Interactive Swagger API documentation |

#### Adding a feed

1. Open `http://localhost:8000/`.
2. Fill in the **Add Feed** form on the right:
   - **Feed name** — a human-readable label (e.g. `TechCrunch`)
   - **Domain** — a short slug used to group feeds (e.g. `tech`)
   - **RSS URL** — the full RSS/Atom feed URL
   - **Update rate** — how often to re-check the feed, in minutes (default: 60)
3. Click **Add Feed**. The feed is fetched immediately in the background.

#### Removing a feed

On the home page, click the **✕** button next to any feed in the configured feeds table.

---

### REST API

The full interactive reference is at `http://localhost:8000/api/docs`.

#### Feed management

```http
GET    /api/feeds                  List all configured feeds
POST   /api/feeds                  Add a new feed
DELETE /api/feeds/{feed_id}        Remove a feed
```

**Add a feed (POST /api/feeds)**

```sh
curl -X POST http://localhost:8000/api/feeds \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "tech",
    "name": "TechCrunch",
    "url": "https://techcrunch.com/feed/",
    "update_rate": 30
  }'
```

**Remove a feed (DELETE /api/feeds/{feed_id})**

```sh
curl -X DELETE http://localhost:8000/api/feeds/<feed_id>
```

#### Browsing content

```http
GET /api/domains                              List all domains with entry counts
GET /api/domains/{domain}/entries             Paginated entries (limit, offset params)
GET /api/domains/{domain}/entries/{entry_id}  Single entry with full content
```

**List domains**

```sh
curl http://localhost:8000/api/domains
```

**Get entries for a domain (50 at a time)**

```sh
curl "http://localhost:8000/api/domains/tech/entries?limit=50&offset=0"
```

**Get a single entry**

```sh
curl http://localhost:8000/api/domains/tech/entries/42
```

#### Search

```http
GET /api/search?q={query}                     Search all domains
GET /api/search?q={query}&domain={domain}     Search within one domain
```

```sh
curl "http://localhost:8000/api/search?q=artificial+intelligence&domain=tech"
```

---

## Data storage

| Path | Contents |
|---|---|
| `feeds.json` | Feed inventory (auto-created) |
| `data/{domain}.db` | SQLite database for each domain (auto-created) |

Each domain database holds: entry ID, feed name, headline, URL, published date, author/tags/summary metadata, and extracted full-page text.

---

## Project layout

```
MiniFeed/
├── main.py              Entry point — starts the server
├── requirements.txt     Python dependencies
├── feeds.json           Feed inventory (auto-created)
├── data/                SQLite databases, one per domain (auto-created)
└── app/
    ├── api.py           FastAPI routes (REST + web UI)
    ├── database.py      SQLite read/write helpers
    ├── feed_manager.py  feeds.json management
    ├── ingest.py        RSS fetching, page text extraction, scheduler
    └── templates/       Jinja2 HTML templates
```
