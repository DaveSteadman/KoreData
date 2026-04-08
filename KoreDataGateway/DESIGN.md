# KoreDataGateway

Requirements and top-level design document for KoreDataGateway, the central hub for the KoreData system.

---

## Purpose

KoreDataGateway has two distinct but related roles:

1. **Agent search interface** — the primary API that LLM agents call. Accepts a unified search request across one or more KoreData services and returns a structured JSON response the agent can act on.

2. **Service management hub** — launches, monitors, and terminates the child services (KoreFeed, KoreLibrary, KoreReference) and provides a web UI for each.

The agent search interface is the **primary goal**. All other features exist to support or manage the system that makes search possible.

---

## Architecture

```
LLM Agent
    │
    ▼
KoreDataGateway  :8200
    ├── POST /search  ◄── primary agent API
    ├── GET  /        ◄── landing page (health + search test UI)
    ├── GET  /status  ◄── machine-readable health
    ├── /feeds/*      ◄── proxied to KoreFeed      → :8000  (child process)
    ├── /library/*    ◄── proxied to KoreLibrary   → :8100  (child process)
    └── /reference/*  ◄── proxied to KoreReference → :8300  (child process)
```

The gateway starts all three child services at startup (subprocess), waits for each to become healthy, and terminates them cleanly at shutdown. It holds persistent `httpx.AsyncClient` connections to each child.

---

## Primary Agent API — `POST /search`

### Purpose

A single endpoint that an LLM agent calls to search across any combination of KoreData services with one request. The agent receives a structured list of results it can use directly or retrieve in full via follow-up calls.

### Request body (JSON)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | `string` | yes | Natural-language or keyword query string |
| `domains` | `array[string]` | no | Which services to search: `"feeds"`, `"reference"`, `"library"`. Omit or pass `[]` to search all three |
| `since` | `string` (ISO 8601 date `YYYY-MM-DD`) | no | Earliest published-date filter — applied to KoreFeed only |
| `until` | `string` (ISO 8601 date `YYYY-MM-DD`) | no | Latest published-date filter — applied to KoreFeed only |
| `limit` | `integer` | no | Maximum results **per domain** (default 5, min 1, max 20) |

Example:
```json
{
  "query": "climate change arctic ice",
  "domains": ["feeds", "reference"],
  "since": "2025-01-01",
  "limit": 5
}
```

### Response body (JSON)

```json
{
  "query": "climate change arctic ice",
  "domains_searched": ["feeds", "reference"],
  "results": {
    "feeds": [
      {
        "type": "feed_entry",
        "id": 1042,
        "title": "Arctic ice reaches record low in 2025",
        "source": "BBC Science",
        "published_at": "2026-03-14 09:00:00",
        "snippet": "…scientists warned that the Arctic ice sheet shrank…",
        "url": "/feeds/science/1042"
      }
    ],
    "reference": [
      {
        "type": "reference_article",
        "title": "Arctic sea ice decline",
        "summary": "Arctic sea ice decline refers to the loss of the Arctic Ocean ice cover…",
        "snippet": "…accelerating ice loss linked to greenhouse gas emissions…",
        "word_count": 4200,
        "url": "/reference/Arctic_sea_ice_decline"
      }
    ]
  }
}
```

- `domains_searched` lists the domains that were actually queried.
- Domains not in the request are absent from `results`.
- A domain that errors returns `{"error": "HTTP <status>"}` instead of an array.

#### Result fields by domain

**`feeds` result:**
| Field | Description |
|-------|-------------|
| `type` | `"feed_entry"` |
| `id` | KoreFeed entry ID |
| `title` | Entry headline |
| `source` | Feed name or domain slug |
| `published_at` | Publication timestamp (`YYYY-MM-DD HH:MM:SS`) |
| `snippet` | First 300 chars of page text / content / body / summary |
| `url` | Gateway path to the full entry — `GET /feeds/{domain}/{id}` |

**`reference` result:**
| Field | Description |
|-------|-------------|
| `type` | `"reference_article"` |
| `title` | Article title |
| `summary` | First-paragraph summary stored with the article |
| `snippet` | FTS highlight snippet (SQLite `snippet()`, ~20 tokens) or summary fallback |
| `word_count` | Body word count |
| `url` | Gateway path to the full article — `GET /reference/{title}` |

**`library` result:**
| Field | Description |
|-------|-------------|
| `type` | `"library_book"` |
| `id` | KoreLibrary book ID |
| `title` | Book title |
| `author` | Author name(s) |
| `snippet` | FTS highlight snippet or first 300 chars of notes |
| `url` | Gateway path to the full book — `GET /library/{id}` |

### Agent retrieval pattern

After receiving search results the agent fetches full content as needed:

- `GET /reference/{title}` — article JSON with body, sections, links, backlinks
- `GET /feeds/{domain}/{entry_id}` — full feed entry including page text
- `GET /library/{book_id}` — full book including body

These routes return the same data used by the web UI.

---

## Landing Page — `GET /`

The landing page is the home for both human operators and the health monitor. It displays:

### Service health panel

For each child service (KoreFeed, KoreLibrary, KoreReference):

- **Name** and status badge: `● ONLINE` (green) / `● OFFLINE` (red)
- **Stats** pulled from the service's `/status` endpoint:
  - KoreFeed: total domains, total feeds, total entries
  - KoreLibrary: total books, incomplete records
  - KoreReference: total articles, total redirects, total links, total categories
- **Quick links** to each service's UI section (Browse, Search, Import where applicable)
- Status auto-refreshes every **60 seconds** via `GET /status`; manual ↺ REFRESH button also available

### Search test panel

Mirrors exactly what an agent calls via `POST /search`:

- `query` text input (Enter key or SEARCH button submits)
- `domains` checkboxes — Feeds / Reference / Library (all checked by default)
- `since` / `until` date pickers with calendar icon
- `No Older Than` num-stepper (sets `since` to today − N days)
- `limit` num-stepper (default 5, max 20)
- Results shown in a switchable **CARDS** / **JSON** view below the panel

### `GET /status`

Machine-readable health endpoint used by the landing page and agents.

```json
{
  "service": "KoreDataGateway",
  "version": "...",
  "children": {
    "korefeed":      { "url": "http://127.0.0.1:8000", "healthy": true, ... },
    "korelibrary":   { "url": "http://127.0.0.1:8100", "healthy": true, ... },
    "korereference": { "url": "http://127.0.0.1:8300", "healthy": true, ... }
  }
}
```

Each child object merges the child's own `/status` fields with `url` and `healthy`.

---

## Service Management

### Startup

At gateway startup:

1. All three child services are started as subprocesses via `subprocess.Popen`.
2. stdout/stderr of each child is redirected to `<service>/data/service.log`.
3. The gateway polls each child's `GET /status` endpoint until it responds 200 or a 20-second timeout elapses.
4. `httpx.AsyncClient` connections are held for the lifetime of the process.

### Shutdown

At gateway shutdown (SIGTERM or process exit):

1. Each child receives `SIGTERM`.
2. Gateway waits up to 6 seconds per child for clean exit.
3. Any child that does not exit is sent `SIGKILL`.
4. Log file handles are closed.

### Configuration — `config/default.json`

| Key | Default | Description |
|-----|---------|-------------|
| `port` | `8200` | Gateway listen port |
| `host` | `0.0.0.0` | Gateway bind address |
| `log_level` | `"info"` | Uvicorn log level |
| `korefeed_url` | `http://127.0.0.1:8000` | KoreFeed base URL |
| `korelibrary_url` | `http://127.0.0.1:8100` | KoreLibrary base URL |
| `korereference_url` | `http://127.0.0.1:8300` | KoreReference base URL |

---

## Web UI Routes

The gateway proxies web UI interactions to the appropriate child service and renders the response via Jinja2 templates. All HTML pages are rendered by the gateway — child services return only JSON.

### Root

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Landing page: service health + search test panel |
| `GET` | `/status` | Machine-readable health JSON (gateway + all children) |
| `POST` | `/search` | Primary agent search API |

### KoreFeed — `/feeds/*`

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/feeds` | Feed domain list with feed management UI |
| `GET` | `/feeds/search` | Search feed entries (`q`, `domain`, `since`, `until`, `limit`) |
| `GET` | `/feeds/{domain}` | Entry list for a domain with age/purge controls |
| `GET` | `/feeds/{domain}/{entry_id}` | Single entry detail |
| `POST` | `/feeds/domains/create` | Create domain |
| `POST` | `/feeds/domains/{domain}/delete` | Delete domain |
| `POST` | `/feeds/domains/{domain}/rename` | Rename domain |
| `POST` | `/feeds/{domain}/feeds/add` | Add feed to domain |
| `POST` | `/feeds/{domain}/feeds/{feed_id}/delete` | Remove feed |
| `POST` | `/feeds/{domain}/feeds/{feed_id}/refresh` | Force refresh (returns JSON) |
| `POST` | `/feeds/{domain}/entries/{entry_id}/delete` | Delete entry (returns JSON) |
| `POST` | `/feeds/{domain}/entries/delete-older-than` | Bulk age purge |
| `POST` | `/feeds/{domain}/entries/delete-by-feed` | Purge by feed name |
| `POST` | `/feeds/entries/bulk-delete` | Bulk delete by ID list (form `sel[]=domain:id`) |
| `POST` | `/feeds/{domain}/settings/age-mode` | Set age/calendar retention mode |
| `POST` | `/feeds/{domain}/entries/delete-outside-calendar` | Calendar purge |
| `PATCH` | `/api/feeds/{feed_id}/rate` | Update feed poll rate in minutes (browser JS → proxied to KoreFeed) |

### KoreLibrary — `/library/*`

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/library` | Book list with inline search panel |
| `GET` | `/library/incomplete` | Books with missing metadata fields |
| `GET` | `/library/search` | Search books (`q`, `author`, `title`, `year`, `language`, `genre`, `limit`, `offset`) |
| `GET` | `/library/import` | Import UI (manual + Kiwix) |
| `POST` | `/library/import/manual` | Manual import form submission |
| `GET` | `/library/kiwix/inventory` | ZIM book inventory (JSON) |
| `GET` | `/library/kiwix/suggest` | Title suggest from Kiwix (JSON) |
| `GET` | `/library/kiwix/search` | Search within Kiwix (JSON) |
| `POST` | `/library/import/kiwix` | Start Kiwix import (JSON) |
| `POST` | `/library/import/kiwix/viewer` | Kiwix viewer proxy (JSON) |
| `GET` | `/library/{book_id}/edit` | Edit book metadata form |
| `POST` | `/library/{book_id}/edit` | Save book edits |
| `POST` | `/library/{book_id}/delete` | Delete book |
| `POST` | `/library/{book_id}/repair-anchors` | Repair broken anchor spans in stored body |
| `GET` | `/library/{book_id}` | View book |

### KoreReference — `/reference/*`

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/reference` | Article index (recent articles) |
| `GET` | `/reference/search` | Search articles by full-text query (`q`, `limit`, `offset`) |
| `GET` | `/reference/categories` | Category list |
| `GET` | `/reference/categories/{name}` | Articles in category |
| `GET` | `/reference/import` | Kiwix crawl import UI |
| `POST` | `/reference/import/crawl` | Start crawl (proxied JSON) |
| `GET` | `/reference/import/status` | Crawl progress (proxied JSON) |
| `POST` | `/reference/import/stop` | Stop crawl (proxied JSON) |
| `GET` | `/reference/new` | New article form |
| `POST` | `/reference/new` | Create article |
| `GET` | `/reference/{title}/edit` | Edit article |
| `POST` | `/reference/{title}/edit` | Save article edits (upsert via KoreReference `/articles`) |
| `POST` | `/reference/{title}/delete` | Delete article |
| `GET` | `/reference/{title}/links-json` | Outbound links JSON (for UI panel) |
| `GET` | `/reference/{title}` | View article with backlinks panel |

---

## Content Rendering

The gateway is solely responsible for converting stored content into HTML for the browser. Child services return and store only raw data.

### Reference article body (wikitext + `[[wikilinks]]`)

Article bodies are stored as wikitext with `[[wikilinks]]` notation and `== Heading ==` section markers. The gateway renders them via the `wikilinks` Jinja2 filter:

1. Split body on `<<<TABLE>>>…<<<ENDTABLE>>>` markers to separate pre-rendered HTML table segments from plain wikitext.
2. For plain wikitext segments: escape HTML, convert `[[Display|Target]]` / `[[Target]]` to `<a href="/reference/{target}">` anchors, convert double newlines to `</p><p>` and single newlines to `<br>`.
3. For `<<<TABLE>>>` segments: resolve `[[wikilinks]]` inside the pre-rendered HTML and pass through as-is.
4. Both resolved and unresolved wikilinks link to `/reference/{title}` (resolution is done at import time by KoreReference, not at render time).

### Reference article — edit round-trip

When editing a Kiwix-imported article whose body was stored as structured `sections` (not raw `== Heading ==` markers), the gateway reconstructs the `== Heading ==` form from `article.sections` before populating the edit textarea so the format round-trips correctly through save.

### Library book body

Library books store bodies as Markdown (imported via `markdownify` from HTML). The body is injected into the page as a JSON string via Jinja2 (`| tojson`), then rendered in the browser as HTML by the `marked.js` library (loaded from CDN). Markdown syntax — headers, bold, links, code blocks — is fully parsed and displayed client-side.

---

## Non-Goals

- The gateway does **not** store any data of its own (no database, no persistent state beyond child process handles).
- The gateway does **not** implement authentication or access control — it is an internal tool on a trusted network.
- The gateway does **not** provide write access to data via the agent API — `POST /search` and all `GET` retrieval routes are read-only from the agent's perspective.
