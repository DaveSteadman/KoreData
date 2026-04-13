# KoreData

A collection of local data services for LLM agents.

KoreData provides structured, searchable content across three domains — live news feeds, long-form books, and encyclopedic reference articles — through a single unified API gateway. Agents query one endpoint and get ranked results from whichever services are relevant.

![KoreData](/Progress/readme_banner.png)

## Why this exists

- **Agent-first**: every service exposes a REST search API designed for LLM agent consumption, not just human browsing.
- **Local and offline**: content is stored in local SQLite databases; no cloud search dependencies, no per-query API costs.
- **Unified gateway**: a single `POST /search` call across KoreDataGateway reaches all three data domains simultaneously.
- **Practical sources**: RSS feeds via trafilatura, ebooks via Project Gutenberg (Kiwix), and Wikipedia-scale reference articles via Kiwix ZIM files.

## Services

| Service | Port | Status | Description |
|---|---|---|---|
| **KoreDataGateway** | 8800 | Planned | Unified agent API; proxies and aggregates results from all child services |
| **KoreFeed** | 8801 | Working | RSS ingest with full-text scraping and per-domain SQLite/FTS5 storage |
| **KoreLibrary** | 8802 | In development | Long-form ebook and document store, imported from Kiwix / Project Gutenberg |
| **KoreRAG** | 8803 | Planned | Vector chunk store for RAG; segments and embeds documents for semantic retrieval |
| **KoreReference** | 8804 | Planned | Wikipedia-scale encyclopedic articles with wikilink traversal |

---

## Get Running Fast

```powershell
git clone https://github.com/DaveSteadman/KoreData.git
cd KoreData
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

`main.py` at the repo root launches KoreDataGateway, which starts and supervises all child services.

Then open:

```text
http://localhost:8800/
```

The feed scheduler starts immediately. Configured feeds are fetched on their own intervals; a restart respects each feed's last-fetched timestamp so nothing is re-ingested unnecessarily.

---

## First 10 Minutes

Once KoreFeed is running, try a keyword search against the live database:

```text
http://localhost:8801/search?q=artificial+intelligence
```

What you should see:

- ranked results from all ingested feed entries, with title, source domain, published date, and a content snippet
- entries link through to the full stored text so you can verify what the agent would receive

Browse by domain to see per-source coverage:

```text
http://localhost:8801/
```

This is the core loop: feeds arrive on schedule, full page text is extracted, stored, and immediately searchable — by browser or by agent.

---

## What You Can Do

- Point an LLM agent at `POST /search` on KoreDataGateway (port 8800) and receive ranked results across all three data domains in one call.
- Manage feed inventories — add, remove, or adjust domains and fetch intervals — via the REST API or the browser UI.
- Import an ebook collection from a local Kiwix server running Project Gutenberg ZIM content into KoreLibrary.
- Import Wikipedia (or any Kiwix-hosted wiki) into KoreReference and traverse inter-article links as part of an agent research workflow.
- Date-filter feed search results so agents can ask for content published within a specific window.

---

## Architecture

```
LLM Agent
    │
    ▼
KoreDataGateway  :8800
    ├── POST /search   ◄── primary agent endpoint
    ├── /feeds/*       ──► KoreFeed      :8801
    ├── /library/*     ──► KoreLibrary   :8802
    ├── /rag/*         ──► KoreRAG       :8803
    └── /reference/*   ──► KoreReference :8804
```

KoreDataGateway launches and supervises the three child services as subprocesses, waits for each to become healthy, and proxies all UI and API requests through. The gateway's `POST /search` endpoint fans requests out to however many services are specified in the call, merges the results, and returns a single structured JSON response.

While KoreDataGateway is under development, each service can be started and used independently.

---

## Works With

KoreData is designed to be the data layer for [MiniAgentFramework](https://github.com/DaveSteadman/MiniAgentFramework), a local-first Ollama-based agent framework. Point MiniAgentFramework at `POST http://localhost:8800/search` to give agents access to live news, books, and reference articles without any cloud search dependency.

---

## Documentation

| Document | Contents |
|---|---|
| **This file** | Overview, quickstart, architecture |
| [KoreDataGateway/DESIGN.md](KoreDataGateway/DESIGN.md) | Gateway API contract, proxy design, unified search schema |
| [KoreFeed/DESIGN.md](KoreFeed/DESIGN.md) | Feed ingest pipeline, database schema, FTS search design |
| [KoreLibrary/DESIGN.md](KoreLibrary/DESIGN.md) | Book storage schema, Kiwix import path, search API |
| [KoreReference/DESIGN.md](KoreReference/DESIGN.md) | Article schema, wikilink model, Wikipedia-scale import design |
| [DESIGN.md](DESIGN.md) | Top-level system purpose and service breakdown |

---

## Screenshots

[![KoreData Web UI – April 2026](Progress/2026-04-13%20WebUI.png)](Progress/2026-04-13%20WebUI.png)
