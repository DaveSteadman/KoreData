# KoreReference

Requirements and top-level design document for KoreReference, a Wikipedia-scale encyclopedia service for LLM agents and the KoreDataGateway.

---

## Purpose

Store, index, and serve a large corpus of encyclopedic reference articles modelled on Wikipedia. The primary consumer is an LLM agent querying the API, but a read-only web UI is also provided via KoreDataGateway.

Content is:
- **Reference** — factual, article-form text, organised by subject.
- **Interlinked** — articles reference other articles by title; link traversal is a first-class API operation.
- **Categorised** — each article belongs to one or more categories, enabling subject-area navigation.
- **Read-only after import** — no authoring, revision history, talk pages or discussion features.
- **Text-only** — no images, audio, or video; body text only.

---

## Content Source

The primary import path is a locally hosted [Kiwix](https://www.kiwix.org/) server serving a Wikipedia ZIM file.

- The Kiwix server exposes article content at `http://<host>/<zim-name>/A/<Article_Title>` as rendered HTML.
- Import strips navigation chrome, reference markers (`[1]`, `[2]`, …), infobox tables, and image captions.
- Internal `<a>` hyperlinks are converted to `[[Display|Target]]` wikilink markup before HTML-to-Markdown conversion, so inline links are preserved in the stored body.
- The remaining HTML is converted to CommonMark Markdown (GFM pipe tables, `##` headings, standard emphasis/bold/lists).
- Wikilinks embedded in the body are also extracted to populate the `links` table.
- Categories are extracted from the bottom of each article page before stripping.
- The `source_url` stores the Kiwix article URL so re-imports can detect and skip unchanged content (by comparing a hash of the raw HTML).

Secondary import path:
- Manual addition via `POST /articles` REST endpoint (for articles outside the ZIM, corrections, or custom entries).

---

## Scale

English Wikipedia contains approximately 6.7 million articles and 20 GB of plain text. The design must accommodate this without requiring distributed infrastructure:

- One SQLite file per language/corpus, stored in `data/`. Example: `data/enwiki.db`, `data/simplewiki.db`.
- SQLite WAL mode enables concurrent reads alongside the single write connection used during import.
- FTS5 full-text index covers `title` and `body`. At Wikipedia scale this index will be several GB; it is built incrementally during import and can be rebuilt with `INSERT INTO articles_fts(articles_fts) VALUES('rebuild')`.
- Import is resumable: the `source_id` (Kiwix article path) deduplicates rows, so an interrupted import can be restarted safely.
- Section content is stored as a JSON array within the article row rather than a separate table, keeping the schema simple while still allowing agents to request a single section by name.

---

## Data Model

### `articles` table

| Column        | Type    | Notes                                                                              |
|---------------|---------|------------------------------------------------------------------------------------|
| `id`          | INTEGER | Primary key, auto-increment                                                        |
| `title`       | TEXT    | Article title, unique per database                                                 |
| `redirect_to` | TEXT    | If non-NULL, this row is a redirect; the value is the canonical title              |
| `summary`     | TEXT    | First prose paragraph, extracted at import; used for lightweight agent lookups    |
| `body`        | TEXT    | Full article body in CommonMark Markdown + `[[wikilinks]]`; section headings as `## Heading` |
| `sections`    | TEXT    | JSON array of `{"title": str, "content": str}` objects, extracted from headings   |
| `categories`  | TEXT    | JSON array of category name strings                                                |
| `word_count`  | INTEGER | Word count of `body`; computed at import                                           |
| `source`      | TEXT    | Origin identifier, e.g. `kiwix/enwiki`, `manual`                                  |
| `source_id`   | TEXT    | Unique article path at source; used for deduplication and re-import detection      |
| `source_hash` | TEXT    | SHA-256 of raw source HTML; unchanged articles are skipped on re-import            |
| `added_at`    | TEXT    | UTC timestamp of first insertion                                                   |
| `updated_at`  | TEXT    | UTC timestamp of last update                                                       |

- `(title)` — unique index; lookups by title are the dominant read pattern.
- `(source_id)` — unique index; used to detect duplicates during import.
- FTS5 virtual table `articles_fts` indexes `title` and `body` with BM25 ranking.

### `links` table

Stores directed outbound links extracted from each article.

| Column           | Type    | Notes                                                                    |
|------------------|---------|--------------------------------------------------------------------------|
| `id`             | INTEGER | Primary key                                                              |
| `from_id`        | INTEGER | FK → `articles.id`                                                       |
| `to_title`       | TEXT    | Target article title as written in the source wikilink                   |
| `to_id`          | INTEGER | FK → `articles.id`; NULL if the target article is not yet imported       |

- Index on `(from_id)` — forward link traversal.
- Index on `(to_id)` — backlink traversal.
- `to_id` is resolved (filled in) as a post-import pass once the full article set is loaded.

### `categories` table

Normalised category registry for efficient category browsing.

| Column    | Type    | Notes                              |
|-----------|---------|------------------------------------|
| `id`      | INTEGER | Primary key                        |
| `name`    | TEXT    | Category name, unique              |
| `count`   | INTEGER | Number of articles in this category (denormalised, updated on import) |

### `article_categories` table

Many-to-many join between articles and categories.

| Column        | Type    | Notes                   |
|---------------|---------|-------------------------|
| `article_id`  | INTEGER | FK → `articles.id`      |
| `category_id` | INTEGER | FK → `categories.id`    |

- Unique index on `(article_id, category_id)`.

---

## API

KoreReference exposes a REST API (FastAPI). There is no local web UI — all user interaction goes through KoreDataGateway. All routes are read-only except admin/import operations.

### Articles

| Method   | Path                              | Description                                                         |
|----------|-----------------------------------|---------------------------------------------------------------------|
| `GET`    | `/articles`                       | List articles (title + summary only); supports `limit`, `offset`   |
| `GET`    | `/articles/{title}`               | Get article by title; transparently follows redirects               |
| `GET`    | `/articles/{title}/summary`       | Summary paragraph only (first-paragraph, fast for agents)           |
| `GET`    | `/articles/{title}/section/{name}`| Single named section of an article                                  |
| `GET`    | `/articles/{title}/links`         | Outbound links from this article                                    |
| `GET`    | `/articles/{title}/backlinks`     | Articles that link to this article; supports `limit`, `offset`      |
| `GET`    | `/articles/{title}/categories`    | Categories this article belongs to                                  |
| `GET`    | `/articles/random`                | A random non-redirect article (useful for agent exploration)        |
| `POST`   | `/articles`                       | Add or upsert an article (admin / manual import)                    |
| `DELETE` | `/articles/{title}`               | Remove an article and its links                                     |

`GET /articles/{title}` response fields:
- `id`, `title`, `summary`, `body`, `sections`, `categories`, `word_count`, `source`, `added_at`, `updated_at`
- `redirect_to` — present and non-null if this is a redirect; the client should re-request the canonical title.

`GET /articles/{title}/summary` response:
- `title`, `summary`, `word_count`, `categories`
- Intended for agents that need a brief orientation before deciding whether to fetch the full body.

`GET /articles/{title}/section/{name}` response:
- `title`, `section`, `content`
- `section` is case-insensitive matched against the section title.
- Returns 404 if the section does not exist.

### Search

| Method | Path      | Description                                                            |
|--------|-----------|------------------------------------------------------------------------|
| `GET`  | `/search` | Full-text and/or prefix search; returns title + summary + snippet      |

Query parameters:
- `q` — full-text query across `title` and `body` (BM25 ranked)
- `title` — prefix match on title only (useful for autocomplete / disambiguation)
- `category` — filter results to a category
- `limit`, `offset` — pagination (default limit 20)

Response per result:
- `id`, `title`, `summary`, `snippet` (FTS5 highlight of matching passage), `score`

### Categories

| Method | Path                   | Description                                    |
|--------|------------------------|------------------------------------------------|
| `GET`  | `/categories`          | List all categories with article counts        |
| `GET`  | `/categories/{name}`   | List articles in a category (title + summary)  |

### Import

| Method | Path             | Description                                                         |
|--------|------------------|---------------------------------------------------------------------|
| `POST` | `/import/kiwix`  | Trigger an import run from the configured Kiwix server              |
| `GET`  | `/import/status` | Progress of an in-progress import (articles done, total estimated)  |

`POST /import/kiwix` body:
- `zim_name` — the ZIM identifier as served by the Kiwix server (e.g. `wikipedia_en_all_nopic`)
- `limit` — optional maximum number of articles to import (for partial/test imports)
- `resume` — if true (default), skip articles whose `source_hash` is unchanged

### Admin

| Method | Path       | Description                                         |
|--------|------------|-----------------------------------------------------|
| `GET`  | `/status`  | Server stats: article count, link count, FTS status |

`GET /status` response includes:
- `total_articles`, `total_redirects`, `total_links`, `total_categories`
- `fts_indexed` — whether the FTS index is up to date
- `databases` — list of loaded database files

---

## Article Body Format

The canonical storage format for article body text is **CommonMark Markdown** with a `[[wikilinks]]` extension. This is the format stored in the `body` column and in each section's `content` field. It is also the format displayed in the edit textarea — no conversion occurs on load or save.

### Syntax reference

| Element | Syntax | Notes |
|---------|--------|-------|
| Section headings | `## Heading` / `### Sub-heading` | H2 for top-level, H3 for sub-sections |
| Internal link | `[[Target Title]]` | Resolved at render time |
| Internal link with display text | `[[Display text\|Target Title]]` | Pipe separates display from target |
| External link | `[text](url)` | Standard Markdown |
| Tables | GFM pipe table syntax | No raw HTML stored |
| Emphasis / bold | `*em*` / `**bold**` | Standard Markdown |
| Lists | `- item` / `1. item` | Standard Markdown |

### Import pipeline (Kiwix)

1. Fetch article HTML from Kiwix (following any redirects).
2. For each internal `<a href="...">` tag in the HTML: replace with `[[Display|Target]]` wikilink markup in-place.
3. Convert the modified HTML to CommonMark Markdown via `markdownify` (or equivalent), preserving GFM pipe tables.
4. Strip any residual navigation chrome or artefacts.
5. Split body into sections on `## ` heading markers; store as JSON array.

### Manual entry

Operators type raw Markdown + `[[wikilinks]]` directly into the edit textarea. The stored value is exactly what was entered — no HTML, no reconstruction.

### Round-trip fidelity requirement

> **The stored `body` text, when displayed in the edit textarea, must be the exact same bytes that were saved.** No conversion, heading reconstruction, or section re-parsing happens on load. What you save is what you edit.

This is the core rule that eliminates the class of bugs where edit → save → re-edit loses formatting.

---

## Presentation (KoreDataGateway)

The KoreDataGateway renders stored Markdown + `[[wikilinks]]` into HTML. Rendering is entirely the Gateway's responsibility; KoreReference stores and serves only the raw Markdown.

### Rendering pipeline

1. **Resolve wikilinks** — for each `[[Target]]` or `[[Display|Target]]`:
   - Target exists in the database → `<a href="/reference/Target">Display</a>`
   - Target absent → `<a class="unresolved" href="#">Display</a>` (dimmed, non-navigable)
2. **Render Markdown to HTML** — CommonMark + GFM tables via `mistune` (or equivalent).
3. **Table styling** — rendered `<table>` elements receive CSS: collapsed borders, distinct header row, alternating row shading.

### Edit UI

- Textarea shows the raw stored Markdown — no HTML, no Server-side conversion.
- Word count is computed from the raw body string.
- On save, the textarea value is stored verbatim; sections are re-extracted from `## ` headings in the submitted body.

### CSS / layout rules

| Element | Rule |
|---------|------|
| Article body | `white-space: normal` — Markdown paragraphs are block elements, not pre-formatted |
| Source URL metadata cell | `word-break: break-all; overflow-wrap: anywhere` — long unspaced strings must not overflow |
| Outbound-links panel | All links (resolved and unresolved) rendered as `<a>` tags; unresolved use `color: var(--dim)` |
| Import progress bar | Capped at 99 % while running; snaps to 100 % only on completion |

---

## Link Resolution and Traversal

Wikilinks in articles are stored as `to_title` strings in the `links` table at import time. After a full import (or incrementally), a resolution pass sets `to_id` by matching `to_title` against `articles.title`, also following redirects.

An agent can use links to:
- **Expand context** — fetch linked articles on related subjects.
- **Verify claims** — check the content of a cited article.
- **Explore topic graphs** — traverse forward links depth-first.

The backlinks endpoint serves the reverse direction: given a subject article, find all articles that reference it. This is useful for understanding how central a concept is.

---

## Redirect Handling

Many titles in Wikipedia resolve to another article (redirects, alt spellings, acronyms). The `redirect_to` column stores the canonical title for redirect rows. `GET /articles/{title}` transparently resolves one level of redirect in the response, returning the canonical article body with a `redirected_from` field. Circular redirects are detected and return a 400 error.

---

## Agent Usage Patterns

The API is designed for the following common agent workflows:

**Lookup** — `GET /articles/{title}` for a known subject. Check `summary` first; fetch `body` or individual sections only when needed.

**Search and select** — `GET /search?q=...` to find relevant articles when the exact title is unknown. Use snippet and summary to pick the best match before fetching the full body.

**Disambiguation** — `GET /search?title=...` returns prefix matches; if multiple articles share a name pattern, the agent selects the appropriate one from the summaries.

**Topic graph traversal** — `GET /articles/{title}/links` to find related subjects; fetch summaries of linked articles to decide which to read in full.

**Category survey** — `GET /categories/{name}` to enumerate all articles in a domain (e.g. "World War II", "Python programming language").

**Random exploration** — `GET /articles/random` for serendipitous context injection.

---

## Configuration

Behaviour is controlled by a JSON config file (`config/default.json`).

| Key           | Default     | Description                                          |
|---------------|-------------|------------------------------------------------------|
| `port`        | `8300`      | HTTP port                                            |
| `host`        | `0.0.0.0`   | Bind address                                         |
| `data_dir`    | `data`      | Directory for database files                         |
| `log_level`   | `info`      | Uvicorn log level                                    |
| `kiwix_url`   | `http://127.0.0.1:8888` | Base URL of local Kiwix server          |
| `default_db`  | `enwiki`    | Default database (ZIM name prefix) used when no corpus is specified |

Multiple databases can coexist in `data_dir`; the `default_db` is loaded at startup and additional databases are loaded on demand by specifying a `db` query parameter on any endpoint.

---

## Application

- Console application, run with `python main.py`.
- Single SQLite write connection per database; multiple read connections via `check_same_thread=False` and WAL mode.
- Import runs in a background thread so the API remains responsive during bulk imports.
- Follows the same versioning scheme as KoreFeed and KoreLibrary: `[NNNN / X.Y+dev]`.
