import json
import re
import sqlite3
import zlib
from contextlib import contextmanager
from pathlib import Path
from typing import Optional


def _compress(text: Optional[str]) -> Optional[bytes]:
    if not text:
        return None
    return zlib.compress(text.encode("utf-8"), level=6)


def _decompress(blob: Optional[bytes]) -> Optional[str]:
    if not blob:
        return None
    if isinstance(blob, str):
        return blob  # legacy uncompressed row
    return zlib.decompress(blob).decode("utf-8")

from app.config import cfg

DATA_DIR = Path(cfg["data_dir"])
_DB_PATH = DATA_DIR / "reference.db"


def get_db_path() -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    return _DB_PATH


@contextmanager
def db_connection():
    conn = sqlite3.connect(str(get_db_path()), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_db() -> None:
    with db_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT    NOT NULL,
                redirect_to TEXT,
                summary     TEXT,
                body        TEXT,
                word_count  INTEGER,
                facts       TEXT
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_title
            ON articles (title)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS links (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                from_id  INTEGER NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
                to_title TEXT    NOT NULL,
                to_id    INTEGER REFERENCES articles(id) ON DELETE SET NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_links_from ON links (from_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_links_to   ON links (to_id)")
        # FTS: contentless — body is stored compressed so triggers can't index it.
        # Python code in upsert/delete manages FTS explicitly with plain text.
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
                title, body,
                tokenize='unicode61 remove_diacritics 1',
                content=''
            )
        """)
        # Drop old content-table triggers if they exist from a previous schema
        for _trg in ("articles_ai", "articles_ad", "articles_au"):
            conn.execute(f"DROP TRIGGER IF EXISTS {_trg}")
        # Migrate: compress body for existing uncompressed rows and rebuild FTS
        _need_compress = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE typeof(body)='text' AND body IS NOT NULL"
        ).fetchone()[0]
        if _need_compress:
            rows = conn.execute("SELECT id, title, body FROM articles WHERE typeof(body)='text'").fetchall()
            # Rebuild FTS clean
            conn.execute("DELETE FROM articles_fts")
            for _row in rows:
                _blob = _compress(_row["body"])
                conn.execute("UPDATE articles SET body=? WHERE id=?", (_blob, _row["id"]))
                conn.execute(
                    "INSERT INTO articles_fts(rowid, title, body) VALUES (?,?,?)",
                    (_row["id"], _row["title"] or "", _row["body"] or ""),
                )
        # Migrate: add facts column if not present (for databases created before this feature)
        _cols = {row[1] for row in conn.execute("PRAGMA table_info(articles)")}
        if "facts" not in _cols:
            conn.execute("ALTER TABLE articles ADD COLUMN facts TEXT")
        # Migrate: drop sections column (data is now derived from body at read time)
        if "sections" in _cols:
            conn.execute("ALTER TABLE articles DROP COLUMN sections")
        # Migrate: drop legacy metadata columns if present
        # SQLite refuses DROP COLUMN when an index references that column, so
        # we first detect and drop any such indexes.
        for _col in ("source", "source_id", "source_hash", "added_at", "updated_at"):
            if _col in _cols:
                for _idx in conn.execute("PRAGMA index_list(articles)").fetchall():
                    _idx_name = _idx[1]
                    _idx_cols = {r[2] for r in conn.execute(f"PRAGMA index_info({_idx_name})")}
                    if _col in _idx_cols:
                        conn.execute(f"DROP INDEX IF EXISTS [{_idx_name}]")
                conn.execute(f"ALTER TABLE articles DROP COLUMN {_col}")
        # Migrate: drop categories column and tables
        if "categories" in _cols:
            conn.execute("ALTER TABLE articles DROP COLUMN categories")
        _tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "article_categories" in _tables:
            conn.execute("DROP TABLE article_categories")
        if "categories" in _tables:
            conn.execute("DROP TABLE categories")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _word_count(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    return len(text.split())


def _parse_json_list(value: Optional[str]) -> list:
    if not value:
        return []
    try:
        return json.loads(value)
    except Exception:
        return []


_ARTICLE_META_COLS = (
    "id", "title", "redirect_to", "summary", "word_count",
)
_ARTICLE_FULL_COLS = _ARTICLE_META_COLS + ("body", "facts")


_HEADING_RE = re.compile(r'^== (.+?) ==$')


def body_to_sections(body: Optional[str]) -> list[dict]:
    """Derive [{title, content}] sections from the inline == Heading == markers in body."""
    if not body:
        return []
    sections: list[dict] = []
    current_title: Optional[str] = None
    current_parts: list[str] = []
    for line in body.split("\n\n"):
        m = _HEADING_RE.match(line.strip())
        if m:
            if current_title is not None:
                sections.append({"title": current_title,
                                  "content": "\n\n".join(current_parts).strip()})
            current_title = m.group(1)
            current_parts = []
        else:
            if line.strip():
                current_parts.append(line)
    if current_title is not None:
        sections.append({"title": current_title,
                          "content": "\n\n".join(current_parts).strip()})
    return sections


def _row_to_dict(row: sqlite3.Row, full: bool = False) -> dict:
    cols = _ARTICLE_FULL_COLS if full else _ARTICLE_META_COLS
    d = {c: row[c] for c in cols}
    if full:
        d["body"]     = _decompress(d.get("body"))
        d["sections"] = body_to_sections(d.get("body"))
        d["facts"]    = _parse_json_list(d.get("facts"))
    return d


# ---------------------------------------------------------------------------
# Article CRUD
# ---------------------------------------------------------------------------

def get_article_by_title(title: str, full: bool = True) -> Optional[dict]:
    cols = ", ".join(_ARTICLE_FULL_COLS if full else _ARTICLE_META_COLS)
    with db_connection() as conn:
        row = conn.execute(
            f"SELECT {cols} FROM articles WHERE title = ?", (title,)
        ).fetchone()
    return _row_to_dict(row, full=full) if row else None


def get_article_by_id(article_id: int, full: bool = True) -> Optional[dict]:
    cols = ", ".join(_ARTICLE_FULL_COLS if full else _ARTICLE_META_COLS)
    with db_connection() as conn:
        row = conn.execute(
            f"SELECT {cols} FROM articles WHERE id = ?", (article_id,)
        ).fetchone()
    return _row_to_dict(row, full=full) if row else None


def resolve_article(title: str) -> Optional[dict]:
    """Fetch article, following up to 5 levels of redirect."""
    seen: set[str] = set()
    redirected_from: Optional[str] = None
    current = title
    while current and current not in seen:
        seen.add(current)
        article = get_article_by_title(current, full=True)
        if article is None:
            return None
        if not article["redirect_to"]:
            if redirected_from:
                article["redirected_from"] = redirected_from
            return article
        redirected_from = redirected_from or current
        current = article["redirect_to"]
    return None


def list_articles(limit: int = 100, offset: int = 0) -> list[dict]:
    cols = ", ".join(_ARTICLE_META_COLS)
    with db_connection() as conn:
        rows = conn.execute(
            f"SELECT {cols} FROM articles WHERE redirect_to IS NULL "
            f"ORDER BY title LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def upsert_article(
    title: str,
    body: Optional[str],
    summary: Optional[str] = None,
    facts: Optional[list] = None,
    redirect_to: Optional[str] = None,
    link_titles: Optional[list[str]] = None,
    **_ignored,
) -> dict:
    """Insert or update an article."""
    title = title.strip()
    wc = _word_count(body)
    facts_json = json.dumps(facts or [])

    with db_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM articles WHERE title = ?", (title,)
        ).fetchone()

        plain_body = body or ""
        compressed_body = _compress(body)

        if existing:
            article_id = existing["id"]
            # Update FTS with plain text before storing compressed
            conn.execute(
                "INSERT INTO articles_fts(articles_fts, rowid, title, body) VALUES('delete',?,?,?)",
                (article_id, title, plain_body),
            )
            conn.execute(
                "INSERT INTO articles_fts(rowid, title, body) VALUES(?,?,?)",
                (article_id, title, plain_body),
            )
            conn.execute("""
                UPDATE articles
                SET redirect_to=?, summary=?, body=?,
                    facts=?, word_count=?
                WHERE id=?
            """, (redirect_to, summary, compressed_body,
                  facts_json, wc, article_id))
            conn.execute("DELETE FROM links WHERE from_id=?", (article_id,))
        else:
            cur = conn.execute("""
                INSERT INTO articles
                    (title, redirect_to, summary, body,
                     facts, word_count)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (title, redirect_to, summary, compressed_body,
                  facts_json, wc))
            article_id = cur.lastrowid
            # Sync FTS with plain text after insert
            conn.execute(
                "INSERT INTO articles_fts(rowid, title, body) VALUES(?,?,?)",
                (article_id, title, plain_body),
            )

        # Insert links (to_id resolved later)
        for lt in (link_titles or []):
            conn.execute(
                "INSERT INTO links (from_id, to_title) VALUES (?, ?)",
                (article_id, lt),
            )

    return get_article_by_id(article_id, full=False)


def delete_article(title: str) -> bool:
    with db_connection() as conn:
        row = conn.execute("SELECT id FROM articles WHERE title=?", (title,)).fetchone()
        if not row:
            return False
        conn.execute(
            "INSERT INTO articles_fts(articles_fts, rowid, title, body) VALUES('delete',?,?,'')",
            (row["id"], title),
        )
        conn.execute("DELETE FROM articles WHERE id=?", (row["id"],))
        return True


def delete_all_articles() -> int:
    """Delete every article row. Returns number of rows deleted."""
    with db_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        conn.execute("DELETE FROM articles")
        conn.execute("DELETE FROM articles_fts")
        return count


def get_random_article() -> Optional[dict]:
    cols = ", ".join(_ARTICLE_META_COLS)
    with db_connection() as conn:
        row = conn.execute(
            f"SELECT {cols} FROM articles WHERE redirect_to IS NULL "
            f"ORDER BY RANDOM() LIMIT 1"
        ).fetchone()
    return _row_to_dict(row) if row else None


# ---------------------------------------------------------------------------
# Links
# ---------------------------------------------------------------------------

def resolve_links(limit: int = 5000) -> int:
    """Fill in to_id for unresolved links. Returns count resolved."""
    with db_connection() as conn:
        cur = conn.execute("""
            UPDATE links SET to_id = (
                SELECT id FROM articles WHERE title = links.to_title
            )
            WHERE to_id IS NULL
            LIMIT ?
        """, (limit,))
        return cur.rowcount


def get_links(title: str) -> list[dict]:
    """Outbound links from an article."""
    with db_connection() as conn:
        rows = conn.execute("""
            SELECT l.to_title, a.id as to_id, a.summary
            FROM links l
            JOIN articles src ON src.title=? AND src.id=l.from_id
            LEFT JOIN articles a ON a.id=l.to_id
            ORDER BY l.to_title
        """, (title,)).fetchall()
    return [{"to_title": r["to_title"], "to_id": r["to_id"], "summary": r["summary"]} for r in rows]


def get_backlinks(title: str, limit: int = 50, offset: int = 0) -> list[dict]:
    """Articles that link to the given article title."""
    with db_connection() as conn:
        rows = conn.execute("""
            SELECT src.id, src.title, src.summary
            FROM links l
            JOIN articles target ON target.title=? AND target.id=l.to_id
            JOIN articles src    ON src.id=l.from_id
            ORDER BY src.title
            LIMIT ? OFFSET ?
        """, (title, limit, offset)).fetchall()
    return [{"id": r["id"], "title": r["title"], "summary": r["summary"]} for r in rows]


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search_articles(
    q: Optional[str] = None,
    title: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    meta_cols = ", ".join(f"a.{c}" for c in _ARTICLE_META_COLS)

    if q:
        # FTS path
        with db_connection() as conn:
            rows = conn.execute(f"""
                SELECT {meta_cols},
                       bm25(articles_fts) AS score
                FROM articles_fts
                JOIN articles a ON a.id=articles_fts.rowid
                WHERE articles_fts MATCH :q
                  AND a.redirect_to IS NULL
                ORDER BY score
                LIMIT :lim OFFSET :off
            """, {"q": q, "lim": limit, "off": offset}).fetchall()
        results = []
        for r in rows:
            d = _row_to_dict(r)
            d["score"] = r["score"]
            results.append(d)
        return results

    # Non-FTS: title prefix filter
    clauses = ["a.redirect_to IS NULL"]
    params: list = []
    if title:
        clauses.append("a.title LIKE ? ESCAPE '\\'")
        params.append(title.replace("%", "\\%").replace("_", "\\_") + "%")
    where = " AND ".join(clauses)
    params += [limit, offset]
    with db_connection() as conn:
        rows = conn.execute(
            f"SELECT {meta_cols} FROM articles a WHERE {where} "
            f"ORDER BY a.title LIMIT ? OFFSET ?",
            params,
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def get_status() -> dict:
    with db_connection() as conn:
        row = conn.execute("""
            SELECT
                SUM(redirect_to IS NULL)     AS total_articles,
                SUM(redirect_to IS NOT NULL) AS total_redirects,
                (SELECT COUNT(*) FROM links)                     AS total_links,
                (SELECT COUNT(*) FROM links WHERE to_id IS NULL)  AS unresolved_links
            FROM articles
        """).fetchone()
    return {
        "total_articles":   row["total_articles"]   or 0,
        "total_redirects":  row["total_redirects"]  or 0,
        "total_links":      row["total_links"]       or 0,
        "unresolved_links": row["unresolved_links"]  or 0,
    }
