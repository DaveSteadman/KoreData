import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

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
                sections    TEXT,
                categories  TEXT,
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                name  TEXT    NOT NULL UNIQUE,
                count INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS article_categories (
                article_id  INTEGER NOT NULL REFERENCES articles(id)  ON DELETE CASCADE,
                category_id INTEGER NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
                PRIMARY KEY (article_id, category_id)
            )
        """)
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS articles_fts USING fts5(
                title, body,
                tokenize='unicode61 remove_diacritics 1',
                content=articles,
                content_rowid=id
            )
        """)
        # FTS sync triggers
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS articles_ai AFTER INSERT ON articles BEGIN
                INSERT INTO articles_fts(rowid, title, body)
                VALUES (new.id, COALESCE(new.title,''), COALESCE(new.body,''));
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS articles_ad AFTER DELETE ON articles BEGIN
                INSERT INTO articles_fts(articles_fts, rowid, title, body)
                VALUES ('delete', old.id, COALESCE(old.title,''), COALESCE(old.body,''));
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS articles_au AFTER UPDATE ON articles BEGIN
                INSERT INTO articles_fts(articles_fts, rowid, title, body)
                VALUES ('delete', old.id, COALESCE(old.title,''), COALESCE(old.body,''));
                INSERT INTO articles_fts(rowid, title, body)
                VALUES (new.id, COALESCE(new.title,''), COALESCE(new.body,''));
            END
        """)
        # Back-fill FTS for any rows that pre-date triggers
        conn.execute("""
            INSERT INTO articles_fts(rowid, title, body)
            SELECT a.id, COALESCE(a.title,''), COALESCE(a.body,'')
            FROM articles a
            WHERE a.id NOT IN (SELECT rowid FROM articles_fts)
        """)
        # Migrate: add facts column if not present (for databases created before this feature)
        _cols = {row[1] for row in conn.execute("PRAGMA table_info(articles)")}
        if "facts" not in _cols:
            conn.execute("ALTER TABLE articles ADD COLUMN facts TEXT")
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


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
    "id", "title", "redirect_to", "summary", "categories", "word_count",
)
_ARTICLE_FULL_COLS = _ARTICLE_META_COLS + ("body", "sections", "facts")


def _row_to_dict(row: sqlite3.Row, full: bool = False) -> dict:
    cols = _ARTICLE_FULL_COLS if full else _ARTICLE_META_COLS
    d = {c: row[c] for c in cols}
    d["categories"] = _parse_json_list(d.get("categories"))
    if full:
        d["sections"] = _parse_json_list(d.get("sections"))
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


def resolve_article(title: str, _depth: int = 0) -> Optional[dict]:
    """Fetch article, following up to 5 levels of redirect."""
    if _depth > 5:
        return None
    article = get_article_by_title(title, full=True)
    if article is None:
        return None
    if article["redirect_to"]:
        target = resolve_article(article["redirect_to"], _depth + 1)
        if target:
            target["redirected_from"] = title
            return target
        # Redirect target doesn't exist — return the article itself so it remains
        # accessible rather than silently 404ing on a broken redirect.
    return article


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
    sections: Optional[list] = None,
    categories: Optional[list] = None,
    facts: Optional[list] = None,
    redirect_to: Optional[str] = None,
    link_titles: Optional[list[str]] = None,
    **_ignored,
) -> dict:
    """Insert or update an article."""
    title = title.strip()
    wc = _word_count(body)
    sections_json = json.dumps(sections or [])
    categories_json = json.dumps(categories or [])
    facts_json = json.dumps(facts or [])

    with db_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM articles WHERE title = ?", (title,)
        ).fetchone()

        if existing:
            article_id = existing["id"]
            conn.execute("""
                UPDATE articles
                SET redirect_to=?, summary=?, body=?, sections=?, categories=?,
                    facts=?, word_count=?
                WHERE id=?
            """, (redirect_to, summary, body, sections_json, categories_json,
                  facts_json, wc, article_id))
            conn.execute("DELETE FROM links WHERE from_id=?", (article_id,))
        else:
            cur = conn.execute("""
                INSERT INTO articles
                    (title, redirect_to, summary, body, sections, categories,
                     facts, word_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (title, redirect_to, summary, body, sections_json, categories_json,
                  facts_json, wc))
            article_id = cur.lastrowid

        # Insert links (to_id resolved later)
        for lt in (link_titles or []):
            conn.execute(
                "INSERT INTO links (from_id, to_title) VALUES (?, ?)",
                (article_id, lt),
            )

        # Sync categories
        _sync_categories(conn, article_id, categories or [])

    return get_article_by_id(article_id, full=False)


def _sync_categories(conn: sqlite3.Connection, article_id: int, categories: list[str]) -> None:
    """Upsert categories and link them to the article."""
    conn.execute("DELETE FROM article_categories WHERE article_id=?", (article_id,))
    for name in categories:
        conn.execute(
            "INSERT OR IGNORE INTO categories (name, count) VALUES (?, 0)", (name,)
        )
        conn.execute(
            "INSERT OR IGNORE INTO article_categories (article_id, category_id) "
            "SELECT ?, id FROM categories WHERE name=?",
            (article_id, name),
        )
    # Recompute counts for affected categories
    conn.execute("""
        UPDATE categories SET count = (
            SELECT COUNT(*) FROM article_categories
            WHERE category_id = categories.id
        )
    """)


def delete_article(title: str) -> bool:
    with db_connection() as conn:
        cur = conn.execute("DELETE FROM articles WHERE title=?", (title,))
        return cur.rowcount > 0


def delete_all_articles() -> int:
    """Delete every article row. Returns number of rows deleted."""
    with db_connection() as conn:
        cur = conn.execute("DELETE FROM articles")
        return cur.rowcount


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
# Categories
# ---------------------------------------------------------------------------

def list_categories() -> list[dict]:
    with db_connection() as conn:
        rows = conn.execute(
            "SELECT id, name, count FROM categories ORDER BY name"
        ).fetchall()
    return [dict(r) for r in rows]


def get_category_articles(name: str, limit: int = 100, offset: int = 0) -> list[dict]:
    cols = ", ".join(f"a.{c}" for c in _ARTICLE_META_COLS)
    with db_connection() as conn:
        rows = conn.execute(f"""
            SELECT {cols}
            FROM article_categories ac
            JOIN categories   c ON c.name=? AND c.id=ac.category_id
            JOIN articles     a ON a.id=ac.article_id
            WHERE a.redirect_to IS NULL
            ORDER BY a.title
            LIMIT ? OFFSET ?
        """, (name, limit, offset)).fetchall()
    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search_articles(
    q: Optional[str] = None,
    title: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    meta_cols = ", ".join(f"a.{c}" for c in _ARTICLE_META_COLS)

    if q:
        # FTS path
        with db_connection() as conn:
            cat_join = (
                "JOIN article_categories ac ON ac.article_id=a.id "
                "JOIN categories cat ON cat.id=ac.category_id AND cat.name=:cat "
                if category else ""
            )
            rows = conn.execute(f"""
                SELECT {meta_cols},
                       snippet(articles_fts, 1, '<b>', '</b>', '…', 20) AS snippet,
                       bm25(articles_fts) AS score
                FROM articles_fts
                JOIN articles a ON a.id=articles_fts.rowid
                {cat_join}
                WHERE articles_fts MATCH :q
                  AND a.redirect_to IS NULL
                ORDER BY score
                LIMIT :lim OFFSET :off
            """, {"q": q, "cat": category, "lim": limit, "off": offset}).fetchall()
        results = []
        for r in rows:
            d = _row_to_dict(r)
            d["snippet"] = r["snippet"]
            d["score"] = r["score"]
            results.append(d)
        return results

    # Non-FTS: title prefix and/or category filter
    clauses = ["a.redirect_to IS NULL"]
    params: list = []
    joins = ""
    if title:
        clauses.append("a.title LIKE ? ESCAPE '\\'")
        params.append(title.replace("%", "\\%").replace("_", "\\_") + "%")
    if category:
        joins = (
            "JOIN article_categories ac ON ac.article_id=a.id "
            "JOIN categories cat ON cat.id=ac.category_id "
        )
        clauses.append("cat.name=?")
        params.append(category)
    where = " AND ".join(clauses)
    params += [limit, offset]
    with db_connection() as conn:
        rows = conn.execute(
            f"SELECT {meta_cols} FROM articles a {joins} WHERE {where} "
            f"ORDER BY a.title LIMIT ? OFFSET ?",
            params,
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def get_status() -> dict:
    with db_connection() as conn:
        total_articles = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE redirect_to IS NULL"
        ).fetchone()[0]
        total_redirects = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE redirect_to IS NOT NULL"
        ).fetchone()[0]
        total_links = conn.execute("SELECT COUNT(*) FROM links").fetchone()[0]
        unresolved_links = conn.execute(
            "SELECT COUNT(*) FROM links WHERE to_id IS NULL"
        ).fetchone()[0]
        total_categories = conn.execute(
            "SELECT COUNT(*) FROM categories"
        ).fetchone()[0]
    return {
        "total_articles":   total_articles,
        "total_redirects":  total_redirects,
        "total_links":      total_links,
        "unresolved_links": unresolved_links,
        "total_categories": total_categories,
    }
