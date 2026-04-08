import sqlite3
import json
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from email.utils import parsedate as _rfc_parsedate
from pathlib import Path
from typing import Optional

from app.config import cfg

DATA_DIR = Path(cfg["data_dir"])


def _parse_published(s: str) -> Optional[datetime]:
    """Parse an RSS date string to a naive UTC datetime. Returns None on failure."""
    if not s:
        return None
    # RFC 2822 (most common in RSS feeds)
    try:
        t = _rfc_parsedate(s)
        if t:
            return datetime(*t[:6])
    except Exception:
        pass
    # ISO 8601 / Atom  (e.g. "2026-01-09T12:00:00Z" or stored by newer ingest)
    try:
        return datetime.fromisoformat(s.rstrip("Z").replace("T", " ")[:19])
    except Exception:
        pass
    return None


def _sanitize_domain(domain: str) -> str:
    """Strip path traversal characters; allow only word chars and hyphens."""
    return re.sub(r"[^\w\-]", "_", domain)


def get_db_path(domain: str) -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    return DATA_DIR / f"{_sanitize_domain(domain)}.db"


@contextmanager
def db_connection(domain: str):
    conn = sqlite3.connect(str(get_db_path(domain)), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(domain: str) -> None:
    with db_connection(domain) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS entries (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                feed_name    TEXT NOT NULL,
                headline     TEXT,
                url          TEXT UNIQUE,
                published    TEXT,
                metadata     TEXT,
                page_text    TEXT,
                ingested_at  TEXT DEFAULT (datetime('now')),
                deleted      INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS domain_settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        # migrate existing databases that pre-date the deleted column
        cols = {row[1] for row in conn.execute("PRAGMA table_info(entries)").fetchall()}
        if "deleted" not in cols:
            conn.execute("ALTER TABLE entries ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0")
        # normalise any published values not yet in UTC YYYY-MM-DD HH:MM:SS
        _normalise_published(conn)

        # FTS5 virtual table for word-boundary search + BM25 ranking
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
                headline, page_text,
                tokenize='unicode61 remove_diacritics 1'
            )
        """)
        # Back-fill any existing entries not yet in the FTS index
        conn.execute("""
            INSERT INTO entries_fts(rowid, headline, page_text)
            SELECT e.id, COALESCE(e.headline, ''), COALESCE(e.page_text, '')
            FROM entries e
            WHERE e.deleted = 0
              AND e.id NOT IN (SELECT rowid FROM entries_fts)
        """)


def _normalise_published(conn: sqlite3.Connection) -> None:
    """Rewrite existing published values to UTC 'YYYY-MM-DD HH:MM:SS' for consistent sorting."""
    rows = conn.execute(
        "SELECT id, published FROM entries WHERE published IS NOT NULL AND published != ''"
    ).fetchall()
    updates = []
    for row in rows:
        raw = row["published"]
        # already canonical: starts with YYYY-MM-DD and has a space at position 10
        if len(raw) >= 19 and raw[4] == "-" and raw[7] == "-" and raw[10] == " ":
            continue
        dt = _parse_published(raw)
        if dt:
            updates.append((dt.strftime("%Y-%m-%d %H:%M:%S"), row["id"]))
    if updates:
        conn.executemany("UPDATE entries SET published = ? WHERE id = ?", updates)


def insert_entry(
    domain: str,
    feed_name: str,
    headline: str,
    url: str,
    published: str,
    metadata: dict,
    page_text: str,
) -> bool:
    with db_connection(domain) as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO entries
                (feed_name, headline, url, published, metadata, page_text)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (feed_name, headline, url, published, json.dumps(metadata), page_text),
        )
        # Only index rows that were actually inserted (not ignored duplicates)
        if cur.rowcount > 0 and cur.lastrowid:
            conn.execute(
                "INSERT INTO entries_fts(rowid, headline, page_text) VALUES (?, ?, ?)",
                (cur.lastrowid, headline or "", page_text or ""),
            )
            return True
        return False


def get_entries(domain: str, limit: int = 50, offset: int = 0) -> list[dict]:
    try:
        with db_connection(domain) as conn:
            rows = conn.execute(
                "SELECT * FROM entries WHERE deleted = 0 ORDER BY published DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def get_entry(domain: str, entry_id: int) -> Optional[dict]:
    try:
        with db_connection(domain) as conn:
            row = conn.execute(
                "SELECT * FROM entries WHERE id = ? AND deleted = 0", (entry_id,)
            ).fetchone()
            return dict(row) if row else None
    except Exception:
        return None


def _fts_term(t: str) -> str:
    """Wrap a user term for FTS5 MATCH — double-quotes escape special syntax."""
    return '"' + t.replace('"', '""') + '"'


def search_entries(
    domain: Optional[str],
    query: str,
    limit: int = 50,
    include_body: bool = False,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> list[dict]:
    body_col    = ", e.page_text" if include_body else ""
    domains     = [domain] if domain else list_domains()
    terms       = [t for t in re.split(r"[\s,]+", query.strip()) if t]
    if not terms:
        return []
    fts_query   = " ".join(_fts_term(t) for t in terms)  # AND by default in FTS5
    per_domain_cap = max(limit, 20)

    date_clauses = ""
    date_params: list = []
    if since:
        date_clauses += " AND e.published >= ?"
        date_params.append(since)
    if until:
        date_clauses += " AND e.published <= ?"
        date_params.append(until)

    results: list[dict] = []
    for d in domains:
        try:
            with db_connection(d) as conn:
                rows = conn.execute(
                    f"""
                    SELECT e.id, e.feed_name, e.headline, e.url, e.published,
                           e.ingested_at{body_col}, ? AS domain
                    FROM entries_fts f
                    JOIN entries e ON e.id = f.rowid
                    WHERE entries_fts MATCH ?
                      AND e.deleted = 0
                      {date_clauses}
                    ORDER BY f.rank, e.published DESC
                    LIMIT ?
                    """,
                    (d, fts_query, *date_params, per_domain_cap),
                ).fetchall()
                results.extend([dict(r) for r in rows])
        except Exception:
            pass
    results.sort(key=lambda r: r.get("published") or "", reverse=True)
    return results[:limit]


def get_recent_entries(
    domain: Optional[str],
    hours: float = 24.0,
    limit: int = 50,
) -> list[dict]:
    modifier       = f"-{hours} hours"
    domains        = [domain] if domain else list_domains()
    per_domain_cap = max(limit, 20)
    results: list[dict] = []
    for d in domains:
        try:
            with db_connection(d) as conn:
                rows = conn.execute(
                    """
                    SELECT id, feed_name, headline, url, published, ingested_at,
                           ? AS domain
                    FROM entries
                    WHERE deleted = 0 AND ingested_at >= datetime('now', ?)
                    ORDER BY published DESC LIMIT ?
                    """,
                    (d, modifier, per_domain_cap),
                ).fetchall()
                results.extend([dict(r) for r in rows])
        except Exception:
            pass
    results.sort(key=lambda r: r.get("published") or "", reverse=True)
    return results[:limit]


def list_domains() -> list[str]:
    DATA_DIR.mkdir(exist_ok=True)
    return [f.stem for f in sorted(DATA_DIR.glob("*.db"))]


def _tombstone(conn: sqlite3.Connection, where: str, params: list) -> int:
    """Soft-delete: blank content fields and set deleted=1. URL is preserved for dedup."""
    # Capture IDs before the update so we can remove them from the FTS index
    ids = [
        r[0] for r in conn.execute(
            f"SELECT id FROM entries WHERE deleted=0 AND {where}", params
        ).fetchall()
    ]
    cur = conn.execute(
        f"UPDATE entries SET headline=NULL, page_text=NULL, metadata=NULL, deleted=1"
        f" WHERE deleted=0 AND {where}",
        params,
    )
    for id_ in ids:
        conn.execute("DELETE FROM entries_fts WHERE rowid=?", (id_,))
    return cur.rowcount


def delete_entry(domain: str, entry_id: int) -> bool:
    """Soft-delete a single entry. Returns True if the row was tombstoned."""
    try:
        with db_connection(domain) as conn:
            return _tombstone(conn, "id = ?", [entry_id]) > 0
    except Exception:
        return False


def delete_entries_by_feed(domain: str, feed_name: str) -> int:
    """Soft-delete all entries from a specific feed. Returns count tombstoned."""
    try:
        with db_connection(domain) as conn:
            return _tombstone(conn, "feed_name = ?", [feed_name])
    except Exception:
        return 0


def delete_entries_older_than(domain: str, days: float) -> int:
    """Soft-delete entries whose *published* date is more than `days` days ago."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    try:
        with db_connection(domain) as conn:
            rows = conn.execute(
                "SELECT id, published FROM entries WHERE deleted = 0"
            ).fetchall()
        ids = [
            r["id"] for r in rows
            if (dt := _parse_published(r["published"])) is not None and dt < cutoff
        ]
        return delete_entries_by_ids(domain, ids)
    except Exception:
        return 0


def delete_entries_by_ids(domain: str, ids: list[int]) -> int:
    """Soft-delete multiple entries by ID list. Returns count tombstoned."""
    if not ids:
        return 0
    validated = [int(i) for i in ids]
    placeholders = ",".join("?" * len(validated))
    try:
        with db_connection(domain) as conn:
            return _tombstone(conn, f"id IN ({placeholders})", validated)
    except Exception:
        return 0


def get_entry_count(domain: str) -> int:
    try:
        with db_connection(domain) as conn:
            row = conn.execute("SELECT COUNT(*) FROM entries WHERE deleted = 0").fetchone()
            return row[0]
    except Exception:
        return 0


def get_feed_counts(domain: str) -> dict[str, int]:
    """Return {feed_name: entry_count} for all non-deleted entries in a domain."""
    try:
        with db_connection(domain) as conn:
            rows = conn.execute(
                "SELECT feed_name, COUNT(*) AS cnt FROM entries WHERE deleted = 0 GROUP BY feed_name"
            ).fetchall()
            return {r["feed_name"]: r["cnt"] for r in rows}
    except Exception:
        return {}


def get_domain_age_settings(domain: str) -> dict:
    """Return age-gating settings for a domain.

    Returns a dict with keys: mode ('none'|'days_previous'|'calendar_period'),
    days (int|None), start_date (str|None), end_date (str|None).
    """
    try:
        with db_connection(domain) as conn:
            rows = conn.execute(
                "SELECT key, value FROM domain_settings "
                "WHERE key IN ('age_mode','age_days','age_start','age_end','max_age_days')"
            ).fetchall()
            s = {r["key"]: r["value"] for r in rows}
            # backwards compat: migrate legacy max_age_days → days_previous
            if "age_mode" not in s and s.get("max_age_days"):
                return {
                    "mode": "days_previous",
                    "days": int(s["max_age_days"]),
                    "start_date": None,
                    "end_date": None,
                }
            mode = s.get("age_mode", "none") or "none"
            days = int(s["age_days"]) if s.get("age_days") else None
            return {
                "mode": mode,
                "days": days,
                "start_date": s.get("age_start"),
                "end_date": s.get("age_end"),
            }
    except Exception:
        return {"mode": "none", "days": None, "start_date": None, "end_date": None}


def set_domain_age_settings(
    domain: str,
    mode: str,
    days: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    """Persist age-gating settings for a domain."""
    init_db(domain)
    with db_connection(domain) as conn:
        conn.executemany(
            "INSERT OR REPLACE INTO domain_settings (key, value) VALUES (?, ?)",
            [
                ("age_mode", mode),
                ("age_days", str(days) if days else None),
                ("age_start", start_date),
                ("age_end", end_date),
                # clear legacy key so it doesn't cause confused fallback
                ("max_age_days", None),
            ],
        )


def delete_entries_outside_calendar(domain: str, start_date: str, end_date: str) -> int:
    """Soft-delete entries whose published date falls outside [start_date, end_date]."""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt   = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        with db_connection(domain) as conn:
            rows = conn.execute(
                "SELECT id, published FROM entries WHERE deleted = 0"
            ).fetchall()
        ids = []
        for r in rows:
            dt = _parse_published(r["published"])
            if dt is None or dt < start_dt or dt > end_dt:
                ids.append(r["id"])
        return delete_entries_by_ids(domain, ids)
    except Exception:
        return 0


def delete_domain_db(domain: str) -> bool:
    """Delete the SQLite database for a domain. Returns False if it didn't exist."""
    path = get_db_path(domain)
    if not path.exists():
        return False
    path.unlink()
    return True


def rename_domain_db(old: str, new: str) -> bool:
    """Rename the SQLite database file for a domain. Returns False if old didn't exist."""
    old_path = get_db_path(old)
    if not old_path.exists():
        return False
    new_path = get_db_path(new)
    old_path.rename(new_path)
    return True
