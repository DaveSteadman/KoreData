import sqlite3
import zlib
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from app.config import cfg


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


DATA_DIR = Path(cfg["data_dir"])
_DB_PATH = DATA_DIR / "rag.db"


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


def init_db() -> None:
    with db_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT,
                source      TEXT,
                tags        TEXT,
                content     BLOB,
                word_count  INTEGER,
                created_at  TEXT DEFAULT (datetime('now','utc'))
            )
        """)
        # Contentless FTS5 — Python CRUD manages FTS explicitly with plain text.
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
                title, source, tags, content,
                tokenize='unicode61 remove_diacritics 1',
                content=''
            )
        """)


def _compute_word_count(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    return len(text.split())


_CHUNK_COLS = ("id", "title", "source", "tags", "word_count", "created_at")
_CHUNK_COLS_WITH_CONTENT = _CHUNK_COLS + ("content",)


def _row_to_dict(row: sqlite3.Row, include_content: bool = False) -> dict:
    cols = _CHUNK_COLS_WITH_CONTENT if include_content else _CHUNK_COLS
    d = {c: row[c] for c in cols}
    if include_content:
        d["content"] = _decompress(d.get("content"))
    return d


def add_chunk(
    content: str,
    title: Optional[str] = None,
    source: Optional[str] = None,
    tags: Optional[str] = None,
) -> dict:
    word_count = _compute_word_count(content)
    compressed = _compress(content)
    with db_connection() as conn:
        cur = conn.execute(
            "INSERT INTO chunks (title, source, tags, content, word_count) VALUES (?, ?, ?, ?, ?)",
            (title, source, tags, compressed, word_count),
        )
        chunk_id = cur.lastrowid
        conn.execute(
            "INSERT INTO chunks_fts(rowid, title, source, tags, content) VALUES (?, ?, ?, ?, ?)",
            (chunk_id, title or "", source or "", tags or "", content or ""),
        )
    return get_chunk(chunk_id, include_content=False)


def get_chunk(chunk_id: int, include_content: bool = True) -> Optional[dict]:
    cols = ", ".join(_CHUNK_COLS_WITH_CONTENT if include_content else _CHUNK_COLS)
    with db_connection() as conn:
        row = conn.execute(
            f"SELECT {cols} FROM chunks WHERE id = ?", (chunk_id,)
        ).fetchone()
    return _row_to_dict(row, include_content=include_content) if row else None


def list_chunks(limit: int = 100, offset: int = 0) -> list[dict]:
    cols = ", ".join(_CHUNK_COLS)
    with db_connection() as conn:
        rows = conn.execute(
            f"SELECT {cols} FROM chunks ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_chunk(chunk_id: int, fields: dict) -> Optional[dict]:
    """Partial update. Only provided keys are written."""
    allowed = {"title", "source", "tags", "content"}
    to_set = {k: v for k, v in fields.items() if k in allowed}
    if not to_set:
        return get_chunk(chunk_id, include_content=False)

    plain_content: Optional[str] = None
    if "content" in to_set:
        plain_content = to_set["content"]
        to_set["content"] = _compress(plain_content)
        to_set["word_count"] = _compute_word_count(plain_content)

    fts_affected = bool({"title", "source", "tags", "content"} & to_set.keys())
    assignments = ", ".join(f"{k} = ?" for k in to_set)
    values = list(to_set.values())

    with db_connection() as conn:
        if fts_affected:
            cur_row = conn.execute(
                "SELECT title, source, tags, content FROM chunks WHERE id = ?", (chunk_id,)
            ).fetchone()
            if cur_row:
                conn.execute(
                    "INSERT INTO chunks_fts(chunks_fts, rowid, title, source, tags, content) "
                    "VALUES ('delete', ?, ?, ?, ?, ?)",
                    (chunk_id,
                     cur_row["title"] or "",
                     cur_row["source"] or "",
                     cur_row["tags"] or "",
                     _decompress(cur_row["content"]) or ""),
                )
        conn.execute(
            f"UPDATE chunks SET {assignments} WHERE id = ?", values + [chunk_id]
        )
        if fts_affected:
            new_row = conn.execute(
                "SELECT title, source, tags, content FROM chunks WHERE id = ?", (chunk_id,)
            ).fetchone()
            if new_row:
                resolved_content = (
                    plain_content if plain_content is not None
                    else _decompress(new_row["content"]) or ""
                )
                conn.execute(
                    "INSERT INTO chunks_fts(rowid, title, source, tags, content) VALUES (?, ?, ?, ?, ?)",
                    (chunk_id,
                     new_row["title"] or "",
                     new_row["source"] or "",
                     new_row["tags"] or "",
                     resolved_content),
                )
    return get_chunk(chunk_id, include_content=False)


def delete_chunk(chunk_id: int) -> bool:
    with db_connection() as conn:
        row = conn.execute(
            "SELECT title, source, tags, content FROM chunks WHERE id = ?", (chunk_id,)
        ).fetchone()
        if not row:
            return False
        conn.execute(
            "INSERT INTO chunks_fts(chunks_fts, rowid, title, source, tags, content) "
            "VALUES ('delete', ?, ?, ?, ?, ?)",
            (chunk_id,
             row["title"] or "",
             row["source"] or "",
             row["tags"] or "",
             _decompress(row["content"]) or ""),
        )
        conn.execute("DELETE FROM chunks WHERE id = ?", (chunk_id,))
    return True


def search_chunks(
    q: str,
    limit: int = 20,
    source: Optional[str] = None,
    tags: Optional[str] = None,
) -> list[dict]:
    """FTS search returning metadata + snippet from the best-matching content."""
    cols = ", ".join(f"c.{col}" for col in _CHUNK_COLS)
    snippet_col = "snippet(chunks_fts, 3, '[', ']', '...', 32) AS snippet"

    sql = f"""
        SELECT {cols}, {snippet_col}
        FROM chunks_fts
        JOIN chunks c ON c.id = chunks_fts.rowid
        WHERE chunks_fts MATCH ?
    """
    params: list = [q]

    if source:
        sql += " AND c.source LIKE ?"
        params.append(f"%{source}%")
    if tags:
        sql += " AND c.tags LIKE ?"
        params.append(f"%{tags}%")

    sql += " ORDER BY rank LIMIT ?"
    params.append(limit)

    with db_connection() as conn:
        rows = conn.execute(sql, params).fetchall()

    results = []
    for row in rows:
        d = {c: row[c] for c in _CHUNK_COLS}
        d["snippet"] = row["snippet"]
        results.append(d)
    return results


def get_status() -> dict:
    with db_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    db_path = get_db_path()
    db_size = db_path.stat().st_size if db_path.exists() else 0
    return {
        "total_chunks": total,
        "db_size_bytes": db_size,
    }
