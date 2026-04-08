import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from app.config import cfg

DATA_DIR = Path(cfg["data_dir"])
_DB_PATH = DATA_DIR / "library.db"

# Fields that are checked for completeness (NULL or empty = incomplete)
COMPLETENESS_FIELDS = ("author", "year", "language", "genre")


def get_db_path() -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    return _DB_PATH


@contextmanager
def db_connection():
    conn = sqlite3.connect(str(get_db_path()), check_same_thread=False)
    conn.row_factory = sqlite3.Row
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
            CREATE TABLE IF NOT EXISTS books (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT    NOT NULL,
                author      TEXT,
                year        INTEGER,
                language    TEXT,
                genre       TEXT,
                notes       TEXT,
                word_count  INTEGER,
                body        TEXT
            )
        """)
        # Migration: drop source-based unique index and legacy metadata columns
        try:
            conn.execute("DROP INDEX IF EXISTS idx_books_source_id")
        except Exception:
            pass
        for col in ("source", "source_id", "added_at", "updated_at"):
            try:
                conn.execute(f"ALTER TABLE books DROP COLUMN {col}")
            except Exception:
                pass
        conn.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS books_fts USING fts5(
                title, author, body,
                tokenize='unicode61 remove_diacritics 1',
                content=books,
                content_rowid=id
            )
        """)
        # Triggers to keep FTS in sync
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS books_ai AFTER INSERT ON books BEGIN
                INSERT INTO books_fts(rowid, title, author, body)
                VALUES (new.id, COALESCE(new.title,''), COALESCE(new.author,''), COALESCE(new.body,''));
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS books_ad AFTER DELETE ON books BEGIN
                INSERT INTO books_fts(books_fts, rowid, title, author, body)
                VALUES ('delete', old.id, COALESCE(old.title,''), COALESCE(old.author,''), COALESCE(old.body,''));
            END
        """)
        conn.execute("""
            CREATE TRIGGER IF NOT EXISTS books_au AFTER UPDATE ON books BEGIN
                INSERT INTO books_fts(books_fts, rowid, title, author, body)
                VALUES ('delete', old.id, COALESCE(old.title,''), COALESCE(old.author,''), COALESCE(old.body,''));
                INSERT INTO books_fts(rowid, title, author, body)
                VALUES (new.id, COALESCE(new.title,''), COALESCE(new.author,''), COALESCE(new.body,''));
            END
        """)
        # Back-fill FTS for any existing rows (e.g. if DB pre-dates triggers)
        conn.execute("""
            INSERT INTO books_fts(rowid, title, author, body)
            SELECT b.id, COALESCE(b.title,''), COALESCE(b.author,''), COALESCE(b.body,'')
            FROM books b
            WHERE b.id NOT IN (SELECT rowid FROM books_fts)
        """)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_page_markers(text: str) -> str:
    """Remove Gutenberg page-break markers like {1}, {vii}, {ix} etc."""
    return re.sub(r"\{[ivxlcdmIVXLCDM\d]+\}", "", text)


def _compute_word_count(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    return len(text.split())


_BOOK_COLS = (
    "id", "title", "author", "year", "language", "genre",
    "notes", "word_count",
)

_BOOK_COLS_WITH_BODY = _BOOK_COLS + ("body",)


def _row_to_dict(row: sqlite3.Row, include_body: bool = False) -> dict:
    cols = _BOOK_COLS_WITH_BODY if include_body else _BOOK_COLS
    return {c: row[c] for c in cols}


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def add_book(
    title: str,
    body: Optional[str],
    author: Optional[str] = None,
    year: Optional[int] = None,
    language: Optional[str] = None,
    genre: Optional[str] = None,
    notes: Optional[str] = None,
    **_ignored,
) -> dict:
    cleaned_body = _strip_page_markers(body) if body else None
    word_count = _compute_word_count(cleaned_body)
    with db_connection() as conn:
        cur = conn.execute("""
            INSERT INTO books (title, author, year, language, genre, notes,
                               word_count, body)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (title, author, year, language, genre, notes,
              word_count, cleaned_body))
        book_id = cur.lastrowid
    return get_book(book_id, include_body=False)


def get_book(book_id: int, include_body: bool = True) -> Optional[dict]:
    cols = ", ".join(_BOOK_COLS_WITH_BODY if include_body else _BOOK_COLS)
    with db_connection() as conn:
        row = conn.execute(
            f"SELECT {cols} FROM books WHERE id = ?", (book_id,)
        ).fetchone()
    return _row_to_dict(row, include_body=include_body) if row else None


def update_book_body(book_id: int, body: str) -> Optional[dict]:
    cleaned = _strip_page_markers(body) if body else None
    word_count = _compute_word_count(cleaned)
    with db_connection() as conn:
        conn.execute(
            "UPDATE books SET body = ?, word_count = ? WHERE id = ?",
            (cleaned, word_count, book_id),
        )
    return get_book(book_id, include_body=False)


def list_books(
    limit: int = 100,
    offset: int = 0,
) -> list[dict]:
    cols = ", ".join(_BOOK_COLS)
    with db_connection() as conn:
        rows = conn.execute(
            f"SELECT {cols} FROM books ORDER BY title LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def update_book(book_id: int, fields: dict) -> Optional[dict]:
    """Partial update. Only provided keys are written."""
    allowed = {
        "title", "author", "year", "language", "genre",
        "notes", "body",
    }
    to_set = {k: v for k, v in fields.items() if k in allowed}
    if not to_set:
        return get_book(book_id, include_body=False)

    if "body" in to_set:
        to_set["body"] = _strip_page_markers(to_set["body"]) if to_set["body"] else None
        to_set["word_count"] = _compute_word_count(to_set["body"])

    assignments = ", ".join(f"{k} = ?" for k in to_set)
    values = list(to_set.values())
    values.append(book_id)

    with db_connection() as conn:
        conn.execute(
            f"UPDATE books SET {assignments} WHERE id = ?", values
        )
    return get_book(book_id, include_body=False)


def delete_book(book_id: int) -> bool:
    with db_connection() as conn:
        cur = conn.execute("DELETE FROM books WHERE id = ?", (book_id,))
    return cur.rowcount > 0


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

def search_books(
    q: Optional[str] = None,
    author: Optional[str] = None,
    title: Optional[str] = None,
    year: Optional[int] = None,
    language: Optional[str] = None,
    genre: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    cols = ", ".join(f"b.{c}" for c in _BOOK_COLS)
    params: list = []

    if q:
        # FTS path — join on FTS table, return BM25-ranked snippets
        snippet_col = (
            "snippet(books_fts, 2, '[', ']', '...', 32) AS snippet"
        )
        sql = f"""
            SELECT {cols}, {snippet_col}
            FROM books_fts
            JOIN books b ON b.id = books_fts.rowid
            WHERE books_fts MATCH ?
        """
        params.append(q)
        filters, filter_params = _build_meta_filters(
            author, title, year, language, genre, table_prefix="b"
        )
        if filters:
            sql += " AND " + " AND ".join(filters)
            params.extend(filter_params)
        sql += " ORDER BY rank LIMIT ? OFFSET ?"
        params += [limit, offset]
    else:
        # Metadata-only path
        snippet_col = "NULL AS snippet"
        sql = f"SELECT {cols}, {snippet_col} FROM books b WHERE 1=1"
        filters, filter_params = _build_meta_filters(
            author, title, year, language, genre, table_prefix="b"
        )
        if filters:
            sql += " AND " + " AND ".join(filters)
            params.extend(filter_params)
        sql += " ORDER BY b.title LIMIT ? OFFSET ?"
        params += [limit, offset]

    with db_connection() as conn:
        rows = conn.execute(sql, params).fetchall()

    result = []
    for row in rows:
        d = _row_to_dict(row, include_body=False)
        d["snippet"] = row["snippet"]
        result.append(d)
    return result


def _build_meta_filters(
    author, title, year, language, genre, table_prefix: str = ""
) -> tuple[list[str], list]:
    prefix = f"{table_prefix}." if table_prefix else ""
    filters: list[str] = []
    params: list = []
    if author:
        filters.append(f"{prefix}author LIKE ?")
        params.append(f"%{author}%")
    if title:
        filters.append(f"{prefix}title LIKE ?")
        params.append(f"%{title}%")
    if year is not None:
        filters.append(f"{prefix}year = ?")
        params.append(year)
    if language:
        filters.append(f"{prefix}language = ?")
        params.append(language)
    if genre:
        filters.append(f"{prefix}genre LIKE ?")
        params.append(f"%{genre}%")
    return filters, params


# ---------------------------------------------------------------------------
# Incomplete records
# ---------------------------------------------------------------------------

def list_incomplete(fields: Optional[list[str]] = None) -> list[dict]:
    """Return books with NULL/empty values in completeness fields."""
    check = [f for f in (fields or list(COMPLETENESS_FIELDS))
             if f in COMPLETENESS_FIELDS]
    if not check:
        check = list(COMPLETENESS_FIELDS)

    conditions = " OR ".join(
        f"({f} IS NULL OR {f} = '')" for f in check
    )
    cols = ", ".join(_BOOK_COLS)
    with db_connection() as conn:
        rows = conn.execute(
            f"SELECT {cols} FROM books WHERE {conditions} ORDER BY title"
        ).fetchall()
    result = []
    for row in rows:
        d = _row_to_dict(row, include_body=False)
        d["missing_fields"] = [
            f for f in COMPLETENESS_FIELDS
            if not row[f]
        ]
        result.append(d)
    return result


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def get_status() -> dict:
    with db_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM books").fetchone()[0]
        incomplete = conn.execute(
            "SELECT COUNT(*) FROM books WHERE "
            "author IS NULL OR author = '' OR "
            "year IS NULL OR "
            "language IS NULL OR language = '' OR "
            "genre IS NULL OR genre = ''"
        ).fetchone()[0]
        no_body = conn.execute(
            "SELECT COUNT(*) FROM books WHERE body IS NULL OR body = ''"
        ).fetchone()[0]
    db_size = get_db_path().stat().st_size if get_db_path().exists() else 0
    return {
        "total_books": total,
        "incomplete_records": incomplete,
        "books_without_body": no_body,
        "db_size_bytes": db_size,
    }


def title_exists(title: str) -> bool:
    with db_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM books WHERE title = ? LIMIT 1",
            (title,),
        ).fetchone()
    return row is not None
