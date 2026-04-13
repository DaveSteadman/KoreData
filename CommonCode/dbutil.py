import re
from typing import Optional


def fts_build_query(q: str) -> str:
    """Convert a raw user search string into a safe FTS5 MATCH expression.

    Each whitespace/comma-separated token is wrapped in double-quotes so that
    FTS5 treats it as a literal phrase rather than interpreting special syntax
    characters (AND, OR, NOT, *, ^, parentheses, unmatched quotes, etc.).
    Tokens are combined with implicit AND (FTS5 default).
    Returns an empty string if the input contains no usable tokens.
    """
    terms = [t for t in re.split(r"[\s,]+", (q or "").strip()) if t]
    return " ".join('"' + t.replace('"', '""') + '"' for t in terms)


def compute_word_count(text: Optional[str]) -> Optional[int]:
    """Return the number of whitespace-separated words in *text*, or None if empty."""
    if not text:
        return None
    return len(text.split())
