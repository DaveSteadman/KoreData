import re
from typing import Optional


def fts_build_query(q: str) -> str:
    """Convert a raw user search string into a safe FTS5 MATCH expression.

    Quoted substrings are treated as phrase searches (words must appear
    consecutively in that order). Unquoted words are individual AND terms.
    All tokens are combined with implicit AND (FTS5 default).

    Examples:
        "art of war"          → "art of war"          (exact phrase)
        art of war            → "art" "of" "war"       (all three words, any order)
        sun tzu "art of war"  → "sun" "tzu" "art of war"
    """
    parts: list[str] = []
    for m in re.finditer(r'"([^"]+)"|(\S+)', (q or "").strip()):
        phrase = m.group(1)
        word   = m.group(2)
        if phrase:
            # Quoted phrase — pass through as a single FTS5 phrase
            inner = phrase.strip().replace('"', '""')
            if inner:
                parts.append(f'"{inner}"')
        elif word:
            # Bare word — strip any stray quotes and emit individually
            clean = word.replace('"', '')
            if clean:
                parts.append(f'"{clean}"')
    return " ".join(parts)


def compute_word_count(text: Optional[str]) -> Optional[int]:
    """Return the number of whitespace-separated words in *text*, or None if empty."""
    if not text:
        return None
    return len(text.split())
