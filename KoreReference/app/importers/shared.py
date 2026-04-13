import re
from copy import deepcopy
from typing import Optional

from bs4 import BeautifulSoup

_PUNCT_SPACE_RE = re.compile(r' +([,\.;:!?])')
_EDIT_RE = re.compile(r'\s*\[edit\]\s*$', re.IGNORECASE)

TABLE_OPEN  = "<<<TABLE>>>"
TABLE_CLOSE = "<<<ENDTABLE>>>"
_ALLOWED_TABLE_TAGS  = frozenset({"table", "thead", "tbody", "tfoot", "tr", "th", "td", "caption"})
_ALLOWED_TABLE_ATTRS = frozenset({"colspan", "rowspan", "scope"})

_NOISE_SELECTOR = (
    "sup, .reference, .reflist, .navbox, .sidebar, "
    ".thumb, .gallery, .mw-editsection, #toc, "
    ".hatnote, .noprint, style, script, "
    ".redirectMsg, .mw-redirectedfrom"
)


def fix_spacing(text: str) -> str:
    """Remove spurious spaces before punctuation introduced by get_text(separator=' ')."""
    return _PUNCT_SPACE_RE.sub(r'\1', text)


_TAG_RE = re.compile(r'<[^>]+>')


def _clean_table(tbl_tag) -> str:
    """Return structural-only HTML for a table: no class, style, or id attributes."""
    tbl = deepcopy(tbl_tag)
    # Process root element AND all descendants (find_all skips the root itself)
    for tag in [tbl] + tbl.find_all(True):
        if tag.name not in _ALLOWED_TABLE_TAGS:
            tag.unwrap()
            continue
        for attr in list(tag.attrs.keys()):
            if attr not in _ALLOWED_TABLE_ATTRS:
                del tag.attrs[attr]
    return str(tbl)


def table_to_fts_text(table_html: str) -> str:
    """Strip all HTML tags from a table block, leaving only cell text for FTS indexing."""
    return _TAG_RE.sub(' ', table_html)


def _inside_table(el) -> bool:
    return any(p.name == "table" for p in el.parents)


def _inside_list(el) -> bool:
    return any(p.name in ("ul", "ol") for p in el.parents)


def remove_noise(soup: BeautifulSoup) -> None:
    """Decompose navigation, reference, caption, and other non-content tags in-place."""
    for tag in soup.select(_NOISE_SELECTOR):
        tag.decompose()


def extract_facts(soup: BeautifulSoup) -> list[list[str]]:
    """Extract key-value fact rows from infoboxes, then decompose them from the tree."""
    facts: list[list[str]] = []
    for infobox in soup.select(".infobox"):
        for row in infobox.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                label = fix_spacing(th.get_text(" ", strip=True))
                value = fix_spacing(td.get_text(" ", strip=True))
                if label and value:
                    facts.append([label, value])
        infobox.decompose()
    return facts


def _serialize_list(el, depth: int = 0) -> list[str]:
    """Recursively serialize a ul/ol element to indented '* '/'# '-prefixed lines."""
    marker = "  " * depth + ("* " if el.name == "ul" else "# ")
    lines: list[str] = []
    for li in el.find_all("li", recursive=False):
        li_copy = deepcopy(li)
        for sub in li_copy.find_all(["ul", "ol"]):
            sub.decompose()
        item_text = fix_spacing(li_copy.get_text(separator=" ", strip=True))
        if item_text:
            lines.append(marker + item_text)
        for sub in li.find_all(["ul", "ol"], recursive=False):
            lines.extend(_serialize_list(sub, depth + 1))
    return lines


def extract_article_html(content_div) -> tuple[str, Optional[str]]:
    """Build body text and summary from a BeautifulSoup content element.

    Encodes headings inline as ``== Heading ==`` so that sections can be
    derived from body at read time — avoiding duplicate storage.

    Caller must have already:
      - Removed noise tags via remove_noise()
      - Rewritten internal <a> hrefs to [[wikilink]] markup
      - Extracted and decomposed infoboxes via extract_facts()
    """
    body_parts: list[str] = []
    for el in content_div.find_all(["p", "ul", "ol", "h2", "h3", "h4", "table"]):
        if _inside_table(el):
            continue
        if el.name in ("h2", "h3", "h4"):
            heading = _EDIT_RE.sub('', el.get_text(strip=True))
            body_parts.append(f"== {heading} ==")
        elif el.name == "table":
            body_parts.append(f"{TABLE_OPEN}{_clean_table(el)}{TABLE_CLOSE}")
        elif el.name in ("ul", "ol"):
            if _inside_list(el):
                continue
            items = _serialize_list(el)
            if items:
                body_parts.append("\n".join(items))
        else:  # p
            if _inside_list(el):
                continue  # content captured via parent li.get_text()
            text = fix_spacing(el.get_text(separator=" ", strip=True))
            if text:
                body_parts.append(text)
    body = "\n\n".join(body_parts)

    summary: Optional[str] = None
    for el in content_div.find_all("p"):
        text = fix_spacing(el.get_text(separator=" ", strip=True))
        if text:
            summary = text
            break

    return body, summary
