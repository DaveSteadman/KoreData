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
    "sup, .reference, .reflist, .navbox, "
    ".thumb, .gallery, .mw-editsection, #toc, "
    ".hatnote, .noprint, style, script, "
    ".redirectMsg, .mw-redirectedfrom"
)


def fix_spacing(text: str) -> str:
    """Remove spurious spaces before punctuation introduced by get_text(separator=' ')."""
    return _PUNCT_SPACE_RE.sub(r'\1', text)


def _clean_table(tbl_tag) -> str:
    """Return structural-only HTML for a table: no class, style, or id attributes."""
    tbl = deepcopy(tbl_tag)
    for tag in tbl.find_all(True):
        if tag.name not in _ALLOWED_TABLE_TAGS:
            tag.unwrap()
            continue
        for attr in list(tag.attrs.keys()):
            if attr not in _ALLOWED_TABLE_ATTRS:
                del tag.attrs[attr]
    return str(tbl)


def _inside_table(el) -> bool:
    return any(p.name == "table" for p in el.parents)


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
        else:
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
