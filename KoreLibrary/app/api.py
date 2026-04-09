import html as _html
import re as _re
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import quote as _urlquote, urlparse as _urlparse, unquote as _urlunquote

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.config import cfg

from app.database import (
    add_book,
    delete_book,
    get_book,
    get_status,
    init_db,
    list_books,
    list_incomplete,
    search_books,
    title_exists,
    update_book,
    update_book_body,
    COMPLETENESS_FIELDS,
)
from app.version import __version__


@asynccontextmanager
async def _lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="KoreLibrary",
    description="Long-form text storage and retrieval service",
    version=__version__,
    lifespan=_lifespan,
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class BookCreate(BaseModel):
    title: str
    body: Optional[str] = None
    author: Optional[str] = None
    year: Optional[int] = None
    language: Optional[str] = None
    genre: Optional[str] = None
    notes: Optional[str] = None


class BookUpdate(BaseModel):
    title: Optional[str] = None
    body: Optional[str] = None
    author: Optional[str] = None
    year: Optional[int] = None
    language: Optional[str] = None
    genre: Optional[str] = None
    notes: Optional[str] = None


class KiwixImportRequest(BaseModel):
    zim_name: str
    title: str
    article_url: Optional[str] = None   # exact Kiwix path, e.g. /content/zim/A/Title
    author: Optional[str] = None
    year: Optional[int] = None
    language: Optional[str] = None
    kiwix_url: Optional[str] = None


# ---------------------------------------------------------------------------
# Books
# ---------------------------------------------------------------------------

@app.get("/books", summary="List all books (metadata only)")
def route_list_books(limit: int = 100, offset: int = 0):
    return list_books(limit=limit, offset=offset)


@app.get("/books/{book_id}", summary="Get a single book with full body")
def route_get_book(book_id: int):
    book = get_book(book_id, include_body=True)
    if book is None:
        raise HTTPException(status_code=404, detail="Book not found")
    return book


@app.post("/books", status_code=201, summary="Add a new book")
def route_add_book(data: BookCreate):
    return add_book(
        title=data.title,
        body=data.body,
        author=data.author,
        year=data.year,
        language=data.language,
        genre=data.genre,
        notes=data.notes,
    )


@app.patch("/books/{book_id}", summary="Update book metadata or body")
def route_update_book(book_id: int, data: BookUpdate):
    if get_book(book_id, include_body=False) is None:
        raise HTTPException(status_code=404, detail="Book not found")
    updated = update_book(book_id, data.model_dump(exclude_none=True))
    return updated


def _repair_kore_anchors(body: str) -> str:
    """Repair bodies imported before the alphanumeric-placeholder fix.

    markdownify escaped KORE_ANCHOR_N_END → KORE\\_ANCHOR\\_N\\_END, so the
    post-processing replace() silently found nothing.  The body still contains
    the TOC fragment links (#anchor_id) in the same order as the placeholders,
    so we can reconstruct the mapping without re-fetching the HTML.
    """
    # Collect unique TOC anchor IDs in order of appearance
    toc_ids: list[str] = []
    seen: set[str] = set()
    for anchor_id in _re.findall(r'\[[^\]]*\]\(#([^)]+)\)', body):
        if anchor_id not in seen:
            seen.add(anchor_id)
            toc_ids.append(anchor_id)

    # Find escaped placeholders in document order
    placeholders = _re.findall(r'KORE\\_ANCHOR\\_\d+\\_END', body)

    if not placeholders:
        return body  # Nothing to repair
    if len(placeholders) != len(toc_ids):
        # Mismatch — can't safely repair; return unchanged
        return body

    result = body
    for placeholder, anchor_id in zip(placeholders, toc_ids):
        result = result.replace(placeholder, f'<span id="{anchor_id}"></span>', 1)
    return result


@app.post("/books/{book_id}/repair-anchors", summary="Repair broken anchor spans in stored body")
def route_repair_anchors(book_id: int):
    book = get_book(book_id, include_body=True)
    if book is None:
        raise HTTPException(status_code=404, detail="Book not found")
    body = book.get("body") or ""
    repaired = _repair_kore_anchors(body)
    if repaired == body:
        return {"repaired": False, "message": "Nothing to repair"}
    updated = update_book_body(book_id, repaired)
    return {"repaired": True, "book": updated}


@app.delete("/books/{book_id}", status_code=204, summary="Delete a book")
def route_delete_book(book_id: int):
    if not delete_book(book_id):
        raise HTTPException(status_code=404, detail="Book not found")
    return JSONResponse(status_code=204, content=None)


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@app.get("/search", summary="Search books by full-text query and/or metadata filters")
def route_search(
    q: Optional[str] = None,
    author: Optional[str] = None,
    title: Optional[str] = None,
    year: Optional[int] = None,
    language: Optional[str] = None,
    genre: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    if not any([q, author, title, year, language, genre]):
        raise HTTPException(
            status_code=400,
            detail="Provide at least one search parameter: q, author, title, year, language, or genre",
        )
    return search_books(
        q=q,
        author=author,
        title=title,
        year=year,
        language=language,
        genre=genre,
        limit=limit,
        offset=offset,
    )


# ---------------------------------------------------------------------------
# Incomplete records
# ---------------------------------------------------------------------------

@app.get("/incomplete", summary="List books with missing metadata fields")
def route_incomplete(fields: Optional[str] = None):
    """
    Returns books where `author`, `year`, `language`, or `genre` are NULL/empty.
    Use `?fields=author,year` to filter to specific missing fields only.
    """
    parsed_fields = None
    if fields:
        parsed_fields = [f.strip() for f in fields.split(",") if f.strip() in COMPLETENESS_FIELDS]
        if not parsed_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Valid fields are: {', '.join(COMPLETENESS_FIELDS)}",
            )
    return list_incomplete(fields=parsed_fields)


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Kiwix — inventory, suggest, import
# ---------------------------------------------------------------------------

_OPDS_NS = "http://www.w3.org/2005/Atom"


def _parse_gutenberg_html(html: str) -> dict:
    """Extract Markdown body and metadata hints from a Kiwix article page."""
    from markdownify import markdownify as _md
    soup = BeautifulSoup(html, "html.parser")

    author: Optional[str] = None
    year: Optional[int] = None
    title: Optional[str] = None
    genre: Optional[str] = None

    for meta in soup.find_all("meta"):
        name = (meta.get("name") or meta.get("property") or "").lower()
        content = meta.get("content", "")
        if not content:
            continue
        if name in ("author", "dc.creator", "citation_author"):
            author = content
        elif name in ("dc.date", "date", "citation_date") and not year:
            try:
                year = int(content[:4])
            except ValueError:
                pass
        elif name in ("dc.title", "title", "citation_title"):
            title = content
        elif name == "dc.subject" and not genre:
            genre = content

    # Fall back to <title> tag, stripping common ZIM suffixes
    if not title:
        tag = soup.find("title")
        if tag and tag.string:
            t = tag.string.strip()
            t = _re.sub(r'\s*[\|\-—]\s*.{3,60}$', '', t).strip()
            if t:
                title = t

    # Fall back to first <h1>
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True) or None

    # Remove chrome: nav, headers, footers, edit links, scripts, TOC
    for tag in soup.select(
        "nav, header, footer, .noprint, #table_of_contents, "
        ".mw-editsection, script, style, noscript, img, figure, .thumbinner"
    ):
        tag.decompose()

    # Preserve anchor targets: <a id="chap01"> and <a name="chap01"> are jump
    # destinations in Gutenberg TOCs.  markdownify silently drops empty elements.
    # Replace with a placeholder that survives markdown conversion, then we
    # restore them as raw HTML spans in a post-processing step.
    anchor_map = {}  # placeholder_key -> id value
    for a in soup.find_all("a"):
        anchor_id = a.get("id") or a.get("name")
        if anchor_id and not a.get("href") and not a.get_text(strip=True):
            # Use alphanumeric-only key — markdownify escapes underscores in
            # plain text (foo_bar → foo\_bar), which silently breaks replace().
            key = f"KANCHORX{len(anchor_map)}X"
            anchor_map[key] = anchor_id
            a.replace_with(key)

    # Strip Kiwix-internal cross-page <a> hrefs but KEEP same-page fragment links
    # (href="#section") so table-of-contents navigation works after rendering.
    # External links (http/https) are also kept as-is.
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith(("http://", "https://", "#")):
            continue
        a.unwrap()   # replace <a> with its text content

    content = soup.find(id="mw-content-text") or soup.find("article") or soup.find("body") or soup

    body_md = _md(str(content), heading_style="ATX", bullets="-", strip=["img"])

    # Restore anchor ID spans from placeholders
    for key, anchor_id in anchor_map.items():
        body_md = body_md.replace(key, f'<span id="{anchor_id}"></span>')

    # Collapse 3+ consecutive blank lines → 2 (paragraph break)
    body_md = _re.sub(r'\n{3,}', '\n\n', body_md).strip()

    return {"body": body_md, "author": author, "year": year, "title": title, "genre": genre}


@app.get("/kiwix/inventory", summary="List available ZIM files from Kiwix OPDS catalog")
async def kiwix_inventory(kiwix_url: Optional[str] = None):
    url = kiwix_url or cfg.get("kiwix_url", "")
    if not url:
        raise HTTPException(status_code=503, detail="kiwix_url not configured")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{url}/catalog/v2/entries", params={"count": -1})
        r.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Kiwix unreachable: {exc}") from exc

    try:
        root = ET.fromstring(r.text)
    except ET.ParseError as exc:
        raise HTTPException(status_code=502, detail=f"Kiwix OPDS parse error: {exc}") from exc

    books = []
    for entry in root.findall(f"{{{_OPDS_NS}}}entry"):
        title  = entry.findtext(f"{{{_OPDS_NS}}}title", default="") or ""
        author_el = entry.find(f"{{{_OPDS_NS}}}author/{{{_OPDS_NS}}}name")
        author = author_el.text if author_el is not None else ""
        # ZIM name comes from the text/html link href:
        # v2 API: /content/ZIMNAME   legacy: /ZIMNAME
        zim_name = ""
        for link in entry.findall(f"{{{_OPDS_NS}}}link"):
            if link.get("type") == "text/html":
                parts = link.get("href", "").strip("/").split("/")
                # skip the "content" path segment if present
                zim_name = parts[1] if len(parts) > 1 and parts[0] == "content" else parts[0]
                break
        if zim_name:
            books.append({"name": zim_name, "title": title, "author": author})

    return {"books": books}


def _extract_link(el) -> str:
    """Get a URL from an XML element: try text (RSS) then href attr (Atom)."""
    # RSS: <link>URL</link>
    if el.text and el.text.strip():
        return el.text.strip()
    # Atom: <link href="URL"/>
    href = el.get("href", "").strip()
    if href:
        return href
    return ""


@app.get("/kiwix/search", summary="Full-text search within a Kiwix ZIM")
async def kiwix_search(zim: str, q: str, count: int = 100, kiwix_url: Optional[str] = None):
    url = kiwix_url or cfg.get("kiwix_url", "")
    if not url:
        raise HTTPException(status_code=503, detail="kiwix_url not configured")
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(
                f"{url}/search",
                params={"books.name": zim, "pattern": q, "format": "xml", "pageLength": count},
            )
        r.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Kiwix unreachable: {exc}") from exc

    # Parse XML — Kiwix returns RSS-like feed
    try:
        root = ET.fromstring(r.text)
    except ET.ParseError as exc:
        raise HTTPException(status_code=502, detail=f"Kiwix search parse error: {exc}") from exc

    results = []
    # Try RSS channel/item structure
    for channel in root.iter("channel"):
        for item in channel.findall("item"):
            title   = (item.findtext("title") or "").strip()
            snippet = (item.findtext("description") or item.findtext("snippet") or "").strip()
            snippet = _re.sub(r"<[^>]+>", " ", _html.unescape(snippet)).strip()
            link_el = item.find("link")
            url_raw = _extract_link(link_el) if link_el is not None else ""
            article_url = _urlparse(url_raw).path if url_raw else None
            if title:
                results.append({"label": title, "value": title, "snippet": snippet, "url": article_url})
    # Fallback: flat <result> elements
    if not results:
        for item in root.iter("result"):
            title   = (item.findtext("title") or "").strip()
            snippet = (item.findtext("snippet") or item.findtext("description") or "").strip()
            snippet = _re.sub(r"<[^>]+>", " ", _html.unescape(snippet)).strip()
            link_el = item.find("link")
            url_raw = _extract_link(link_el) if link_el is not None else ""
            article_url = _urlparse(url_raw).path if url_raw else None
            if title:
                results.append({"label": title, "value": title, "snippet": snippet, "url": article_url})

    return results


@app.get("/kiwix/suggest", summary="Suggest article titles from a Kiwix ZIM")
async def kiwix_suggest(zim: str, pattern: str = "", count: int = 50, kiwix_url: Optional[str] = None):
    url = kiwix_url or cfg.get("kiwix_url", "")
    if not url:
        raise HTTPException(status_code=503, detail="kiwix_url not configured")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(
                f"{url}/suggest",
                params={"content": zim, "term": pattern, "count": count},
            )
        r.raise_for_status()
        items = r.json()
        cleaned = []
        for item in items:
            if not isinstance(item, dict):
                continue
            # Filter out the pseudo full-text-search suggestion Kiwix appends
            url_field = item.get("url", "")
            raw_label = item.get("label") or item.get("value") or ""
            if (
                "/search?" in url_field
                or not item.get("value")
                or _re.search(r"containing\s+['\"]", raw_label, _re.IGNORECASE)
            ):
                continue
            label = _html.unescape(item.get("label") or item.get("value") or "")
            label = _re.sub(r"<[^>]+>", "", label).strip()
            # Use clean value field as the canonical title for import
            value = _html.unescape(item.get("value") or label)
            value = _re.sub(r"<[^>]+>", "", value).strip()
            item["label"] = label
            item["value"] = value
            # Guarantee url is always present so the client can use it directly
            if not item.get("url"):
                item["url"] = f"/content/{zim}/A/{_urlquote(value.replace(' ', '_'), safe='')}"
            cleaned.append(item)
        return cleaned
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Kiwix unreachable: {exc}") from exc


async def _kiwix_search_url(host: str, zim: str, title: str) -> tuple[Optional[str], Optional[str]]:
    """Search Kiwix XML for a title. Returns (content_path, error_detail)."""
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            r = await client.get(
                f"{host}/search",
                params={"books.name": zim, "pattern": title, "format": "xml", "pageLength": 5},
            )
        if r.status_code != 200:
            return None, f"Kiwix search returned HTTP {r.status_code}"
        root = ET.fromstring(r.text)
        # Walk RSS <item> and Atom <entry> elements looking for an exact title match.
        # This handles both Gutenberg (/A/Title.ID) and Khan Academy (hash-based) paths.
        for item in list(root.iter("item")) + list(root.iter("entry")) + list(root.iter("result")):
            item_title = (item.findtext("title") or "").strip()
            link_el = item.find("link")
            raw = _extract_link(link_el) if link_el is not None else ""
            if not raw:
                raw = (item.findtext("guid") or "").strip()
            if raw and item_title.lower() == title.lower():
                path = _urlparse(raw).path
                if path:
                    return path, None
        return None, f"No exact title match in Kiwix search XML (response: {r.text[:2000]!r})"
    except ET.ParseError as exc:
        return None, f"Kiwix search XML parse error: {exc}"
    except Exception as exc:
        return None, f"Kiwix search error: {exc}"


@app.post("/import/kiwix", status_code=201, summary="Import a single book from Kiwix by title")
async def import_kiwix(data: KiwixImportRequest):
    url = data.kiwix_url or cfg.get("kiwix_url", "")
    if not url:
        raise HTTPException(status_code=503, detail="kiwix_url not configured")

    if title_exists(data.title):
        raise HTTPException(status_code=409, detail=f"Already imported: {data.title!r}")

    # Use the URL Kiwix told us (from suggest), falling back to a reconstructed path
    kiwix_path_id = data.title.replace(" ", "_")
    if data.article_url:
        # article_url is a path like /content/zim/A/Title -- prepend the host
        article_url = url.rstrip("/") + data.article_url
    else:
        encoded_path = _urlquote(kiwix_path_id, safe="")
        article_url = f"{url}/content/{data.zim_name}/A/{encoded_path}"
    search_err: Optional[str] = None
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            r = await client.get(article_url)
            if r.status_code == 404 and data.article_url and '/A/' in data.article_url:
                # Retry 1: re-encode special chars in the path segment (dots → %2E etc.)
                pfx, _, title_part = data.article_url.partition('/A/')
                alt = url.rstrip("/") + pfx + '/A/' + _urlquote(title_part, safe='-_~')
                if alt != article_url:
                    r = await client.get(alt)
            if r.status_code == 404 and data.article_url and '/A/' in data.article_url:
                # Retry 2: underscores → %20 spaces (Kiwix suggest uses _ but ZIM may store %20)
                # e.g. suggest gives /A/A._A._Strachan but ZIM has /A/A.%20A.%20Strachan
                pfx, _, title_part = data.article_url.partition('/A/')
                alt2 = url.rstrip("/") + pfx + '/A/' + _urlquote(title_part.replace('_', ' '), safe='-~.')
                if alt2 not in (article_url, alt):
                    r = await client.get(alt2)
            if r.status_code == 404:
                # Suggest URLs lack the numeric Gutenberg ID suffix; search the XML
                # endpoint which returns <link> elements with the canonical full path.
                resolved, search_err = await _kiwix_search_url(url, data.zim_name, data.title)
                if resolved:
                    r = await client.get(url.rstrip("/") + resolved)
        if r.status_code == 404:
            detail = f"Not found in Kiwix: {data.title!r}"
            if search_err:
                detail += f" (search fallback: {search_err})"
            raise HTTPException(status_code=404, detail=detail)
        r.raise_for_status()
    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Kiwix fetch failed: {exc}") from exc

    parsed = _parse_gutenberg_html(r.text)
    return add_book(
        title=data.title,
        body=parsed["body"],
        author=data.author or parsed["author"],
        year=data.year or parsed["year"],
        language=data.language,
        genre=parsed["genre"],
    )


class KiwixViewerImportRequest(BaseModel):
    viewer_url: str          # e.g. http://host/viewer#zim/Article%20Name.123
    kiwix_url: Optional[str] = None   # override host (rarely needed)
    language: str = "en"


class KiwixViewerBatchRequest(BaseModel):
    urls: list[str]
    language: str = "en"
    kiwix_url: Optional[str] = None


async def _fetch_and_import_viewer_url(viewer_url: str, language: str, kiwix_url: Optional[str]) -> dict:
    """Shared logic for single and batch viewer-URL imports.

    Returns a result dict with keys: status ('ok'|'exists'|'error'), title, id, detail.
    Never raises — errors are captured in the result dict.
    """
    result: dict = {"url": viewer_url, "status": "error", "title": None, "id": None, "detail": None}
    try:
        up = _urlparse(viewer_url)
        host = kiwix_url or f"{up.scheme}://{up.netloc}"
        fragment = up.fragment
        if not fragment or '/' not in fragment:
            result["detail"] = "URL must contain a fragment like #zim/Article"
            return result
        slash = fragment.index('/')
        zim = fragment[:slash]
        article_path = fragment[slash + 1:]

        content_url = f"{host}/content/{zim}/{article_path}"
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                r = await client.get(content_url)
            if r.status_code == 404:
                result["detail"] = f"Not found in Kiwix: {content_url}"
                return result
            r.raise_for_status()
        except httpx.HTTPError as exc:
            result["detail"] = f"Kiwix fetch failed: {exc}"
            return result

        parsed = _parse_gutenberg_html(r.text)
        title = parsed["title"] or _urlunquote(article_path.rsplit('.', 1)[0].replace('_', ' '))
        result["title"] = title

        if title_exists(title):
            result["status"] = "exists"
            result["detail"] = f"Already imported: {title!r}"
            return result

        book = add_book(
            title=title,
            body=parsed["body"],
            author=parsed["author"],
            year=parsed["year"],
            language=language,
            genre=parsed["genre"],
        )
        result["status"] = "ok"
        result["id"] = book["id"]
    except Exception as exc:
        result["detail"] = str(exc)
    return result


@app.post("/import/kiwix/viewer", status_code=201, summary="Import a Kiwix article by its viewer URL")
async def import_kiwix_viewer(data: KiwixViewerImportRequest):
    """
    Parses a URL of the form  http://HOST/viewer#ZIM/Article%20Name.ID
    and imports the article directly.  The host, ZIM name, and article path
    are all derived from the URL — no guessing required.
    """
    r = await _fetch_and_import_viewer_url(data.viewer_url, data.language, data.kiwix_url)
    if r["status"] == "error":
        raise HTTPException(status_code=502, detail=r["detail"])
    if r["status"] == "exists":
        raise HTTPException(status_code=409, detail=r["detail"])
    return get_book(r["id"], include_body=False)


@app.post("/import/kiwix/viewer/batch", summary="Batch-import Kiwix articles from a list of viewer URLs")
async def import_kiwix_viewer_batch(data: KiwixViewerBatchRequest):
    """Processes each URL in order, skipping blank lines. Never aborts early."""
    results = []
    for raw in data.urls:
        url = raw.strip()
        if not url or url.startswith('#'):
            continue
        result = await _fetch_and_import_viewer_url(url, data.language, data.kiwix_url)
        results.append(result)
    ok    = sum(1 for r in results if r["status"] == "ok")
    exist = sum(1 for r in results if r["status"] == "exists")
    err   = sum(1 for r in results if r["status"] == "error")
    return {"results": results, "summary": {"ok": ok, "exists": exist, "error": err}}


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

@app.get("/status", summary="Server status and database statistics")
def route_status():
    stats = get_status()
    return {
        "service": "KoreLibrary",
        "version": __version__,
        **stats,
    }
