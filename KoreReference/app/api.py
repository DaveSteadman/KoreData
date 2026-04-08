import re
import threading
from collections import deque
from contextlib import asynccontextmanager
from copy import deepcopy
from typing import Optional
from urllib.parse import unquote, urlparse

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.config import cfg
from app.database import (
    delete_all_articles,
    delete_article,
    get_article_by_title,
    get_backlinks,
    get_category_articles,
    get_links,
    get_random_article,
    get_status,
    init_db,
    list_articles,
    list_categories,
    resolve_article,
    resolve_links,
    search_articles,
    upsert_article,
)
from app.version import __version__


@asynccontextmanager
async def _lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="KoreReference",
    description="Wikipedia-scale encyclopedia service for LLM agents",
    version=__version__,
    lifespan=_lifespan,
)


# ---------------------------------------------------------------------------
# Import state (in-memory; single worker)
# ---------------------------------------------------------------------------

_import_lock = threading.Lock()
_import_state: dict = {
    "running": False, "done": 0, "total": 0, "errors": 0,
    "last_error": None, "mode": None, "seed": None,
}


# Spurious spaces before punctuation introduced by get_text(separator=" ") when
# an <a> tag immediately precedes "," or "." etc.  e.g. "[[link]] , more" → "[[link]], more"
_PUNCT_SPACE_RE = re.compile(r' +([,\.;:!?])')


def _fix_spacing(text: str) -> str:
    return _PUNCT_SPACE_RE.sub(r'\1', text)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ArticleCreate(BaseModel):
    title: str
    body: Optional[str] = None
    summary: Optional[str] = None
    sections: Optional[list] = None
    categories: Optional[list[str]] = None
    facts: Optional[list] = None
    redirect_to: Optional[str] = None
    link_titles: Optional[list[str]] = None


class KiwixImportRequest(BaseModel):
    zim_name: str
    titles: Optional[list[str]] = None   # explicit list; if omitted uses search prefix
    prefix: str = ""                      # search prefix for bulk; "" = all (A→Z walk)
    limit: Optional[int] = None           # cap on number of articles
    resume: bool = True                   # skip articles whose source_hash is unchanged


class KiwixCrawlRequest(BaseModel):
    seed_url: str            # Kiwix viewer URL or direct article URL
    max_depth: int = 1       # 0 = seed only, 1 = seed + direct links, 2 = two hops, …
    limit: int = 200         # hard cap on total articles imported
    resume: bool = True      # skip articles already in DB


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_article(title: str) -> dict:
    article = resolve_article(title)
    if article is None:
        raise HTTPException(status_code=404, detail=f"Article not found: {title!r}")
    return article


def _parse_seed_url(seed_url: str) -> tuple[str, str, str]:
    """
    Parse a Kiwix seed into (kiwix_base, zim_name, start_title).
    Accepts two formats:
      http://host/viewer#zim_name/Article_Title   (Kiwix viewer fragment URL)
      http://host/zim_name/A/Article_Title         (direct content URL)
    """
    p = urlparse(seed_url.strip())
    base = f"{p.scheme}://{p.netloc}"
    # Format 1: fragment URL  e.g. viewer#wikipedia_en/Architecture
    if p.fragment:
        parts = p.fragment.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Cannot parse URL fragment: {p.fragment!r}")
        zim_name, raw_title = parts
        title = unquote(raw_title).replace("_", " ").split("#")[0].strip()
        return base, zim_name, title
    # Format 2: direct URL  e.g. /zim_name/A/Article_Title
    path_parts = p.path.strip("/").split("/")
    if len(path_parts) >= 3 and path_parts[1].upper() == "A":
        zim_name = path_parts[0]
        raw_title = "/".join(path_parts[2:])
        title = unquote(raw_title).replace("_", " ").split("#")[0].strip()
        return base, zim_name, title
    raise ValueError(
        f"Unrecognised Kiwix URL: {seed_url!r}. "
        "Expected http://host/viewer#zim/Title or http://host/zim/A/Title"
    )


def _parse_kiwix_article(html: str, title: str, zim_name: str) -> dict:
    """Extract plain text, summary, sections, categories, and wikilinks from Kiwix HTML."""

    # Marker pair used to embed table HTML inside wikitext body strings
    _TABLE_OPEN  = "<<<TABLE>>>"
    _TABLE_CLOSE = "<<<ENDTABLE>>>"
    _ALLOWED_TABLE_TAGS  = {"table", "thead", "tbody", "tfoot", "tr", "th", "td", "caption"}
    _ALLOWED_TABLE_ATTRS = {"colspan", "rowspan", "scope"}

    def _clean_table(tbl_tag) -> str:
        """Return structural-only HTML for a table (no classes, styles, etc.)."""
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
    soup = BeautifulSoup(html, "html.parser")

    # Remove navigation, references, captions (infobox extracted separately below)
    for tag in soup.select(
        "sup, .reference, .reflist, .navbox, "
        ".thumb, .gallery, .mw-editsection, #toc, "
        ".hatnote, .noprint, style, script"
    ):
        tag.decompose()

    # Extract categories (Wikipedia puts them in #mw-normal-catlinks)
    categories: list[str] = []
    cat_div = soup.find(id="mw-normal-catlinks")
    if cat_div:
        categories = [a.get_text(strip=True) for a in cat_div.find_all("a")[1:]]
        cat_div.decompose()

    # Extract wikilinks (internal links to other articles).
    # Handles ../A/Title (old ZIM), ../Title, ./Title, bare Title (new ZIM formats).
    # Unquotes percent-encoded titles. Does NOT filter on '.' so "St. Louis" works.
    link_titles: list[str] = []
    seen_links: set[str] = set()
    for a in soup.find_all("a", href=True):
        href: str = unquote(a["href"]).split("#")[0].strip()
        if not href or "://" in href or href.startswith("mailto:") or href.startswith("/"):
            continue
        if href.startswith("../A/"):
            raw = href[5:]
        elif href.startswith("./"):
            raw = href[2:]
        elif href.startswith("A/") and "/" not in href[2:]:
            raw = href[2:]
        elif href.startswith("../") and "/" not in href[3:]:
            raw = href[3:]
        elif "/" not in href and not href.startswith("."):
            raw = href  # bare relative slug in newer ZIM format
        else:
            continue
        if not raw or "/" in raw:
            continue
        linked_title = raw.replace("_", " ").strip()
        if linked_title and linked_title not in seen_links and linked_title != title:
            link_titles.append(linked_title)
            seen_links.add(linked_title)

    # Replace internal <a> tags with [[Display|Target]] wikilink markup in-place
    # so that subsequent get_text() calls embed wikilinks in body/section text.
    for a in soup.find_all("a", href=True):
        href: str = unquote(a["href"]).split("#")[0].strip()
        if not href or "://" in href or href.startswith("mailto:") or href.startswith("/"):
            continue
        if href.startswith("../A/"):
            raw = href[5:]
        elif href.startswith("./"):
            raw = href[2:]
        elif href.startswith("A/") and "/" not in href[2:]:
            raw = href[2:]
        elif href.startswith("../") and "/" not in href[3:]:
            raw = href[3:]
        elif "/" not in href and not href.startswith("."):
            raw = href
        else:
            continue
        if not raw or "/" in raw:
            continue
        target = raw.replace("_", " ").strip()
        display = a.get_text(strip=True)
        if target and display:
            wikilink = f"[[{target}]]" if display == target else f"[[{display}|{target}]]"
            a.replace_with(wikilink)

    # Extract infobox facts (anchors already converted to [[wikilinks]] above)
    facts: list[list[str]] = []
    for infobox in soup.select(".infobox"):
        for row in infobox.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                label = _fix_spacing(th.get_text(" ", strip=True))
                value = _fix_spacing(td.get_text(" ", strip=True))
                if label and value:
                    facts.append([label, value])
        infobox.decompose()

    # Extract sections from headings, with wikilink-embedded content
    sections: list[dict] = []
    content_div = soup.find(id="mw-content-text") or soup.find("body") or soup
    current_heading: Optional[str] = None
    current_parts: list[str] = []
    for el in content_div.find_all(["h2", "h3", "h4", "p", "ul", "ol", "table"]):
        if _inside_table(el):
            continue
        if el.name in ("h2", "h3", "h4"):
            if current_heading is not None:
                sections.append({"title": current_heading, "content": "\n".join(current_parts).strip()})
            current_heading = el.get_text(strip=True).rstrip("[edit]").strip()
            current_parts = []
        elif el.name == "table":
            current_parts.append(f"{_TABLE_OPEN}{_clean_table(el)}{_TABLE_CLOSE}")
        else:
            text = _fix_spacing(el.get_text(separator=" ", strip=True))
            if text:
                current_parts.append(text)
    if current_heading is not None:
        sections.append({"title": current_heading, "content": "\n".join(current_parts).strip()})

    # Build wikitext body: preamble paragraphs then == Heading == sections
    body_parts: list[str] = []
    for el in content_div.find_all(["p", "ul", "ol", "h2", "h3", "h4", "table"]):
        if _inside_table(el):
            continue
        if el.name in ("h2", "h3", "h4"):
            heading = el.get_text(strip=True).rstrip("[edit]").strip()
            body_parts.append(f"== {heading} ==")
        elif el.name == "table":
            body_parts.append(f"{_TABLE_OPEN}{_clean_table(el)}{_TABLE_CLOSE}")
        else:
            text = _fix_spacing(el.get_text(separator=" ", strip=True))
            if text:
                body_parts.append(text)
    body = "\n\n".join(body_parts)

    # Summary = first non-empty paragraph
    summary: Optional[str] = None
    for el in content_div.find_all("p"):
        text = _fix_spacing(el.get_text(separator=" ", strip=True))
        if text:
            summary = text
            break

    return {
        "body": body,
        "summary": summary,
        "sections": sections,
        "categories": categories,
        "link_titles": link_titles,
        "facts": facts,
    }


# ---------------------------------------------------------------------------
# Background import worker
# ---------------------------------------------------------------------------

def _kiwix_article_url(kiwix_base: str, zim_name: str, title: str) -> str:
    """Kiwix serves articles at /<zim_name>/A/<Title_With_Underscores>"""
    return f"{kiwix_base}/{zim_name}/A/{title.replace(' ', '_')}"


def _kiwix_suggest_titles(client: httpx.Client, kiwix_base: str, zim_name: str,
                          prefix: str, limit: int) -> list[str]:
    """
    Use the Kiwix suggestion API to enumerate article titles.
    GET /<zim>/suggest?content=<zim>&pattern=<prefix>&count=<n>
    Returns a JSON list of {"label": title, "value": title, "url": ...}
    """
    url = f"{kiwix_base}/suggest"
    resp = client.get(url, params={"content": zim_name, "pattern": prefix, "count": limit})
    resp.raise_for_status()
    items = resp.json()
    return [item["label"] for item in items if isinstance(item, dict) and item.get("label")]


def _import_one(client: httpx.Client, kiwix_base: str, zim_name: str,
                title: str, resume: bool) -> None:
    """Fetch and upsert a single article by title. Raises on HTTP error."""
    url = _kiwix_article_url(kiwix_base, zim_name, title)
    resp = client.get(url)
    resp.raise_for_status()
    html = resp.text
    parsed = _parse_kiwix_article(html, title, zim_name)
    upsert_article(
        title=title,
        body=parsed["body"],
        summary=parsed["summary"],
        sections=parsed["sections"],
        categories=parsed["categories"],
        facts=parsed["facts"],
        link_titles=parsed["link_titles"],
    )


def _run_kiwix_import(
    zim_name: str,
    titles: Optional[list[str]],
    prefix: str,
    limit: Optional[int],
    resume: bool,
) -> None:
    global _import_state
    kiwix_base = cfg["kiwix_url"].rstrip("/")

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        # Resolve the title list
        if titles:
            work = titles[:limit] if limit else titles
        else:
            fetch_limit = limit or 50_000
            try:
                work = _kiwix_suggest_titles(client, kiwix_base, zim_name, prefix, fetch_limit)
            except Exception as exc:
                _import_state.update({"running": False, "last_error": str(exc)})
                return

        _import_state["total"] = len(work)

        for title in work:
            if not _import_state["running"]:
                break
            try:
                _import_one(client, kiwix_base, zim_name, title, resume)
                _import_state["done"] += 1
            except Exception as exc:
                _import_state["errors"] += 1
                _import_state["last_error"] = f"{title}: {exc}"

    # Post-import: resolve links
    resolve_links(limit=500_000)
    _import_state["running"] = False


def _run_kiwix_crawl(seed_url: str, max_depth: int, limit: int, resume: bool) -> None:
    """BFS crawl starting from seed_url; follows wikilinks up to max_depth hops."""
    global _import_state
    try:
        kiwix_base, zim_name, start_title = _parse_seed_url(seed_url)
    except ValueError as exc:
        _import_state.update({"running": False, "last_error": str(exc)})
        return

    queue: deque[tuple[str, int]] = deque([(start_title, 0)])
    visited: set[str] = {start_title}
    _import_state["total"] = 1

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        while queue and _import_state["running"]:
            if _import_state["done"] >= limit:
                break

            title, depth = queue.popleft()

            # Resume: article already in DB — skip HTTP fetch but still expand its links
            if resume:
                existing = get_article_by_title(title, full=False)
                if existing is not None:
                    db_links = get_links(title) if depth < max_depth else []
                    if db_links or depth >= max_depth:
                        # Have links (or don't need them) — skip re-fetch
                        _import_state["done"] += 1
                        for lnk in db_links:
                            lt = (lnk.get("to_title") or "").strip()
                            if lt and lt not in visited:
                                if _import_state["done"] + len(queue) < limit:
                                    visited.add(lt)
                                    queue.append((lt, depth + 1))
                        _import_state["total"] = max(_import_state["total"], len(visited))
                        continue
                    # Article exists but has no links and we need depth expansion —
                    # fall through to re-fetch so links get extracted and saved

            try:
                url = _kiwix_article_url(kiwix_base, zim_name, title)
                resp = client.get(url)
                resp.raise_for_status()
                html = resp.text
                parsed = _parse_kiwix_article(html, title, zim_name)
                upsert_article(
                    title=title,
                    body=parsed["body"],
                    summary=parsed["summary"],
                    sections=parsed["sections"],
                    categories=parsed["categories"],
                    facts=parsed["facts"],
                    link_titles=parsed["link_titles"],
                )
                _import_state["done"] += 1

                if depth < max_depth:
                    for lt in parsed["link_titles"]:
                        lt = lt.strip()
                        if lt and lt not in visited:
                            if _import_state["done"] + len(queue) < limit:
                                visited.add(lt)
                                queue.append((lt, depth + 1))
                    _import_state["total"] = max(_import_state["total"], len(visited))

            except Exception as exc:
                _import_state["errors"] += 1
                _import_state["last_error"] = f"{title}: {exc}"

    resolve_links(limit=500_000)
    _import_state["running"] = False


# ---------------------------------------------------------------------------
# Articles
# ---------------------------------------------------------------------------

@app.get("/articles", summary="List articles (metadata only)")
def route_list_articles(limit: int = 100, offset: int = 0):
    return list_articles(limit=limit, offset=offset)


@app.get("/articles/random", summary="Random non-redirect article")
def route_random_article():
    article = get_random_article()
    if article is None:
        raise HTTPException(status_code=404, detail="No articles in database")
    return article


@app.get("/articles/{title}", summary="Get article by title, following redirects")
def route_get_article(title: str):
    article = resolve_article(title)
    if article is None:
        raise HTTPException(status_code=404, detail=f"Article not found: {title!r}")
    return article


@app.get("/articles/{title}/summary", summary="Summary paragraph only")
def route_get_summary(title: str):
    article = resolve_article(title)
    if article is None:
        raise HTTPException(status_code=404, detail=f"Article not found: {title!r}")
    return {
        "title":      article["title"],
        "summary":    article.get("summary"),
        "word_count": article.get("word_count"),
        "categories": article.get("categories", []),
    }


@app.get("/articles/{title}/section/{section_name}", summary="Single named section")
def route_get_section(title: str, section_name: str):
    article = resolve_article(title)
    if article is None:
        raise HTTPException(status_code=404, detail=f"Article not found: {title!r}")
    sections: list = article.get("sections") or []
    match = next(
        (s for s in sections if s["title"].lower() == section_name.lower()),
        None,
    )
    if match is None:
        raise HTTPException(status_code=404, detail=f"Section not found: {section_name!r}")
    return {"title": article["title"], "section": match["title"], "content": match["content"]}


@app.get("/articles/{title}/links", summary="Outbound links from an article")
def route_get_links(title: str):
    if get_article_by_title(title, full=False) is None:
        raise HTTPException(status_code=404, detail=f"Article not found: {title!r}")
    return get_links(title)


@app.get("/articles/{title}/backlinks", summary="Articles that link to this article")
def route_get_backlinks(title: str, limit: int = 50, offset: int = 0):
    if get_article_by_title(title, full=False) is None:
        raise HTTPException(status_code=404, detail=f"Article not found: {title!r}")
    return get_backlinks(title, limit=limit, offset=offset)


@app.get("/articles/{title}/categories", summary="Categories for an article")
def route_get_article_categories(title: str):
    article = get_article_by_title(title, full=False)
    if article is None:
        raise HTTPException(status_code=404, detail=f"Article not found: {title!r}")
    return {"title": article["title"], "categories": article["categories"]}


@app.post("/articles", status_code=201, summary="Add or upsert an article")
def route_upsert_article(data: ArticleCreate):
    return upsert_article(
        title=data.title,
        body=data.body,
        summary=data.summary,
        sections=data.sections,
        categories=data.categories,
        facts=data.facts,
        redirect_to=data.redirect_to,
        link_titles=data.link_titles,
    )


@app.delete("/articles", summary="Delete all articles")
def route_delete_all_articles():
    count = delete_all_articles()
    return {"deleted": count}


@app.delete("/articles/{title}", status_code=204, summary="Remove an article and its links")
def route_delete_article(title: str):
    if not delete_article(title):
        raise HTTPException(status_code=404, detail=f"Article not found: {title!r}")
    return JSONResponse(status_code=204, content=None)


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@app.get("/search", summary="Full-text and prefix search across articles")
def route_search(
    q: Optional[str] = None,
    title: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
):
    if not any([q, title, category]):
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of: q, title, category",
        )
    return search_articles(q=q, title=title, category=category, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Categories
# ---------------------------------------------------------------------------

@app.get("/categories", summary="List all categories with article counts")
def route_list_categories():
    return list_categories()


@app.get("/categories/{name}", summary="List articles in a category")
def route_category_articles(name: str, limit: int = 100, offset: int = 0):
    articles = get_category_articles(name, limit=limit, offset=offset)
    if not articles and not list_categories():
        raise HTTPException(status_code=404, detail=f"Category not found: {name!r}")
    return articles


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

@app.post("/import/kiwix", summary="Trigger import from configured Kiwix server")
def route_import_kiwix(req: KiwixImportRequest, background_tasks: BackgroundTasks):
    if not _import_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Import already running")
    _import_state.update({
        "running": True, "done": 0, "total": 0,
        "errors": 0, "last_error": None, "mode": "prefix", "seed": None,
    })
    _import_lock.release()
    background_tasks.add_task(
        _run_kiwix_import, req.zim_name, req.titles, req.prefix, req.limit, req.resume
    )
    return {"started": True, "zim_name": req.zim_name}


@app.post("/import/kiwix/crawl", status_code=202, summary="BFS crawl from a Kiwix article URL")
def route_import_kiwix_crawl(req: KiwixCrawlRequest, background_tasks: BackgroundTasks):
    if not _import_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Import already running")
    try:
        _, _, start_title = _parse_seed_url(req.seed_url)
    except ValueError as exc:
        _import_lock.release()
        raise HTTPException(status_code=422, detail=str(exc))
    _import_state.update({
        "running": True, "done": 0, "total": 1,
        "errors": 0, "last_error": None, "mode": "crawl", "seed": start_title,
    })
    _import_lock.release()
    background_tasks.add_task(
        _run_kiwix_crawl, req.seed_url, req.max_depth, req.limit, req.resume
    )
    return {"started": True, "seed": start_title, "max_depth": req.max_depth, "limit": req.limit}


@app.post("/import/stop", summary="Abort in-progress import or crawl")
def route_import_stop():
    if _import_state.get("running"):
        _import_state["running"] = False
        return {"stopped": True}
    return {"stopped": False, "detail": "No import was running"}



@app.post("/import/article", status_code=201, summary="Import a single article by title from Kiwix")
def route_import_article(zim_name: str, title: str):
    """Synchronous single-article import — useful for testing and on-demand fetch."""
    kiwix_base = cfg["kiwix_url"].rstrip("/")
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            _import_one(client, kiwix_base, zim_name, title, resume=True)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Kiwix returned {exc.response.status_code} for {title!r}",
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return get_article_by_title(title, full=False)


@app.get("/import/status", summary="Progress of in-progress import")
def route_import_status():
    return dict(_import_state)


# ---------------------------------------------------------------------------
# Admin
# ---------------------------------------------------------------------------

@app.get("/status", summary="Server status and database statistics")
def route_status():
    return {
        "service": "KoreReference",
        "version": __version__,
        **get_status(),
    }
