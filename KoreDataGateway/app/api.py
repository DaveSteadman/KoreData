import asyncio
import json as _json
import re
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from markupsafe import Markup, escape
from pydantic import BaseModel, Field

from app.config import cfg
from app.version import __version__

# ---------------------------------------------------------------------------
# Child process management
# ---------------------------------------------------------------------------

_BASE = Path(__file__).parent.parent.parent  # KoreData/ root

_SERVICES = [
    (_BASE / "KoreFeed",      "KoreFeed"),
    (_BASE / "KoreLibrary",   "KoreLibrary"),
    (_BASE / "KoreReference", "KoreReference"),
]

_children: list[tuple[subprocess.Popen, str, object]] = []


def _start_children() -> None:
    for service_dir, label in _SERVICES:
        log_path = service_dir / "data" / "service.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = open(log_path, "a", encoding="utf-8")  # noqa: SIM115
        proc = subprocess.Popen(
            [sys.executable, "main.py"],
            cwd=service_dir,
            stdout=log_file,
            stderr=log_file,
        )
        _children.append((proc, label, log_file))
        print(f"  ► {label} starting  (pid {proc.pid})  log → {log_path.relative_to(_BASE)}")


def _stop_children() -> None:
    for proc, label, log_file in reversed(_children):
        if proc.poll() is not None:
            continue  # already exited
        print(f"  ◼ Stopping {label}  (pid {proc.pid})")
        proc.terminate()
    for proc, label, log_file in reversed(_children):
        try:
            proc.wait(timeout=6)
        except subprocess.TimeoutExpired:
            print(f"  ✗ Force-killing {label}")
            proc.kill()
        try:
            log_file.close()
        except Exception:
            pass


async def _wait_for(client: httpx.AsyncClient, label: str, timeout: float = 20.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        try:
            r = await client.get("/status", timeout=2.0)
            if r.status_code == 200:
                print(f"  ✓ {label} ready")
                return
        except Exception:
            pass
        await asyncio.sleep(0.5)
    print(f"  ⚠ {label} did not respond within {timeout:.0f}s — continuing anyway")


# ---------------------------------------------------------------------------
# App + lifespan
# ---------------------------------------------------------------------------

_feed_client: httpx.AsyncClient | None = None
_lib_client:  httpx.AsyncClient | None = None
_ref_client:  httpx.AsyncClient | None = None


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _feed_client, _lib_client, _ref_client
    print("\n  KoreDataGateway — starting child services")
    _start_children()
    _feed_client = httpx.AsyncClient(base_url=cfg["korefeed_url"],      timeout=15.0)
    _lib_client  = httpx.AsyncClient(base_url=cfg["korelibrary_url"],   timeout=15.0)
    _ref_client  = httpx.AsyncClient(base_url=cfg["korereference_url"], timeout=15.0)
    await asyncio.gather(
        _wait_for(_feed_client,  "KoreFeed"),
        _wait_for(_lib_client,   "KoreLibrary"),
        _wait_for(_ref_client,   "KoreReference"),
    )
    print("  All services ready\n")
    yield
    print("\n  KoreDataGateway — shutting down child services")
    await _feed_client.aclose()
    await _lib_client.aclose()
    await _ref_client.aclose()
    _stop_children()


app = FastAPI(
    title="KoreDataGateway",
    description="Central web UI for KoreData services",
    version=__version__,
    lifespan=_lifespan,
)

TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.globals["app_version"] = __version__


_TABLE_MARKER_RE = re.compile(r'<<<TABLE>>>(.*?)<<<ENDTABLE>>>', re.DOTALL)
_WIKILINK_RE     = re.compile(r'\[\[([^\]]+)\]\]')


def _resolve_wikilinks_in_html(html: str) -> str:
    """Replace [[Display|Target]] / [[Target]] patterns inside already-safe HTML."""
    def _repl(m: re.Match) -> str:
        inner = m.group(1)
        if "|" in inner:
            display, target = inner.split("|", 1)
        else:
            display = target = inner
        target  = target.strip()
        display = display.strip()
        return f'<a href="/reference/{quote(target)}">{escape(display)}</a>'
    return _WIKILINK_RE.sub(_repl, html)


def _process_wikitext(text: str) -> str:
    """Escape text, convert [[wikilinks]], paragraphs and line breaks to HTML."""
    parts: list[str] = []
    last_end = 0
    for m in _WIKILINK_RE.finditer(text):
        parts.append(str(escape(text[last_end:m.start()])))
        inner = m.group(1)
        if "|" in inner:
            display, target = inner.split("|", 1)
        else:
            display = target = inner
        target = target.strip()
        display = display.strip()
        parts.append(f'<a href="/reference/{quote(target)}">{escape(display)}</a>')
        last_end = m.end()
    parts.append(str(escape(text[last_end:])))
    html = "".join(parts)
    html = html.replace('\r\n', '\n').replace('\r', '\n')
    html = re.sub(r'\n{2,}', '</p><p>', html)
    html = html.replace('\n', '<br>')
    return f'<p>{html}</p>'


def _wikilinks_filter(text: str) -> Markup:
    """Convert [[Title]] wikilinks to anchors; pass <<<TABLE>>>...<<<ENDTABLE>>> through as raw HTML.
    HTML-escapes all user text. Double newlines → paragraph breaks; single → <br>."""
    if not text:
        return Markup("")
    result: list[str] = []
    last_end = 0
    for m in _TABLE_MARKER_RE.finditer(text):
        segment = text[last_end:m.start()]
        if segment.strip():
            result.append(_process_wikitext(segment))
        result.append(_resolve_wikilinks_in_html(m.group(1)))  # table HTML with wikilinks resolved
        last_end = m.end()
    remaining = text[last_end:]
    if remaining.strip():
        result.append(_process_wikitext(remaining))
    return Markup("".join(result))


templates.env.filters["wikilinks"] = _wikilinks_filter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_wiki_links(body: str) -> list[str]:
    """Extract unique [[Title]] link targets from body text."""
    seen: set[str] = set()
    result: list[str] = []
    for m in re.finditer(r'\[\[([^\]|]+?)(?:\|[^\]]*)?\]\]', body or ""):
        t = m.group(1).strip()
        if t and t not in seen:
            seen.add(t)
            result.append(t)
    return result


def _parse_wiki_sections(body: str) -> list[dict] | None:
    """Extract sections from == Heading == markers in wikitext body."""
    sections: list[dict] = []
    current_heading: str | None = None
    current_parts: list[str] = []
    for line in (body or "").split("\n"):
        hm = re.match(r'^==+\s*(.+?)\s*==+\s*$', line)
        if hm:
            if current_heading is not None:
                sections.append({"title": current_heading, "content": "\n".join(current_parts).strip()})
            current_heading = hm.group(1)
            current_parts = []
        else:
            current_parts.append(line)
    if current_heading is not None:
        sections.append({"title": current_heading, "content": "\n".join(current_parts).strip()})
    return sections or None


def _extract_summary(body: str) -> str | None:
    """Return first non-heading, non-empty line from body as summary."""
    for line in (body or "").split("\n"):
        line = line.strip()
        if line and not re.match(r'^==', line):
            return re.sub(r'\[\[(?:[^\]|]+\|)?([^\]]+)\]\]', r'\1', line)
    return None


def _sections_to_edit_body(article: dict) -> str:
    """Reconstruct wiki-formatted body (== Heading == markers) from stored sections.
    Used so editing a Kiwix-imported article round-trips correctly through save."""
    body = (article.get("body") or "").strip()
    sections = article.get("sections") or []
    if not sections:
        return body
    # If body already contains == markers, it's already wiki-formatted
    if re.search(r'^==', body, re.MULTILINE):
        return body
    # Reconstruct from sections
    parts: list[str] = []
    for s in sections:
        parts.append(f"== {s['title']} ==")
        content = (s.get("content") or "").strip()
        if content:
            parts.append(content)
    return "\n\n".join(parts)


def _parse_year(value: Optional[str]) -> Optional[int]:
    """Parse a year string from a browser form field; returns None if blank or non-numeric."""
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_article_form(
    body: Optional[str],
    summary: Optional[str],
    categories: Optional[str],
    redirect_to: Optional[str],
    facts_raw: Optional[str] = None,
) -> dict:
    """Parse and normalise the shared form fields for new-article and edit-article POST handlers."""
    body = body.replace("\r\n", "\n").replace("\r", "\n").strip() if body else None
    summary = summary.strip() if summary else None
    cats = [c.strip() for c in (categories or "").split(",") if c.strip()]
    links = _parse_wiki_links(body or "")
    sections = _parse_wiki_sections(body or "")
    facts: list[list[str]] = []
    for line in (facts_raw or "").splitlines():
        line = line.strip()
        if ":" in line:
            label, _, value = line.partition(":")
            label = label.strip()
            value = value.strip()
            if label and value:
                facts.append([label, value])
    return {
        "body":        body,
        "summary":     summary or _extract_summary(body or ""),
        "cats":        cats,
        "links":       links,
        "sections":    sections,
        "facts":       facts,
        "redirect_to": redirect_to.strip() if redirect_to and redirect_to.strip() else None,
    }


def _svc_ui(r: Any, label: str, slug: str, url: str) -> dict:
    """Build a service summary dict for the landing page template."""
    healthy = not isinstance(r, Exception) and r.status_code == 200
    return {"label": label, "slug": slug, "url": url, "healthy": healthy,
            "stats": r.json() if healthy else {}}


def _svc_status(r: Any, url: str) -> dict:
    """Build a child status dict for the /status endpoint (flattens child /status fields)."""
    healthy = not isinstance(r, Exception) and r.status_code == 200
    return {"url": url, "healthy": healthy, **(r.json() if healthy else {})}


# ---------------------------------------------------------------------------
# Unified search — agent API
# ---------------------------------------------------------------------------

class _SearchRequest(BaseModel):
    query: str
    domains: list[str] = Field(default_factory=list)
    since: Optional[str] = None
    until: Optional[str] = None
    limit: int = Field(default=5, ge=1, le=20)


def _map_feed_entry(e: dict) -> dict:
    domain = e.get("domain", "")
    eid    = e.get("id", "")
    body   = e.get("page_text") or e.get("content") or e.get("body") or e.get("summary") or ""
    return {
        "type":         "feed_entry",
        "id":           eid,
        "title":        e.get("headline") or e.get("title", ""),
        "source":       e.get("feed_name") or e.get("source_name") or domain,
        "published_at": e.get("published") or e.get("published_at") or e.get("ingested_at"),
        "snippet":      body[:300].strip(),
        "url":          f"/feeds/{domain}/{eid}",
    }


def _map_ref_article(a: dict) -> dict:
    title = a.get("title", "")
    return {
        "type":       "reference_article",
        "title":      title,
        "summary":    a.get("summary", ""),
        "snippet":    a.get("snippet") or (a.get("summary") or "")[:300],
        "word_count": a.get("word_count"),
        "url":        f"/reference/{quote(title, safe='')}",
    }


def _map_lib_book(b: dict) -> dict:
    return {
        "type":    "library_book",
        "id":      b.get("id"),
        "title":   b.get("title", ""),
        "author":  b.get("author", ""),
        "snippet": b.get("snippet") or (b.get("notes") or "")[:300],
        "url":     f"/library/{b.get('id', '')}",
    }


@app.post("/search")
async def api_search(req: _SearchRequest):
    search_domains = [d.lower() for d in req.domains] if req.domains else ["feeds", "reference", "library"]
    limit = req.limit

    async def _feeds():
        params: dict = {"q": req.query, "limit": limit}
        if req.since: params["since"] = req.since
        if req.until: params["until"] = req.until
        r = await _feed_client.get("/api/search", params=params, timeout=10.0)
        if r.status_code != 200:
            return {"error": f"HTTP {r.status_code}"}
        return [_map_feed_entry(e) for e in (r.json() or [])[:limit]]

    async def _reference():
        params: dict = {"q": req.query, "limit": limit}
        r = await _ref_client.get("/search", params=params, timeout=10.0)
        if r.status_code != 200:
            return {"error": f"HTTP {r.status_code}"}
        return [_map_ref_article(a) for a in (r.json() or [])[:limit]]

    async def _library():
        params: dict = {"q": req.query, "limit": limit}
        r = await _lib_client.get("/search", params=params, timeout=10.0)
        if r.status_code != 200:
            return {"error": f"HTTP {r.status_code}"}
        return [_map_lib_book(b) for b in (r.json() or [])[:limit]]

    tasks: list[tuple[str, Any]] = []
    if "feeds"     in search_domains: tasks.append(("feeds",     _feeds()))
    if "reference" in search_domains: tasks.append(("reference", _reference()))
    if "library"   in search_domains: tasks.append(("library",   _library()))

    gathered = await asyncio.gather(*(coro for _, coro in tasks), return_exceptions=True)
    results = {
        key: ({"error": str(val)} if isinstance(val, Exception) else val)
        for (key, _), val in zip(tasks, gathered)
    }
    return {
        "query":            req.query,
        "domains_searched": [key for key, _ in tasks],
        "results":          results,
    }


def _add_next_mins(feeds: list) -> None:
    """Compute _next_mins for each feed dict in-place."""
    now = datetime.utcnow()
    for f in feeds:
        last = f.get("last_fetched_at")
        if last:
            try:
                nxt = datetime.fromisoformat(last) + timedelta(minutes=int(f.get("update_rate", 60)))
                f["_next_mins"] = int((nxt - now).total_seconds() / 60)
            except Exception:
                f["_next_mins"] = None
        else:
            f["_next_mins"] = None


# ===========================================================================
# KoreFeed — Web UI (GET / render)
# ===========================================================================

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def web_root(request: Request):
    kf_r, kl_r, kr_r = await asyncio.gather(
        _feed_client.get("/status", timeout=3.0),
        _lib_client.get("/status", timeout=3.0),
        _ref_client.get("/status", timeout=3.0),
        return_exceptions=True,
    )
    services = [
        _svc_ui(kf_r, "KoreFeed",      "feeds",     cfg["korefeed_url"]),
        _svc_ui(kl_r, "KoreLibrary",   "library",   cfg["korelibrary_url"]),
        _svc_ui(kr_r, "KoreReference", "reference", cfg["korereference_url"]),
    ]
    return templates.TemplateResponse(request, "home.html", {"services": services})


@app.get("/feeds", response_class=HTMLResponse)
async def web_index(request: Request):
    domains_r, feeds_r = await asyncio.gather(
        _feed_client.get("/api/domains"),
        _feed_client.get("/api/feeds"),
    )
    domains   = domains_r.json() if domains_r.status_code == 200 else []
    all_feeds = feeds_r.json()   if feeds_r.status_code == 200   else []
    _add_next_mins(all_feeds)
    all_feeds.sort(key=lambda f: (
        0 if f["_next_mins"] is None else (1 if f["_next_mins"] <= 0 else 2),
        f["_next_mins"] if f["_next_mins"] is not None else 0,
    ))
    return templates.TemplateResponse(
        request, "feed_index.html",
        {"domains": domains, "all_feeds": all_feeds},
    )


@app.get("/feeds/search", response_class=HTMLResponse)
async def web_search(
    request: Request,
    q: str = "",
    domain: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    limit: int = 50,
):
    results = []
    if q:
        params: dict = {"q": q, "limit": limit}
        if domain: params["domain"] = domain
        if since:  params["since"]  = since
        if until:  params["until"]  = until
        r = await _feed_client.get("/api/search", params=params)
        results = r.json() if r.status_code == 200 else []
    return templates.TemplateResponse(
        request, "feed_search.html",
        {"q": q, "domain": domain, "since": since or "", "until": until or "",
         "limit": limit, "results": results},
    )


@app.get("/feeds/{domain}", response_class=HTMLResponse)
async def web_domain(request: Request, domain: str, limit: int = 50, offset: int = 0):
    entries_r, all_domains_r, feeds_all_r, age_r, counts_r = await asyncio.gather(
        _feed_client.get(f"/api/domains/{domain}/entries", params={"limit": limit, "offset": offset}),
        _feed_client.get("/api/domains"),
        _feed_client.get("/api/feeds"),
        _feed_client.get(f"/api/domains/{domain}/age-settings"),
        _feed_client.get(f"/api/domains/{domain}/feed-counts"),
    )
    entries      = entries_r.json()     if entries_r.status_code == 200     else []
    all_domains  = all_domains_r.json() if all_domains_r.status_code == 200 else []
    all_feeds    = feeds_all_r.json()   if feeds_all_r.status_code == 200   else []
    age_settings = age_r.json()         if age_r.status_code == 200         else {"mode": "none"}
    feed_counts  = counts_r.json()      if counts_r.status_code == 200      else {}

    domain_info = next((d for d in all_domains if d["domain"] == domain), {})
    total       = domain_info.get("entry_count", len(entries))
    feeds       = [f for f in all_feeds if f.get("domain") == domain]
    _add_next_mins(feeds)
    feed_refresh_mins = {f["id"]: f.get("_next_mins") for f in feeds}

    return templates.TemplateResponse(
        request, "feed_domain.html",
        {
            "domain":            domain,
            "entries":           entries,
            "total":             total,
            "limit":             limit,
            "offset":            offset,
            "feeds":             feeds,
            "age_settings":      age_settings,
            "feed_counts":       feed_counts,
            "feed_refresh_mins": feed_refresh_mins,
        },
    )


@app.get("/feeds/{domain}/{entry_id}", response_class=HTMLResponse)
async def web_entry(request: Request, domain: str, entry_id: int):
    r = await _feed_client.get(f"/api/domains/{domain}/entries/{entry_id}")
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="Entry not found")
    entry = r.json()
    if entry.get("metadata") and isinstance(entry["metadata"], str):
        try:
            entry["metadata"] = _json.loads(entry["metadata"])
        except Exception:
            pass
    return templates.TemplateResponse(
        request, "feed_entry.html",
        {"domain": domain, "entry": entry},
    )


# ===========================================================================
# KoreFeed — Web UI (POST / mutations)
# ===========================================================================

@app.post("/feeds/domains/create")
async def web_create_domain(domain: str = Form(...)):
    await _feed_client.post("/api/domains", params={"domain": domain})
    return RedirectResponse("/feeds", status_code=303)


@app.post("/feeds/domains/{domain}/delete")
async def web_delete_domain(domain: str):
    await _feed_client.delete(f"/api/domains/{domain}")
    return RedirectResponse("/feeds", status_code=303)


@app.post("/feeds/domains/{domain}/rename")
async def web_rename_domain(domain: str, new_name: str = Form(...)):
    await _feed_client.post(f"/api/domains/{domain}/rename", params={"new_name": new_name})
    return RedirectResponse("/feeds", status_code=303)


@app.post("/feeds/{domain}/feeds/add")
async def web_add_feed(
    domain: str,
    name: str = Form(...),
    url: str = Form(...),
    update_rate: int = Form(60),
    feed_type: str = Form("rss"),
):
    await _feed_client.post("/api/feeds", json={
        "domain": domain, "name": name, "url": url,
        "update_rate": update_rate, "feed_type": feed_type,
    })
    return RedirectResponse(f"/feeds/{domain}", status_code=303)


@app.post("/feeds/{domain}/feeds/{feed_id}/delete")
async def web_delete_feed(domain: str, feed_id: str):
    await _feed_client.delete(f"/api/feeds/{feed_id}")
    return RedirectResponse(f"/feeds/{domain}", status_code=303)


@app.post("/feeds/{domain}/feeds/{feed_id}/refresh")
async def web_refresh_feed(domain: str, feed_id: str):
    await _feed_client.post(f"/api/feeds/{feed_id}/trigger")
    return JSONResponse({"triggered": feed_id})


@app.post("/feeds/{domain}/entries/{entry_id}/delete")
async def web_delete_entry(request: Request, domain: str, entry_id: int):
    await _feed_client.delete(f"/api/domains/{domain}/entries/{entry_id}")
    return JSONResponse({"deleted": entry_id})


@app.post("/feeds/{domain}/entries/delete-older-than")
async def web_delete_older_than(domain: str, days: float = Form(...)):
    await _feed_client.delete(
        f"/api/domains/{domain}/entries",
        params={"older_than_days": days},
    )
    return RedirectResponse(f"/feeds/{domain}", status_code=303)


@app.post("/feeds/{domain}/entries/delete-by-feed")
async def web_delete_by_feed(domain: str, feed_name: str = Form(...)):
    await _feed_client.delete(
        f"/api/domains/{domain}/entries",
        params={"feed_name": feed_name},
    )
    return RedirectResponse(f"/feeds/{domain}", status_code=303)


@app.post("/feeds/entries/bulk-delete")
async def web_bulk_delete_entries(request: Request, sel: list[str] = Form(default=[])):
    by_domain: dict[str, list[int]] = {}
    for item in sel:
        parts = item.split(":", 1)
        if len(parts) == 2:
            d, eid = parts
            try:
                by_domain.setdefault(d, []).append(int(eid))
            except ValueError:
                pass
    for d, ids in by_domain.items():
        await _feed_client.post(f"/api/domains/{d}/entries/bulk-delete", json=ids)
    ref = request.headers.get("referer", "/feeds/search")
    return RedirectResponse(ref, status_code=303)


@app.post("/feeds/{domain}/settings/age-mode")
async def web_set_age_mode(
    domain: str,
    mode: str = Form(...),
    days: Optional[int] = Form(None),
    start_date: Optional[str] = Form(None),
    end_date: Optional[str] = Form(None),
):
    await _feed_client.post(
        f"/api/domains/{domain}/age-settings",
        json={"mode": mode, "days": days, "start_date": start_date, "end_date": end_date},
    )
    return RedirectResponse(f"/feeds/{domain}", status_code=303)


@app.post("/feeds/{domain}/entries/delete-outside-calendar")
async def web_delete_outside_calendar(
    domain: str,
    start_date: str = Form(...),
    end_date: str = Form(...),
):
    await _feed_client.post(
        f"/api/domains/{domain}/entries/purge-outside-calendar",
        params={"start_date": start_date, "end_date": end_date},
    )
    return RedirectResponse(f"/feeds/{domain}", status_code=303)


# ---------------------------------------------------------------------------
# KoreFeed API proxy (called directly by browser JS)
# ---------------------------------------------------------------------------

@app.patch("/api/feeds/{feed_id}/rate")
async def api_proxy_feed_rate(feed_id: str, minutes: int):
    r = await _feed_client.patch(f"/api/feeds/{feed_id}/rate", params={"minutes": minutes})
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="Feed not found")
    return r.json()


# ===========================================================================
# KoreLibrary — Web UI
# ===========================================================================

@app.get("/library", response_class=HTMLResponse)
async def lib_index(request: Request, limit: int = 200, offset: int = 0):
    books_r, status_r = await asyncio.gather(
        _lib_client.get("/books", params={"limit": limit, "offset": offset}),
        _lib_client.get("/status"),
    )
    books  = books_r.json()  if books_r.status_code == 200  else []
    status = status_r.json() if status_r.status_code == 200 else {}
    return templates.TemplateResponse(
        request, "library_index.html",
        {
            "books":  books,
            "total":  status.get("total_books", len(books)),
            "limit":  limit,
            "offset": offset,
            "mode":   "all",
        },
    )


@app.get("/library/incomplete", response_class=HTMLResponse)
async def lib_incomplete(request: Request, fields: Optional[str] = None):
    params: dict = {}
    if fields:
        params["fields"] = fields
    r = await _lib_client.get("/incomplete", params=params)
    books = r.json() if r.status_code == 200 else []
    return templates.TemplateResponse(
        request, "library_index.html",
        {
            "books":         books,
            "total":         len(books),
            "limit":         9999,
            "offset":        0,
            "mode":          "incomplete",
            "filter_fields": fields,
        },
    )


@app.get("/library/search", response_class=HTMLResponse)
async def lib_search(
    request: Request,
    q: Optional[str] = None,
    author: Optional[str] = None,
    title: Optional[str] = None,
    year: Optional[str] = None,   # str to tolerate empty string from browser form
    language: Optional[str] = None,
    genre: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    year_int = _parse_year(year)
    results = []
    searched = any([q, author, title, year_int, language, genre])
    if searched:
        params: dict = {"limit": limit, "offset": offset}
        if q:        params["q"]        = q
        if author:   params["author"]   = author
        if title:    params["title"]    = title
        if year_int: params["year"]     = year_int
        if language: params["language"] = language
        if genre:    params["genre"]    = genre
        r = await _lib_client.get("/search", params=params)
        results = r.json() if r.status_code == 200 else []
    return templates.TemplateResponse(
        request, "library_search.html",
        {
            "results":  results,
            "searched": searched,
            "q":        q or "",
            "author":   author or "",
            "title":    title or "",
            "year":     year_int or "",
            "language": language or "",
            "genre":    genre or "",
            "limit":    limit,
        },
    )


@app.get("/library/import", response_class=HTMLResponse)
async def lib_import(request: Request, error: Optional[str] = None):
    return templates.TemplateResponse(
        request, "library_import.html",
        {"error": error},
    )


@app.post("/library/import/manual", response_class=HTMLResponse)
async def lib_import_manual(
    request: Request,
    title:    str           = Form(...),
    body:     Optional[str] = Form(None),
    author:   Optional[str] = Form(None),
    year:     Optional[str] = Form(None),
    language: Optional[str] = Form(None),
    genre:    Optional[str] = Form(None),
    notes:    Optional[str] = Form(None),
):
    payload: dict = {"title": title}
    if body:      payload["body"]      = body
    if author:    payload["author"]    = author
    year_int = _parse_year(year)
    if year_int is not None: payload["year"] = year_int
    if language:  payload["language"]  = language
    if genre:     payload["genre"]     = genre
    if notes:     payload["notes"]     = notes

    r = await _lib_client.post("/books", json=payload)
    if r.status_code in (200, 201):
        book_id = r.json().get("id")
        return RedirectResponse(url=f"/library/{book_id}", status_code=303)
    return templates.TemplateResponse(
        request, "library_import.html",
        {"error": r.json().get("detail", f"Error {r.status_code}")},
        status_code=400,
    )


@app.get("/library/kiwix/inventory")
async def lib_kiwix_inventory(kiwix_url: Optional[str] = None):
    params = {}
    if kiwix_url:
        params["kiwix_url"] = kiwix_url
    r = await _lib_client.get("/kiwix/inventory", params=params)
    return JSONResponse(content=r.json(), status_code=r.status_code)


@app.get("/library/kiwix/suggest")
async def lib_kiwix_suggest(zim: str, pattern: str = "", count: int = 100, kiwix_url: Optional[str] = None):
    params: dict = {"zim": zim, "pattern": pattern, "count": count}
    if kiwix_url:
        params["kiwix_url"] = kiwix_url
    r = await _lib_client.get("/kiwix/suggest", params=params)
    return JSONResponse(content=r.json(), status_code=r.status_code)


@app.get("/library/kiwix/search")
async def lib_kiwix_search(zim: str, q: str, count: int = 100, kiwix_url: Optional[str] = None):
    params: dict = {"zim": zim, "q": q, "count": count}
    if kiwix_url:
        params["kiwix_url"] = kiwix_url
    r = await _lib_client.get("/kiwix/search", params=params)
    return JSONResponse(content=r.json(), status_code=r.status_code)


@app.post("/library/import/kiwix")
async def lib_import_kiwix(request: Request):
    payload = await request.json()
    r = await _lib_client.post("/import/kiwix", json=payload)
    return JSONResponse(content=r.json(), status_code=r.status_code)


@app.post("/library/import/kiwix/viewer")
async def lib_import_kiwix_viewer(request: Request):
    payload = await request.json()
    r = await _lib_client.post("/import/kiwix/viewer", json=payload)
    return JSONResponse(content=r.json(), status_code=r.status_code)


@app.post("/library/import/kiwix/viewer/batch")
async def lib_import_kiwix_viewer_batch(request: Request):
    payload = await request.json()
    r = await _lib_client.post("/import/kiwix/viewer/batch", json=payload)
    return JSONResponse(content=r.json(), status_code=r.status_code)


@app.get("/library/{book_id}/edit", response_class=HTMLResponse)
async def lib_book_edit(request: Request, book_id: int):
    r = await _lib_client.get(f"/books/{book_id}")
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="Book not found")
    return templates.TemplateResponse(
        request, "library_edit.html",
        {"book": r.json(), "error": None},
    )


@app.post("/library/{book_id}/edit", response_class=HTMLResponse)
async def lib_book_edit_post(
    request:   Request,
    book_id:   int,
    title:     str           = Form(...),
    body:      Optional[str] = Form(None),
    author:    Optional[str] = Form(None),
    year:      Optional[str] = Form(None),
    language:  Optional[str] = Form(None),
    genre:     Optional[str] = Form(None),
    notes:     Optional[str] = Form(None),
    source:    Optional[str] = Form(None),
):
    payload: dict = {"title": title}
    if body is not None: payload["body"] = body
    if author:    payload["author"]    = author
    year_int = _parse_year(year)
    if year_int is not None: payload["year"] = year_int
    if language:  payload["language"]  = language
    if genre:     payload["genre"]     = genre
    if notes:     payload["notes"]     = notes
    if source:    payload["source"]    = source

    r = await _lib_client.patch(f"/books/{book_id}", json=payload)
    if r.status_code == 200:
        return RedirectResponse(url=f"/library/{book_id}", status_code=303)
    book_r = await _lib_client.get(f"/books/{book_id}")
    return templates.TemplateResponse(
        request, "library_edit.html",
        {"book": book_r.json(), "error": r.json().get("detail", f"Error {r.status_code}")},
        status_code=400,
    )


@app.post("/library/{book_id}/delete")
async def lib_book_delete(book_id: int):
    r = await _lib_client.delete(f"/books/{book_id}")
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=r.status_code, detail="Delete failed")
    return RedirectResponse(url="/library", status_code=303)


@app.post("/library/{book_id}/repair-anchors")
async def lib_repair_anchors(book_id: int):
    r = await _lib_client.post(f"/books/{book_id}/repair-anchors")
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="Book not found")
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail="Repair failed")
    return RedirectResponse(url=f"/library/{book_id}", status_code=303)


@app.get("/library/{book_id}", response_class=HTMLResponse)
async def lib_book(request: Request, book_id: int):
    r = await _lib_client.get(f"/books/{book_id}")
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="Book not found")
    return templates.TemplateResponse(
        request, "library_book.html",
        {"book": r.json()},
    )


# ===========================================================================
# KoreReference — Web UI
# ===========================================================================

@app.get("/reference/import", response_class=HTMLResponse)
async def ref_import(request: Request):
    status_r = await _ref_client.get("/import/status")
    status = status_r.json() if status_r.status_code == 200 else {}
    return templates.TemplateResponse(
        request, "reference_import.html",
        {"status": status},
    )


@app.post("/reference/import/crawl")
async def ref_import_crawl(request: Request):
    payload = await request.json()
    r = await _ref_client.post("/import/kiwix/crawl", json=payload)
    return JSONResponse(content=r.json(), status_code=r.status_code)


@app.post("/reference/import/wikipedia")
async def ref_import_wikipedia(request: Request):
    payload = await request.json()
    r = await _ref_client.post("/import/wikipedia/crawl", json=payload)
    return JSONResponse(content=r.json(), status_code=r.status_code)


@app.get("/reference/import/status")
async def ref_import_status():
    r = await _ref_client.get("/import/status")
    return JSONResponse(content=r.json(), status_code=r.status_code)


@app.post("/reference/import/stop")
async def ref_import_stop():
    r = await _ref_client.post("/import/stop")
    return JSONResponse(content=r.json(), status_code=r.status_code)


@app.get("/reference", response_class=HTMLResponse)
async def ref_index(request: Request, limit: int = 100, offset: int = 0):
    articles_r, status_r = await asyncio.gather(
        _ref_client.get("/articles", params={"limit": limit, "offset": offset}),
        _ref_client.get("/status"),
    )
    articles = articles_r.json() if articles_r.status_code == 200 else []
    status   = status_r.json()   if status_r.status_code == 200   else {}
    return templates.TemplateResponse(
        request, "reference_index.html",
        {
            "articles": articles,
            "total":    status.get("total_articles", len(articles)),
            "limit":    limit,
            "offset":   offset,
        },
    )


@app.get("/reference/search", response_class=HTMLResponse)
async def ref_search(
    request: Request,
    q: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
):
    results = []
    searched = bool(q)
    if searched:
        params: dict = {"q": q, "limit": limit, "offset": offset}
        r = await _ref_client.get("/search", params=params)
        results = r.json() if r.status_code == 200 else []
    return templates.TemplateResponse(
        request, "reference_search.html",
        {
            "results":  results,
            "searched": searched,
            "q":        q or "",
            "limit":    limit,
        },
    )


@app.get("/reference/categories", response_class=HTMLResponse)
async def ref_categories(request: Request):
    r = await _ref_client.get("/categories")
    categories = r.json() if r.status_code == 200 else []
    return templates.TemplateResponse(
        request, "reference_categories.html",
        {"categories": categories},
    )


@app.get("/reference/categories/{name}", response_class=HTMLResponse)
async def ref_category(request: Request, name: str, limit: int = 100, offset: int = 0):
    r = await _ref_client.get(f"/categories/{name}", params={"limit": limit, "offset": offset})
    articles = r.json() if r.status_code == 200 else []
    return templates.TemplateResponse(
        request, "reference_categories.html",
        {"categories": None, "category_name": name, "articles": articles,
         "limit": limit, "offset": offset},
    )


@app.get("/reference/new", response_class=HTMLResponse)
async def ref_article_new(request: Request):
    return templates.TemplateResponse(
        request, "reference_edit.html",
        {"article": None, "error": None},
    )


@app.post("/reference/new", response_class=HTMLResponse)
async def ref_article_new_post(
    request:     Request,
    title:       str            = Form(...),
    summary:     Optional[str]  = Form(None),
    body:        Optional[str]  = Form(None),
    categories:  Optional[str]  = Form(None),
    facts:       Optional[str]  = Form(None),
    redirect_to: Optional[str]  = Form(None),
):
    title = title.strip()
    f = _parse_article_form(body, summary, categories, redirect_to, facts)
    payload: dict = {"title": title}
    if f["body"]:        payload["body"]        = f["body"]
    if f["summary"]:     payload["summary"]     = f["summary"]
    if f["sections"]:    payload["sections"]    = f["sections"]
    if f["cats"]:        payload["categories"]  = f["cats"]
    if f["facts"]:       payload["facts"]       = f["facts"]
    if f["redirect_to"]: payload["redirect_to"] = f["redirect_to"]
    if f["links"]:       payload["link_titles"] = f["links"]
    r = await _ref_client.post("/articles", json=payload)
    if r.status_code in (200, 201):
        stored_title = ((r.json() or {}).get("title") or title)
        return RedirectResponse(url=f"/reference/{quote(stored_title, safe='')}", status_code=303)
    return templates.TemplateResponse(
        request, "reference_edit.html",
        {"article": None, "error": r.json().get("detail", f"Error {r.status_code}"),
         "form": {"title": title, "summary": summary or "", "body": f["body"] or "",
                  "categories": categories or "", "redirect_to": redirect_to or ""}},
        status_code=400,
    )


@app.get("/reference/{title}/edit", response_class=HTMLResponse)
async def ref_article_edit(request: Request, title: str):
    r = await _ref_client.get(f"/articles/{quote(title, safe='')}")
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail=f"Article not found: {title!r}")
    article = r.json()
    return templates.TemplateResponse(
        request, "reference_edit.html",
        {"article": article, "edit_body": _sections_to_edit_body(article), "error": None},
    )


@app.post("/reference/{title}/edit", response_class=HTMLResponse)
async def ref_article_edit_post(
    request:     Request,
    title:       str,
    summary:     Optional[str]  = Form(None),
    body:        Optional[str]  = Form(None),
    categories:  Optional[str]  = Form(None),
    facts:       Optional[str]  = Form(None),
    redirect_to: Optional[str]  = Form(None),
):
    f = _parse_article_form(body, summary, categories, redirect_to, facts)
    payload: dict = {"title": title}
    if f["body"] is not None: payload["body"]       = f["body"]
    if f["summary"]:          payload["summary"]    = f["summary"]
    payload["sections"]    = f["sections"] or []
    payload["categories"]  = f["cats"]
    payload["facts"]       = f["facts"]
    payload["link_titles"] = f["links"]
    if f["redirect_to"]:   payload["redirect_to"] = f["redirect_to"]
    r = await _ref_client.post("/articles", json=payload)
    if r.status_code in (200, 201):
        stored_title = ((r.json() or {}).get("title") or title)
        return RedirectResponse(url=f"/reference/{quote(stored_title, safe='')}", status_code=303)
    art_r = await _ref_client.get(f"/articles/{quote(title, safe='')}")
    article = art_r.json() if art_r.status_code == 200 else None
    return templates.TemplateResponse(
        request, "reference_edit.html",
        {"article": article, "edit_body": _sections_to_edit_body(article or {}),
         "error": r.json().get("detail", f"Error {r.status_code}")},
        status_code=400,
    )


@app.post("/reference/delete-all")
async def ref_delete_all():
    r = await _ref_client.delete("/articles")
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=r.status_code, detail="Delete failed")
    return RedirectResponse(url="/reference", status_code=303)


@app.post("/reference/{title}/delete")
async def ref_article_delete(title: str):
    r = await _ref_client.delete(f"/articles/{quote(title, safe='')}")
    if r.status_code not in (200, 204):
        raise HTTPException(status_code=r.status_code, detail="Delete failed")
    return RedirectResponse(url="/reference", status_code=303)


@app.get("/reference/{title}/links-json")
async def ref_article_links_json(title: str):
    r = await _ref_client.get(f"/articles/{quote(title, safe='')}/links")
    if r.status_code == 404:
        return JSONResponse([])
    return JSONResponse(r.json())


@app.get("/reference/{title}", response_class=HTMLResponse)
async def ref_article(request: Request, title: str):
    r = await _ref_client.get(f"/articles/{quote(title, safe='')}")
    if r.status_code == 404:
        raise HTTPException(status_code=404, detail=f"Article not found: {title!r}")
    article = r.json()
    # Fetch backlinks count (lightweight)
    bl_r = await _ref_client.get(f"/articles/{quote(title, safe='')}/backlinks", params={"limit": 10})
    backlinks = bl_r.json() if bl_r.status_code == 200 else []
    return templates.TemplateResponse(
        request, "reference_article.html",
        {"article": article, "backlinks": backlinks},
    )


# ===========================================================================
# Gateway status
# ===========================================================================

@app.get("/status")
async def gateway_status():
    kf_r, kl_r, kr_r = await asyncio.gather(
        _feed_client.get("/status", timeout=3.0),
        _lib_client.get("/status", timeout=3.0),
        _ref_client.get("/status", timeout=3.0),
        return_exceptions=True,
    )
    return {
        "service": "KoreDataGateway",
        "version": __version__,
        "children": {
            "korefeed":      _svc_status(kf_r, cfg["korefeed_url"]),
            "korelibrary":   _svc_status(kl_r, cfg["korelibrary_url"]),
            "korereference": _svc_status(kr_r, cfg["korereference_url"]),
        },
    }
