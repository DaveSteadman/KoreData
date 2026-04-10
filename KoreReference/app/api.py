from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.config import cfg
from app.database import (
    delete_all_articles,
    delete_article,
    get_article_by_title,
    get_backlinks,
    get_links,
    get_random_article,
    get_status,
    get_unresolved_link_titles,
    init_db,
    list_articles,
    resolve_article,
    resolve_links,
    search_articles,
    upsert_article,
)
from app.importers.kiwix import (
    import_one,
    parse_seed_url,
    run_kiwix_backfill,
    run_kiwix_crawl,
    run_kiwix_import,
)
from app.importers.state import import_lock, import_state

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
# Pydantic models
# ---------------------------------------------------------------------------

class ArticleCreate(BaseModel):
    title: str
    body: Optional[str] = None
    summary: Optional[str] = None
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


@app.post("/articles", status_code=201, summary="Add or upsert an article")
def route_upsert_article(data: ArticleCreate):
    return upsert_article(
        title=data.title,
        body=data.body,
        summary=data.summary,
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
    limit: int = 20,
    offset: int = 0,
):
    if not any([q, title]):
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of: q, title",
        )
    return search_articles(q=q, title=title, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

@app.post("/import/kiwix", summary="Trigger import from configured Kiwix server")
def route_import_kiwix(req: KiwixImportRequest, background_tasks: BackgroundTasks):
    if not import_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Import already running")
    import_state.update({
        "running": True, "done": 0, "total": 0,
        "errors": 0, "last_error": None, "mode": "prefix", "seed": None,
        "redirects_stored": 0, "last_redirect": None,
    })
    import_lock.release()
    background_tasks.add_task(
        run_kiwix_import, req.zim_name, req.titles, req.prefix, req.limit, req.resume
    )
    return {"started": True, "zim_name": req.zim_name}


@app.post("/import/kiwix/crawl", status_code=202, summary="BFS crawl from a Kiwix article URL")
def route_import_kiwix_crawl(req: KiwixCrawlRequest, background_tasks: BackgroundTasks):
    if not import_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Import already running")
    try:
        _, _, start_title = parse_seed_url(req.seed_url)
    except ValueError as exc:
        import_lock.release()
        raise HTTPException(status_code=422, detail=str(exc))
    import_state.update({
        "running": True, "done": 0, "total": 1,
        "errors": 0, "last_error": None, "mode": "crawl", "seed": start_title,
        "redirects_stored": 0, "last_redirect": None,
    })
    import_lock.release()
    background_tasks.add_task(
        run_kiwix_crawl, req.seed_url, req.max_depth, req.limit, req.resume
    )
    return {"started": True, "seed": start_title, "max_depth": req.max_depth, "limit": req.limit}



@app.post("/import/stop", summary="Abort in-progress import or crawl")
def route_import_stop():
    if import_state.get("running"):
        import_state["running"] = False
        return {"stopped": True}
    return {"stopped": False, "detail": "No import was running"}



@app.post("/import/article", status_code=201, summary="Import a single article by title from Kiwix")
def route_import_article(zim_name: str, title: str):
    """Synchronous single-article import — useful for testing and on-demand fetch."""
    kiwix_base = cfg["kiwix_url"].rstrip("/")
    try:
        with httpx.Client(timeout=30.0, follow_redirects=False) as client:
            import_one(client, kiwix_base, zim_name, title, resume=True)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Kiwix returned {exc.response.status_code} for {title!r}",
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return get_article_by_title(title, full=False)


class KiwixBackfillRequest(BaseModel):
    zim_name: str
    limit: int = 10_000


@app.post("/import/kiwix/backfill", status_code=202, summary="Fetch unresolved link targets from Kiwix")
def route_import_kiwix_backfill(req: KiwixBackfillRequest, background_tasks: BackgroundTasks):
    """Fetch every link target that exists in the links table but has no article row.

    This repairs the historical gap where redirect pages were silently dropped during
    import.  Titles like 'colour', 'NYC', 'World War 2' are stored as links.to_title
    but were never imported; this endpoint fetches each one and stores it — either as
    a redirect row or as a full article if it turns out to be a real page.
    """
    if not import_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Import already running")
    pending = get_unresolved_link_titles(limit=req.limit)
    if not pending:
        import_lock.release()
        return {"started": False, "detail": "No unresolved link targets found"}
    import_state.update({
        "running": True, "done": 0, "total": len(pending),
        "errors": 0, "last_error": None, "mode": "backfill", "seed": None,
        "redirects_stored": 0, "last_redirect": None,
    })
    import_lock.release()
    background_tasks.add_task(run_kiwix_backfill, req.zim_name, req.limit)
    return {"started": True, "pending": len(pending), "zim_name": req.zim_name}


@app.get("/import/status", summary="Progress of in-progress import")
def route_import_status():
    return dict(import_state)


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
