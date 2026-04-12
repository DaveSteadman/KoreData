from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import JSONResponse
from pydantic import BaseModel, HttpUrl

from app.database import (
    delete_domain_db,
    delete_entries_by_feed,
    delete_entries_by_ids,
    delete_entries_older_than,
    delete_entries_outside_calendar,
    delete_entry,
    get_domain_age_settings,
    get_entries,
    get_entry,
    get_entry_count,
    get_feed_counts,
    get_recent_entries,
    init_db,
    list_domains,
    rename_domain_db,
    search_entries,
    set_domain_age_settings,
)
from app.feed_manager import (
    add_feed,
    create_domain,
    delete_domain_feeds,
    get_feed,
    list_feed_domains,
    load_feeds,
    load_feeds_for_domain,
    remove_feed,
    rename_domain_feeds,
    update_feed,
    update_feed_rate,
)
from app.ingest import schedule_feeds, scheduler, start_scheduler, trigger_immediate
@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Migrate all existing databases (adds `deleted` column if absent, etc.)
    for _domain in list_domains():
        init_db(_domain)
    start_scheduler()
    yield
    if scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(
    title="MiniFeed",
    description="RSS ingest server for LLM agents",
    lifespan=_lifespan,
)

# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------


@app.get("/status", tags=["meta"])
def api_status():
    """Health check used by KoreDataGateway."""
    domains = list_domains()
    all_feeds = load_feeds()
    total_entries = sum(get_entry_count(d) for d in domains)
    return {
        "status": "ok",
        "service": "KoreFeed",
        "total_domains": len(domains),
        "total_feeds": len(all_feeds),
        "total_entries": total_entries,
    }


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    return await http_exception_handler(request, exc)


@app.exception_handler(Exception)
async def _generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={"detail": f"{type(exc).__name__}: {exc}"},
    )


# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------


class FeedCreate(BaseModel):
    domain: str
    name: str
    url: HttpUrl
    update_rate: int = 60  # minutes
    feed_type: str = "rss"


@app.get("/api/feeds", tags=["feeds"])
def api_list_feeds():
    """Return all configured RSS feeds."""
    return load_feeds()


@app.post("/api/feeds", status_code=201, tags=["feeds"])
def api_add_feed(body: FeedCreate):
    """Add a new RSS feed to the inventory."""
    feed = add_feed(body.domain, body.name, str(body.url), body.update_rate, feed_type=body.feed_type)
    init_db(body.domain)
    schedule_feeds()
    trigger_immediate(feed)
    return feed


@app.delete("/api/feeds/{feed_id}", tags=["feeds"])
def api_remove_feed(feed_id: str):
    """Remove a feed from the inventory."""
    if not remove_feed(feed_id):
        raise HTTPException(status_code=404, detail="Feed not found")
    schedule_feeds()
    return {"deleted": feed_id}


class FeedUpdate(BaseModel):
    name: str
    url: HttpUrl
    update_rate: int = 60
    feed_type: str = "rss"


@app.put("/api/feeds/{feed_id}", tags=["feeds"])
def api_update_feed(feed_id: str, body: FeedUpdate):
    """Update name, url, update_rate and type for an existing feed."""
    updated = update_feed(feed_id, body.name, str(body.url), body.update_rate, body.feed_type)
    if updated is None:
        raise HTTPException(status_code=404, detail="Feed not found")
    schedule_feeds()
    return updated


@app.get("/api/domains", tags=["domains"])
def api_list_domains():
    """List all domains with entry counts."""
    db_domains  = set(list_domains())
    feed_domains = set(list_feed_domains())
    all_domains = sorted(db_domains | feed_domains)
    return [{"domain": d, "entry_count": get_entry_count(d)} for d in all_domains]


@app.post("/api/domains", status_code=201, tags=["domains"])
def api_create_domain(domain: str):
    """Create a new empty domain."""
    create_domain(domain)
    init_db(domain)
    return {"domain": domain}


@app.delete("/api/domains/{domain}", tags=["domains"])
def api_delete_domain(domain: str):
    """Delete a domain, its feed list, and its database."""
    delete_domain_feeds(domain)
    delete_domain_db(domain)
    schedule_feeds()
    return {"deleted": domain}


@app.post("/api/domains/{domain}/rename", tags=["domains"])
def api_rename_domain(domain: str, new_name: str):
    """Rename a domain."""
    rename_domain_feeds(domain, new_name)
    rename_domain_db(domain, new_name)
    schedule_feeds()
    return {"renamed": new_name}


@app.get("/api/domains/{domain}/entries", tags=["content"])
def api_get_entries(domain: str, limit: int = 50, offset: int = 0):
    """Paginated list of entries for a domain."""
    return get_entries(domain, limit=limit, offset=offset)


@app.get("/api/domains/{domain}/entries/{entry_id}", tags=["content"])
def api_get_entry(domain: str, entry_id: int):
    """Fetch a single entry by ID."""
    entry = get_entry(domain, entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Entry not found")
    return entry


@app.delete("/api/domains/{domain}/entries/{entry_id}", tags=["content"])
def api_delete_entry(domain: str, entry_id: int):
    """Delete a single entry by ID."""
    if not delete_entry(domain, entry_id):
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"deleted": entry_id}


@app.delete("/api/domains/{domain}/entries", tags=["content"])
def api_delete_entries(
    domain: str,
    feed_name: Optional[str] = None,
    older_than_days: Optional[float] = None,
):
    """Bulk-delete entries by feed name or age (provide exactly one filter)."""
    if feed_name:
        count = delete_entries_by_feed(domain, feed_name)
        return {"deleted": count, "filter": "feed_name", "value": feed_name}
    if older_than_days is not None:
        if older_than_days <= 0:
            raise HTTPException(status_code=400, detail="older_than_days must be > 0")
        count = delete_entries_older_than(domain, older_than_days)
        return {"deleted": count, "filter": "older_than_days", "value": older_than_days}
    raise HTTPException(status_code=400, detail="Provide feed_name or older_than_days")


@app.post("/api/domains/{domain}/entries/bulk-delete", tags=["content"])
def api_bulk_delete_entries(domain: str, ids: list[int]):
    """Delete multiple entries by ID list."""
    count = delete_entries_by_ids(domain, ids)
    return {"deleted": count}


@app.get("/api/search", tags=["content"])
def api_search(
    q: str,
    domain: Optional[str] = None,
    limit: int = 50,
    full: bool = False,
    since: Optional[str] = None,
    until: Optional[str] = None,
):
    """Full-text search across headline and page text.

    Set full=true to include page_text in each result.
    since / until are ISO 8601 date strings (YYYY-MM-DD) applied against the
    entry published date.  Either or both may be omitted.
    """
    return search_entries(domain or None, q, limit=limit, include_body=full,
                          since=since, until=until)


@app.get("/api/recent", tags=["content"])
def api_recent(domain: Optional[str] = None, hours: float = 24.0, limit: int = 50):
    """Return entries ingested within the last N hours, newest first.

    Searches all domains unless domain is specified.
    """
    if hours <= 0:
        raise HTTPException(status_code=400, detail="hours must be greater than 0")
    return get_recent_entries(domain or None, hours=hours, limit=limit)


# ---------------------------------------------------------------------------
# Additional API endpoints (used by KoreDataGateway)
# ---------------------------------------------------------------------------


@app.patch("/api/feeds/{feed_id}/rate", tags=["feeds"])
def api_update_feed_rate(feed_id: str, minutes: int):
    if minutes < 1:
        raise HTTPException(status_code=400, detail="minutes must be >= 1")
    if not update_feed_rate(feed_id, minutes):
        raise HTTPException(status_code=404, detail="Feed not found")
    schedule_feeds()
    return {"ok": True, "update_rate": minutes}


@app.post("/api/feeds/{feed_id}/trigger", tags=["feeds"])
def api_trigger_feed(feed_id: str):
    """Manually trigger an immediate fetch for a feed."""
    feed = get_feed(feed_id)
    if not feed:
        raise HTTPException(status_code=404, detail="Feed not found")
    trigger_immediate(feed)
    return {"triggered": feed_id}


@app.get("/api/domains/{domain}/age-settings", tags=["domains"])
def api_get_age_settings(domain: str):
    """Get entry age/date filter settings for a domain."""
    return get_domain_age_settings(domain)


class AgeSettingsBody(BaseModel):
    mode: str
    days: Optional[int] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


@app.post("/api/domains/{domain}/age-settings", tags=["domains"])
def api_set_age_settings(domain: str, body: AgeSettingsBody):
    """Set entry age/date filter settings for a domain."""
    valid = {"none", "days_previous", "calendar_period"}
    if body.mode not in valid:
        raise HTTPException(status_code=400, detail=f"mode must be one of: {', '.join(valid)}")
    set_domain_age_settings(
        domain, body.mode,
        days=body.days,
        start_date=body.start_date or None,
        end_date=body.end_date or None,
    )
    return {"ok": True}


@app.get("/api/domains/{domain}/feed-counts", tags=["domains"])
def api_feed_counts(domain: str):
    """Return per-feed entry counts for a domain."""
    return get_feed_counts(domain)


@app.post("/api/domains/{domain}/entries/purge-outside-calendar", tags=["content"])
def api_purge_outside_calendar(domain: str, start_date: str, end_date: str):
    """Delete all entries published outside the given date range."""
    delete_entries_outside_calendar(domain, start_date, end_date)
    return {"ok": True}


# (Web UI removed — all UI is now served by KoreDataGateway)

@app.patch("/api/feeds/{feed_id}/rate", tags=["feeds"])
def api_update_feed_rate(feed_id: str, minutes: int):
    if minutes < 1:
        raise HTTPException(status_code=400, detail="minutes must be >= 1")
    if not update_feed_rate(feed_id, minutes):
        raise HTTPException(status_code=404, detail="Feed not found")
    schedule_feeds()
    return {"ok": True, "update_rate": minutes}


@app.post("/api/feeds/{feed_id}/trigger", tags=["feeds"])
def api_trigger_feed(feed_id: str):
    """Manually trigger an immediate fetch for a feed."""
    feed = get_feed(feed_id)
    if not feed:
        raise HTTPException(status_code=404, detail="Feed not found")
    trigger_immediate(feed)
    return {"triggered": feed_id}


@app.get("/api/domains/{domain}/age-settings", tags=["domains"])
def api_get_age_settings(domain: str):
    """Get entry age/date filter settings for a domain."""
    return get_domain_age_settings(domain)


class AgeSettingsBody(BaseModel):
    mode: str
    days: Optional[int] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


@app.post("/api/domains/{domain}/age-settings", tags=["domains"])
def api_set_age_settings(domain: str, body: AgeSettingsBody):
    """Set entry age/date filter settings for a domain."""
    valid = {"none", "days_previous", "calendar_period"}
    if body.mode not in valid:
        raise HTTPException(status_code=400, detail=f"mode must be one of: {', '.join(valid)}")
    set_domain_age_settings(
        domain, body.mode,
        days=body.days,
        start_date=body.start_date or None,
        end_date=body.end_date or None,
    )
    return {"ok": True}


@app.get("/api/domains/{domain}/feed-counts", tags=["domains"])
def api_feed_counts(domain: str):
    """Return per-feed entry counts for a domain."""
    return get_feed_counts(domain)


@app.post("/api/domains/{domain}/entries/purge-outside-calendar", tags=["content"])
def api_purge_outside_calendar(domain: str, start_date: str, end_date: str):
    """Delete all entries published outside the given date range."""
    delete_entries_outside_calendar(domain, start_date, end_date)
    return {"ok": True}

# (Web UI removed — all UI is now served by KoreDataGateway)
