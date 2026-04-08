import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from app.config import cfg

FEEDS_DIR = Path(cfg["feeds_dir"])


def _domain_file(domain: str) -> Path:
    """Return the path for a domain's feed file, sanitising the name."""
    safe = re.sub(r"[^\w\-]", "_", domain)
    return FEEDS_DIR / f"{safe}.json"


def _load_domain_file(domain: str) -> list[dict]:
    path = _domain_file(domain)
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _save_domain_file(domain: str, feeds: list[dict]) -> None:
    FEEDS_DIR.mkdir(exist_ok=True)
    with open(_domain_file(domain), "w", encoding="utf-8") as f:
        json.dump(feeds, f, indent=2)


def load_feeds() -> list[dict]:
    """Return all feeds from every domain file."""
    FEEDS_DIR.mkdir(exist_ok=True)
    result: list[dict] = []
    for path in sorted(FEEDS_DIR.glob("*.json")):
        with open(path, encoding="utf-8") as f:
            result.extend(json.load(f))
    return result


def load_feeds_for_domain(domain: str) -> list[dict]:
    return _load_domain_file(domain)


def list_feed_domains() -> list[str]:
    """Return domain names that have a feeds JSON file."""
    FEEDS_DIR.mkdir(exist_ok=True)
    return [p.stem for p in sorted(FEEDS_DIR.glob("*.json"))]


def get_feed(feed_id: str) -> Optional[dict]:
    return next((f for f in load_feeds() if f["id"] == feed_id), None)


def add_feed(domain: str, name: str, url: str, update_rate: int, feed_type: str = "rss") -> dict:
    feeds = _load_domain_file(domain)
    feed = {
        "id": str(uuid.uuid4()),
        "domain": domain,
        "name": name,
        "url": url,
        "update_rate": update_rate,
        "type": feed_type,
    }
    feeds.append(feed)
    _save_domain_file(domain, feeds)
    return feed


def remove_feed(feed_id: str) -> bool:
    FEEDS_DIR.mkdir(exist_ok=True)
    for path in FEEDS_DIR.glob("*.json"):
        with open(path, encoding="utf-8") as f:
            feeds = json.load(f)
        new_feeds = [f for f in feeds if f["id"] != feed_id]
        if len(new_feeds) < len(feeds):
            with open(path, "w", encoding="utf-8") as f:
                json.dump(new_feeds, f, indent=2)
            return True
    return False


# ---------------------------------------------------------------------------
# Domain lifecycle
# ---------------------------------------------------------------------------

def create_domain(domain: str) -> bool:
    """Create an empty feed file for a new domain. Returns False if it already exists."""
    path = _domain_file(domain)
    FEEDS_DIR.mkdir(exist_ok=True)
    if path.exists():
        return False
    with open(path, "w", encoding="utf-8") as f:
        json.dump([], f, indent=2)
    return True


def delete_domain_feeds(domain: str) -> bool:
    """Delete the feed file for a domain. Returns False if it didn't exist."""
    path = _domain_file(domain)
    if not path.exists():
        return False
    path.unlink()
    return True


def update_feed_last_fetched(feed_id: str) -> None:
    """Record the current UTC time as last_fetched_at for the given feed."""
    FEEDS_DIR.mkdir(exist_ok=True)
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    for path in FEEDS_DIR.glob("*.json"):
        with open(path, encoding="utf-8") as f:
            feeds = json.load(f)
        for feed in feeds:
            if feed["id"] == feed_id:
                feed["last_fetched_at"] = now
                with open(path, "w", encoding="utf-8") as fh:
                    json.dump(feeds, fh, indent=2)
                return


def update_feed_status(
    feed_id: str,
    status: str,                  # "ok" | "error"
    error: Optional[str] = None,
    duration_s: Optional[float] = None,
    new_entries: Optional[int] = None,
    content_status: Optional[str] = None,  # "good" | "poor" | "none"
) -> None:
    """Persist the outcome of the most recent ingest attempt for a feed."""
    FEEDS_DIR.mkdir(exist_ok=True)
    for path in FEEDS_DIR.glob("*.json"):
        with open(path, encoding="utf-8") as f:
            feeds = json.load(f)
        for feed in feeds:
            if feed["id"] == feed_id:
                feed["last_status"] = status
                feed["last_error"] = error
                feed["last_duration_s"] = round(duration_s, 1) if duration_s is not None else None
                feed["last_new_entries"] = new_entries
                if content_status is not None:
                    feed["content_status"] = content_status
                with open(path, "w", encoding="utf-8") as fh:
                    json.dump(feeds, fh, indent=2)
                return

def update_feed_rate(feed_id: str, minutes: int) -> bool:
    """Update update_rate for a feed and persist. Returns True if found."""
    FEEDS_DIR.mkdir(exist_ok=True)
    for path in FEEDS_DIR.glob("*.json"):
        with open(path, encoding="utf-8") as f:
            feeds = json.load(f)
        for feed in feeds:
            if feed["id"] == feed_id:
                feed["update_rate"] = minutes
                with open(path, "w", encoding="utf-8") as fh:
                    json.dump(feeds, fh, indent=2)
                return True
    return False


def rename_domain_feeds(old: str, new: str) -> bool:
    """Rename a domain's feed file and update the domain field in every feed entry."""
    old_path = _domain_file(old)
    if not old_path.exists():
        return False
    new_path = _domain_file(new)
    feeds = _load_domain_file(old)
    for f in feeds:
        f["domain"] = new
    with open(new_path, "w", encoding="utf-8") as fh:
        json.dump(feeds, fh, indent=2)
    old_path.unlink()
    return True
