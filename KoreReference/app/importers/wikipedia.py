import random
import time
from collections import deque
from typing import Optional
from urllib.parse import unquote, urlparse

import httpx
from bs4 import BeautifulSoup

from app.database import get_article_by_title, get_links, resolve_links, upsert_article
from app.importers.shared import extract_article_html, extract_facts, remove_noise
from app.importers.state import import_state


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

def is_wikipedia_url(url: str) -> bool:
    """Return True if url points to a Wikipedia article."""
    try:
        return urlparse(url.strip()).netloc.lower().endswith(".wikipedia.org")
    except Exception:
        return False


def parse_wikipedia_url(url: str) -> tuple[str, str]:
    """Return (language_code, article_title) from a Wikipedia article URL.

    Accepts https://en.wikipedia.org/wiki/Title and mobile https://en.m.wikipedia.org/wiki/Title.
    """
    p = urlparse(url.strip())
    lang = p.netloc.lower().split(".")[0]  # "en" from "en.wikipedia.org"
    if "/wiki/" not in p.path:
        raise ValueError(f"Not a Wikipedia article URL: {url!r}")
    raw = p.path.split("/wiki/", 1)[1].split("/")[0]
    if not raw:
        raise ValueError(f"Cannot extract title from URL: {url!r}")
    return lang, unquote(raw).replace("_", " ").strip()


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def parse_wikipedia_article(
    html: str, title: str, api_categories: list[str], api_links: list[str]
) -> dict:
    """Parse Wikipedia Action API rendered HTML.

    Categories and links come from structured API data so they are complete and
    accurate even though the rendered HTML omits #mw-normal-catlinks.
    """
    soup = BeautifulSoup(html, "html.parser")
    remove_noise(soup)

    # Rewrite /wiki/Title hrefs to [[wikilink]] markup; skip namespace pages (contain ':').
    for a in soup.find_all("a", href=True):
        href = a["href"].split("#")[0]
        if not href.startswith("/wiki/"):
            continue
        raw = href[6:]
        if not raw:
            continue
        decoded = unquote(raw).replace("_", " ").strip()
        if not decoded or ":" in decoded:
            continue
        display = a.get_text(strip=True)
        if decoded and display:
            wikilink = f"[[{decoded}]]" if display == decoded else f"[[{display}|{decoded}]]"
            a.replace_with(wikilink)

    facts = extract_facts(soup)
    content_div = (
        soup.find(id="mw-content-text")
        or soup.find(class_="mw-parser-output")
        or soup.find("body")
        or soup
    )
    body, sections, summary = extract_article_html(content_div)

    return {
        "body": body,
        "summary": summary,
        "sections": sections,
        "categories": api_categories,
        "link_titles": api_links,
        "facts": facts,
    }


# ---------------------------------------------------------------------------
# Import worker
# ---------------------------------------------------------------------------

def run_wikipedia_crawl(
    seed_url: str,
    max_depth: int,
    limit: int,
    resume: bool,
    rate_min: float,
    rate_max: float,
) -> None:
    """BFS crawl from a Wikipedia article URL using the MediaWiki Action API.

    Sleeps random.uniform(rate_min, rate_max) seconds between HTTP requests
    to be a polite bot per Wikipedia's API etiquette guidelines.
    """
    try:
        lang, start_title = parse_wikipedia_url(seed_url)
    except ValueError as exc:
        import_state.update({"running": False, "last_error": str(exc)})
        return

    api_base = f"https://{lang}.wikipedia.org/w/api.php"
    headers = {
        "User-Agent": (
            "KoreData/1.0 (https://github.com/KoreData; reference-importer) "
            f"python-httpx/{httpx.__version__}"
        ),
    }

    queue: deque[tuple[str, int]] = deque([(start_title, 0)])
    visited: set[str] = {start_title}
    import_state["total"] = 1
    _already_fetched = False  # first HTTP request is immediate; subsequent ones wait

    with httpx.Client(timeout=30.0, follow_redirects=True, headers=headers) as client:
        while queue and import_state["running"]:
            if import_state["done"] >= limit:
                break

            title, depth = queue.popleft()

            # Resume: article already in DB — skip HTTP fetch
            if resume:
                existing = get_article_by_title(title, full=False)
                if existing is not None:
                    db_links = get_links(title) if depth < max_depth else []
                    if db_links or depth >= max_depth:
                        import_state["done"] += 1
                        for lnk in db_links:
                            lt = (lnk.get("to_title") or "").strip()
                            if lt and lt not in visited:
                                if import_state["done"] + len(queue) < limit:
                                    visited.add(lt)
                                    queue.append((lt, depth + 1))
                        import_state["total"] = max(import_state["total"], len(visited))
                        continue

            # Polite delay — never before the very first HTTP request
            if _already_fetched:
                time.sleep(random.uniform(rate_min, rate_max))
            _already_fetched = True

            try:
                params = {
                    "action": "parse",
                    "page": title,
                    "prop": "text|categories|links",
                    "format": "json",
                    "maxlag": "5",
                    "redirects": "1",
                }
                resp = client.get(api_base, params=params)
                resp.raise_for_status()
                data = resp.json()
                if "error" in data:
                    raise ValueError(data["error"].get("info", str(data["error"])))

                parsed_api = data.get("parse", {})
                # Skip redirect articles — their canonical target will be crawled
                # under its own title when encountered in the link graph.
                if parsed_api.get("redirects"):
                    import_state["done"] += 1
                    continue
                html_text  = parsed_api.get("text", {}).get("*", "")
                # Categories: exclude hidden maintenance categories
                api_categories = [
                    c["*"].replace("_", " ")
                    for c in parsed_api.get("categories", [])
                    if "hidden" not in c
                ]
                # Links: article namespace (ns=0) only, must exist on the wiki
                api_links = [
                    lnk["*"]
                    for lnk in parsed_api.get("links", [])
                    if lnk.get("ns") == 0 and "exists" in lnk
                ]

                parsed = parse_wikipedia_article(html_text, title, api_categories, api_links)
                upsert_article(
                    title=title,
                    body=parsed["body"],
                    summary=parsed["summary"],
                    sections=parsed["sections"],
                    categories=parsed["categories"],
                    facts=parsed["facts"],
                    link_titles=parsed["link_titles"],
                )
                import_state["done"] += 1

                if depth < max_depth:
                    for lt in parsed["link_titles"]:
                        lt = lt.strip()
                        if lt and lt not in visited:
                            if import_state["done"] + len(queue) < limit:
                                visited.add(lt)
                                queue.append((lt, depth + 1))
                    import_state["total"] = max(import_state["total"], len(visited))

            except Exception as exc:
                import_state["errors"] += 1
                import_state["last_error"] = f"{title}: {exc}"

    resolve_links(limit=500_000)
    import_state["running"] = False
