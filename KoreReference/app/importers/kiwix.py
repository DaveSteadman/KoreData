from collections import deque
from typing import Optional
from urllib.parse import unquote, urlparse

import httpx
from bs4 import BeautifulSoup

from app.config import cfg
from app.database import get_article_by_title, get_links, get_unresolved_link_titles, resolve_links, upsert_article
from app.importers.shared import extract_article_html, extract_facts, remove_noise
from app.importers.state import import_state


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

def parse_seed_url(seed_url: str) -> tuple[str, str, str]:
    """Parse a Kiwix seed into (kiwix_base, zim_name, start_title).

    Accepts these formats:
      http://host/viewer#zim_name/Article_Title        (Kiwix viewer fragment URL)
      http://host/content/zim_name/Article_Title        (new direct content URL)
      http://host/zim_name/A/Article_Title              (old direct content URL)
    """
    p = urlparse(seed_url.strip())
    base = f"{p.scheme}://{p.netloc}"
    if p.fragment:
        parts = p.fragment.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Cannot parse URL fragment: {p.fragment!r}")
        zim_name, raw_title = parts
        title = unquote(raw_title).replace("_", " ").split("#")[0].strip()
        return base, zim_name, title
    path_parts = p.path.strip("/").split("/")
    # New format: /content/<zim_name>/<Title>
    if len(path_parts) >= 3 and path_parts[0] == "content":
        zim_name = path_parts[1]
        raw_title = "/".join(path_parts[2:])
        title = unquote(raw_title).replace("_", " ").split("#")[0].strip()
        return base, zim_name, title
    # Old format: /<zim_name>/A/<Title>
    if len(path_parts) >= 3 and path_parts[1].upper() == "A":
        zim_name = path_parts[0]
        raw_title = "/".join(path_parts[2:])
        title = unquote(raw_title).replace("_", " ").split("#")[0].strip()
        return base, zim_name, title
    raise ValueError(
        f"Unrecognised Kiwix URL: {seed_url!r}. "
        "Expected http://host/viewer#zim/Title or http://host/content/zim/Title"
    )


def article_url(kiwix_base: str, zim_name: str, title: str) -> str:
    """Kiwix serves articles at /content/<zim_name>/<Title_With_Underscores>."""
    return f"{kiwix_base}/content/{zim_name}/{title.replace(' ', '_')}"


def suggest_titles(
    client: httpx.Client, kiwix_base: str, zim_name: str, prefix: str, limit: int
) -> list[str]:
    """Enumerate article titles via the Kiwix suggestion API.

    GET /suggest?content=<zim>&pattern=<prefix>&count=<n>
    Returns a JSON list of {"label": title, "value": title, "url": ...}
    """
    resp = client.get(
        f"{kiwix_base}/suggest",
        params={"content": zim_name, "pattern": prefix, "count": limit},
    )
    resp.raise_for_status()
    return [
        item["label"]
        for item in resp.json()
        if isinstance(item, dict) and item.get("label")
    ]


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def _resolve_href(href: str) -> Optional[str]:
    """Extract the article slug from a Kiwix internal href, or None if not an article link.

    Handles ../A/Title (old ZIM), ./Title, bare Title (new ZIM formats).
    """
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
        return None
    return raw if raw and "/" not in raw else None


def parse_kiwix_article(html: str, title: str) -> dict:
    """Extract body, summary, sections, categories, facts, and wikilinks from Kiwix HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Kiwix serves redirect articles as a meta-refresh page with an empty <body>.
    # httpx only follows HTTP 3xx, not <meta http-equiv="refresh">, so we detect
    # and bail out before wasting time on a parse that will produce no content.
    meta_refresh = soup.find("meta", attrs={"http-equiv": lambda v: v and v.lower() == "refresh"})
    if meta_refresh:
        redirect_to = None
        content = meta_refresh.get("content", "")
        lower = content.lower()
        if "url=" in lower:
            url_part = content[lower.index("url=") + 4:].strip()
            raw = _resolve_href(url_part)
            if raw:
                redirect_to = unquote(raw).replace("_", " ").strip()
        return {"redirect": True, "redirect_to": redirect_to, "body": "", "summary": None,
                "link_titles": [], "facts": []}

    remove_noise(soup)

    # Single pass: collect link titles and rewrite <a> tags to [[wikilink]] markup
    link_titles: list[str] = []
    seen_links: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = unquote(a["href"]).split("#")[0].strip()
        if not href or "://" in href or href.startswith("mailto:") or href.startswith("/"):
            continue
        raw = _resolve_href(href)
        if raw is None:
            continue
        target = raw.replace("_", " ").strip()
        if not target:
            continue
        if target not in seen_links and target != title:
            link_titles.append(target)
            seen_links.add(target)
        display = a.get_text(strip=True)
        if display:
            wikilink = f"[[{target}]]" if display == target else f"[[{display}|{target}]]"
            a.replace_with(wikilink)

    facts = extract_facts(soup)
    content_div = soup.find(id="mw-content-text") or soup.find("body") or soup
    body, summary = extract_article_html(content_div)

    return {
        "body": body,
        "summary": summary,
        "link_titles": link_titles,
        "facts": facts,
    }


# ---------------------------------------------------------------------------
# Import workers
# ---------------------------------------------------------------------------

def import_one(
    client: httpx.Client, kiwix_base: str, zim_name: str, title: str, resume: bool
) -> None:
    """Fetch and upsert a single article by title. Raises on HTTP error."""
    resp = client.get(article_url(kiwix_base, zim_name, title))

    # Kiwix signals redirects via HTTP 301/302. Because the client does NOT follow
    # redirects, we detect them here via the Location header and store a redirect row.
    # NOTE: raise_for_status() in httpx 0.28+ raises for 3xx too, so check
    # is_redirect BEFORE calling it.
    if resp.is_redirect:
        location = resp.headers.get("location", "")
        # Location is like /content/<zim_name>/<Canonical_Title>
        # Extract the last path segment as the target title.
        raw = location.rstrip("/").rsplit("/", 1)[-1].split("#")[0] if location else ""
        redirect_to = unquote(raw).replace("_", " ").strip() if raw else None
        if redirect_to and redirect_to != title:
            upsert_article(title=title, body=None, redirect_to=redirect_to)
            import_state["redirects_stored"] += 1
            import_state["last_redirect"] = f"{title!r} -> {redirect_to!r}"
        return

    resp.raise_for_status()
    parsed = parse_kiwix_article(resp.text, title)
    if parsed.get("redirect"):
        # Fallback: HTML meta-refresh redirect (some ZIM formats)
        redirect_to = parsed.get("redirect_to")
        if redirect_to:
            upsert_article(title=title, body=None, redirect_to=redirect_to)
            import_state["redirects_stored"] += 1
            import_state["last_redirect"] = f"{title!r} -> {redirect_to!r}"
        return
    if not (parsed["body"] or parsed["summary"]):
        return  # empty stub — skip
    upsert_article(
        title=title,
        body=parsed["body"],
        summary=parsed["summary"],
        facts=parsed["facts"],
        link_titles=parsed["link_titles"],
    )


def run_kiwix_import(
    zim_name: str,
    titles: Optional[list[str]],
    prefix: str,
    limit: Optional[int],
    resume: bool,
) -> None:
    kiwix_base = cfg["kiwix_url"].rstrip("/")

    with httpx.Client(timeout=30.0, follow_redirects=False) as client:
        if titles:
            work = titles[:limit] if limit else titles
        else:
            fetch_limit = limit or 50_000
            try:
                work = suggest_titles(client, kiwix_base, zim_name, prefix, fetch_limit)
            except Exception as exc:
                import_state.update({"running": False, "last_error": str(exc)})
                return

        import_state["total"] = len(work)

        for title in work:
            if not import_state["running"]:
                break
            try:
                import_one(client, kiwix_base, zim_name, title, resume)
                import_state["done"] += 1
            except Exception as exc:
                import_state["errors"] += 1
                import_state["last_error"] = f"{title}: {exc}"

    import_state["running"] = False
    resolve_links()


def run_kiwix_backfill(zim_name: str, limit: int) -> None:
    """Fetch every unresolved link target from Kiwix and store it (as a full article or redirect).

    This repairs the gap left by the old importer that silently dropped redirect pages:
    the links table has millions of to_title values that point at titles never stored
    in articles (because they were redirect pages at the time of import).  Fetching
    each one now will either store it as a redirect row or as a full article.
    """
    kiwix_base = cfg["kiwix_url"].rstrip("/")
    titles = get_unresolved_link_titles(limit=limit)
    import_state["total"] = len(titles)

    with httpx.Client(timeout=30.0, follow_redirects=False) as client:
        for title in titles:
            if not import_state["running"]:
                break
            try:
                import_one(client, kiwix_base, zim_name, title, resume=False)
                import_state["done"] += 1
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    import_state["done"] += 1  # title not in this ZIM — skip quietly
                else:
                    import_state["errors"] += 1
                    import_state["last_error"] = f"{title}: HTTP {exc.response.status_code}"
            except Exception as exc:
                import_state["errors"] += 1
                import_state["last_error"] = f"{title}: {exc}"

    import_state["running"] = False
    resolve_links()


def run_kiwix_crawl(seed_url: str, max_depth: int, limit: int, resume: bool) -> None:
    """BFS crawl starting from seed_url, following wikilinks up to max_depth hops."""
    try:
        kiwix_base, zim_name, start_title = parse_seed_url(seed_url)
    except ValueError as exc:
        import_state.update({"running": False, "last_error": str(exc)})
        return

    queue: deque[tuple[str, int]] = deque([(start_title, 0)])
    visited: set[str] = {start_title}
    import_state["total"] = 1
    import_state["limit"] = limit

    with httpx.Client(timeout=30.0, follow_redirects=False) as client:
        while queue and import_state["running"]:
            if import_state["done"] >= limit:
                break

            title, depth = queue.popleft()

            # Resume: article already in DB — skip HTTP fetch but still expand its links
            if resume:
                existing = get_article_by_title(title, full=False)
                if existing is not None:
                    db_links = get_links(title) if depth < max_depth else []
                    if db_links or depth >= max_depth:
                        for lnk in db_links:
                            lt = (lnk.get("to_title") or "").strip()
                            if lt and lt not in visited:
                                visited.add(lt)
                                queue.append((lt, depth + 1))
                        continue
                    # Article exists but has no links and we need depth expansion —
                    # fall through to re-fetch so links get extracted and saved

            try:
                resp = client.get(article_url(kiwix_base, zim_name, title))

                if resp.is_redirect:
                    location = resp.headers.get("location", "")
                    # Location: /content/<zim_name>/<Canonical_Title>
                    raw = location.rstrip("/").rsplit("/", 1)[-1].split("#")[0] if location else ""
                    redirect_to = unquote(raw).replace("_", " ").strip() if raw else None
                    if redirect_to and redirect_to != title:
                        upsert_article(title=title, body=None, redirect_to=redirect_to)
                        import_state["redirects_stored"] += 1
                        import_state["last_redirect"] = f"{title!r} -> {redirect_to!r}"
                    import_state["done"] += 1
                    continue

                resp.raise_for_status()

                parsed = parse_kiwix_article(resp.text, title)
                if parsed.get("redirect"):
                    redirect_to = parsed.get("redirect_to")
                    if redirect_to:
                        upsert_article(title=title, body=None, redirect_to=redirect_to)
                        import_state["redirects_stored"] += 1
                        import_state["last_redirect"] = f"{title!r} -> {redirect_to!r}"
                    import_state["done"] += 1
                    continue
                if not (parsed["body"] or parsed["summary"]):
                    import_state["done"] += 1
                    continue  # empty stub — skip without storing
                upsert_article(
                    title=title,
                    body=parsed["body"],
                    summary=parsed["summary"],
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

    import_state["running"] = False
    resolve_links()
