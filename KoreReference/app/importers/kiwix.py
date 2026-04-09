from collections import deque
from typing import Optional
from urllib.parse import unquote, urlparse

import httpx
from bs4 import BeautifulSoup

from app.config import cfg
from app.database import get_article_by_title, get_links, resolve_links, upsert_article
from app.importers.shared import extract_article_html, extract_facts, remove_noise
from app.importers.state import import_state


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

def parse_seed_url(seed_url: str) -> tuple[str, str, str]:
    """Parse a Kiwix seed into (kiwix_base, zim_name, start_title).

    Accepts two formats:
      http://host/viewer#zim_name/Article_Title   (Kiwix viewer fragment URL)
      http://host/zim_name/A/Article_Title         (direct content URL)
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
    if len(path_parts) >= 3 and path_parts[1].upper() == "A":
        zim_name = path_parts[0]
        raw_title = "/".join(path_parts[2:])
        title = unquote(raw_title).replace("_", " ").split("#")[0].strip()
        return base, zim_name, title
    raise ValueError(
        f"Unrecognised Kiwix URL: {seed_url!r}. "
        "Expected http://host/viewer#zim/Title or http://host/zim/A/Title"
    )


def article_url(kiwix_base: str, zim_name: str, title: str) -> str:
    """Kiwix serves articles at /<zim_name>/A/<Title_With_Underscores>."""
    return f"{kiwix_base}/{zim_name}/A/{title.replace(' ', '_')}"


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
    remove_noise(soup)

    # Categories come from #mw-normal-catlinks (absent in 2025+ ZIM files)
    categories: list[str] = []
    cat_div = soup.find(id="mw-normal-catlinks")
    if cat_div:
        categories = [a.get_text(strip=True) for a in cat_div.find_all("a")[1:]]
        cat_div.decompose()

    # Collect internal link titles (first pass — before rewriting hrefs)
    link_titles: list[str] = []
    seen_links: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = unquote(a["href"]).split("#")[0].strip()
        if not href or "://" in href or href.startswith("mailto:") or href.startswith("/"):
            continue
        raw = _resolve_href(href)
        if raw is None:
            continue
        linked_title = raw.replace("_", " ").strip()
        if linked_title and linked_title not in seen_links and linked_title != title:
            link_titles.append(linked_title)
            seen_links.add(linked_title)

    # Rewrite <a> tags to [[wikilink]] markup (second pass)
    for a in soup.find_all("a", href=True):
        href = unquote(a["href"]).split("#")[0].strip()
        if not href or "://" in href or href.startswith("mailto:") or href.startswith("/"):
            continue
        raw = _resolve_href(href)
        if raw is None:
            continue
        target = raw.replace("_", " ").strip()
        display = a.get_text(strip=True)
        if target and display:
            wikilink = f"[[{target}]]" if display == target else f"[[{display}|{target}]]"
            a.replace_with(wikilink)

    facts = extract_facts(soup)
    content_div = soup.find(id="mw-content-text") or soup.find("body") or soup
    body, sections, summary = extract_article_html(content_div)

    return {
        "body": body,
        "summary": summary,
        "sections": sections,
        "categories": categories,
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
    resp.raise_for_status()
    parsed = parse_kiwix_article(resp.text, title)
    upsert_article(
        title=title,
        body=parsed["body"],
        summary=parsed["summary"],
        sections=parsed["sections"],
        categories=parsed["categories"],
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

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
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

    resolve_links(limit=500_000)
    import_state["running"] = False


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

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
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
                        import_state["done"] += 1
                        for lnk in db_links:
                            lt = (lnk.get("to_title") or "").strip()
                            if lt and lt not in visited:
                                if import_state["done"] + len(queue) < limit:
                                    visited.add(lt)
                                    queue.append((lt, depth + 1))
                        import_state["total"] = max(import_state["total"], len(visited))
                        continue
                    # Article exists but has no links and we need depth expansion —
                    # fall through to re-fetch so links get extracted and saved

            try:
                resp = client.get(article_url(kiwix_base, zim_name, title))
                resp.raise_for_status()
                parsed = parse_kiwix_article(resp.text, title)
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
