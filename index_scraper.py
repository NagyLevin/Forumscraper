#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://forum.index.hu"
MAIN_FORUM_URL = "https://forum.index.hu/Topic/showTopicList"

SHOW_TOPIC_LIST_RE = re.compile(r"/Topic/showTopicList(?:\?|$)", re.IGNORECASE)
SHOW_ARTICLE_RE = re.compile(r"/Article/showArticle2?(?:\?|$)", re.IGNORECASE)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def sanitize_filename(name: str, max_len: int = 180) -> str:
    name = clean_text(name)
    if not name:
        return "ismeretlen"

    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))

    replacements = [
        ("/", "-"),
        ("\\", "-"),
        (":", " -"),
        ("*", ""),
        ("?", ""),
        ('"', ""),
        ("<", "("),
        (">", ")"),
        ("|", "-"),
    ]
    for src, dst in replacements:
        name = name.replace(src, dst)

    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"[. ]+$", "", name)

    if len(name) > max_len:
        name = name[:max_len].rstrip(" .")

    return name or "ismeretlen"


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def ensure_dirs(base_output: Path) -> Path:
    index_dir = base_output / "index"
    index_dir.mkdir(parents=True, exist_ok=True)
    return index_dir


def ensure_visited_file(folder: Path) -> Path:
    visited = folder / "visited_topics.txt"
    if not visited.exists():
        visited.write_text("", encoding="utf-8")
    return visited


def load_visited(visited_file: Path) -> Set[str]:
    if not visited_file.exists():
        return set()
    return {
        line.strip()
        for line in visited_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def append_visited(visited_file: Path, topic_url: str) -> None:
    with visited_file.open("a", encoding="utf-8") as f:
        f.write(topic_url.strip() + "\n")


def split_name_like_person(name: str) -> Dict[str, str]:
    name = clean_text(name)
    if not name:
        return {"name": ""}

    parts = name.split()
    if len(parts) >= 2:
        return {"family": parts[0], "given": " ".join(parts[1:])}
    return {"name": name}


def parse_int_from_text(text: str) -> Optional[int]:
    text = clean_text(text)
    if not text:
        return None
    m = re.search(r"-?\d+", text.replace(".", "").replace(" ", ""))
    if m:
        try:
            return int(m.group(0))
        except ValueError:
            return None
    return None


def extract_query_param(url: str, key: str) -> Optional[str]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    vals = query.get(key)
    if vals:
        return vals[0]
    return None


def save_topic_json(topic_file: Path, payload: Dict) -> None:
    topic_file.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


class BrowserFetcher:
    def __init__(self, headless: bool = True, slow_mo: int = 0):
        self.headless = headless
        self.slow_mo = slow_mo
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            slow_mo=self.slow_mo,
        )
        self.context = self.browser.new_context(
            locale="hu-HU",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
        )
        self.page = self.context.new_page()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.page:
                self.page.close()
        except Exception:
            pass
        try:
            if self.context:
                self.context.close()
        except Exception:
            pass
        try:
            if self.browser:
                self.browser.close()
        except Exception:
            pass
        try:
            if self.playwright:
                self.playwright.stop()
        except Exception:
            pass

    def accept_cookies_if_present(self) -> None:
        candidates = [
            "text=ELFOGADOM",
            "text=Elfogadom",
            "button:has-text('ELFOGADOM')",
            "button:has-text('Elfogadom')",
            "input[type='submit'][value='ELFOGADOM']",
            "input[type='submit'][value='Elfogadom']",
        ]

        for selector in candidates:
            try:
                locator = self.page.locator(selector).first
                if locator.is_visible(timeout=1500):
                    print(f"[DEBUG] Sütigomb megtalálva: {selector}")
                    locator.click(timeout=3000)
                    self.page.wait_for_timeout(1500)
                    return
            except Exception:
                pass

        print("[DEBUG] Nem találtam külön sütis elfogadó gombot, vagy már el volt fogadva.")

    def fetch(self, url: str, wait_ms: int = 1500) -> Tuple[str, str]:
        print(f"[DEBUG] LETÖLTVE: {url}")
        self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
        self.page.wait_for_timeout(wait_ms)

        self.accept_cookies_if_present()

        try:
            self.page.wait_for_load_state("networkidle", timeout=5000)
        except PlaywrightTimeoutError:
            pass

        final_url = self.page.url
        html = self.page.content()

        print(f"[DEBUG] Végső URL: {final_url}")
        print(f"[DEBUG] HTML első 500 karakter:\n{html[:500]}\n")

        return final_url, html


def parse_main_categories(html: str, page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[Dict] = []

    maintd = soup.select_one("td#maintd")
    if not maintd:
        print("[DEBUG] Nem található td#maintd a főoldalon.")
        return results

    containers = maintd.select("div.fcontainer, div.container")
    print(f"[DEBUG] Főoldali releváns container elemek száma: {len(containers)}")

    for idx, container in enumerate(containers, start=1):
        title_p = container.select_one("p.ftitle, p.title")
        links_p = container.select_one("p.flinks, p.links")
        body_p = container.select_one("p.fbody, p.body")

        if not title_p or not links_p:
            continue

        title_a = title_p.select_one("a[href]")
        if not title_a:
            continue

        category_title = clean_text(title_a.get_text(" ", strip=True))
        category_url = urljoin(page_url, title_a.get("href", ""))

        sublinks = []
        seen = set()

        for a in links_p.select("a[href]"):
            sub_title = clean_text(a.get_text(" ", strip=True))
            href = a.get("href", "").strip()

            if not sub_title or not href:
                continue

            full_url = urljoin(page_url, href)

            if not SHOW_TOPIC_LIST_RE.search(full_url):
                continue

            if full_url in seen:
                continue
            seen.add(full_url)

            sublinks.append(
                {
                    "title": sub_title,
                    "url": full_url,
                }
            )

        body_text = clean_text(body_p.get_text(" ", strip=True)) if body_p else ""

        print(
            f"[DEBUG] Fórumcsoport #{idx}: {category_title} | kis linkek: {len(sublinks)}"
        )

        if sublinks:
            results.append(
                {
                    "category_title": category_title,
                    "category_url": category_url,
                    "category_description": body_text,
                    "subforums": sublinks,
                }
            )

    return results


def parse_subforum_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        "td#maintd h1",
        "div#mainspacer h1",
        "h1",
        "title",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        text = clean_text(node.get_text(" ", strip=True))
        text = re.sub(r"\s*-\s*Index Fórum.*$", "", text, flags=re.I)
        if text:
            return text
    return "ismeretlen_alforum"


def parse_topic_rows_from_subforum_page(html: str, page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    topics: List[Dict] = []
    seen = set()

    rows = soup.select("td#maintd table tr")
    print(f"[DEBUG] Topiclista sorok száma: {len(rows)}")

    for row in rows:
        anchors = row.select("a[href]")
        if not anchors:
            continue

        title_anchor = None
        for a in anchors:
            href = a.get("href", "")
            full_url = urljoin(page_url, href)
            if SHOW_ARTICLE_RE.search(full_url):
                title_anchor = a
                break

        if not title_anchor:
            continue

        topic_title = clean_text(title_anchor.get_text(" ", strip=True))
        if not topic_title:
            continue

        topic_url = urljoin(page_url, title_anchor.get("href", ""))
        if topic_url in seen:
            continue
        seen.add(topic_url)

        raw_cells = row.find_all("td")
        creator = clean_text(raw_cells[1].get_text(" ", strip=True)) if len(raw_cells) > 1 else None
        last_user = clean_text(raw_cells[2].get_text(" ", strip=True)) if len(raw_cells) > 2 else None
        count_text = clean_text(raw_cells[3].get_text(" ", strip=True)) if len(raw_cells) > 3 else ""
        comment_count = parse_int_from_text(count_text)

        topics.append(
            {
                "title": topic_title,
                "url": topic_url,
                "creator": creator,
                "last_user": last_user,
                "comment_count": comment_count,
            }
        )

    return topics


def get_subforum_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    candidates = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        full = urljoin(current_url, href)
        if not SHOW_TOPIC_LIST_RE.search(full):
            continue

        txt = clean_text(a.get_text(" ", strip=True))
        img = a.select_one("img[alt]")
        alt = clean_text(img.get("alt", "")) if img else ""

        if alt in {"10>", ">", ">>"} or txt in {">", ">>"}:
            candidates.append(full)

    if candidates:
        return candidates[0]

    current_nt_start = extract_query_param(current_url, "nt_start")
    current_t = extract_query_param(current_url, "t")
    if current_t is not None:
        current_start_int = int(current_nt_start) if current_nt_start and current_nt_start.isdigit() else 0

        possible = []
        for a in soup.select("a[href]"):
            full = urljoin(current_url, a.get("href", ""))
            if not SHOW_TOPIC_LIST_RE.search(full):
                continue
            t_val = extract_query_param(full, "t")
            nt_start = extract_query_param(full, "nt_start")
            if t_val == current_t and nt_start and nt_start.isdigit():
                nt_start_int = int(nt_start)
                if nt_start_int > current_start_int:
                    possible.append((nt_start_int, full))

        if possible:
            possible.sort(key=lambda x: x[0])
            return possible[0][1]

    return None


def extract_topic_title(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        "td#maintd h1",
        "div#mainspacer h1",
        "h1",
        "title",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        text = clean_text(node.get_text(" ", strip=True))
        text = re.sub(r"\s*-\s*Index Fórum.*$", "", text, flags=re.I)
        if text:
            return text
    return fallback


def extract_topic_meta(html: str, topic_url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text("\n", strip=True))

    opener = None
    opened_date = None
    post_count = None
    commenter_count = None

    m = re.search(
        r"Nyitotta:\s*(.+?),\s*([0-9]{4}\.[0-9]{2}\.[0-9]{2}\s+[0-9]{2}:[0-9]{2})\s*\|\s*Hozzászólások:\s*([0-9]+)\s*\|\s*Hozzászólók:\s*([0-9]+)",
        page_text,
        flags=re.I,
    )
    if m:
        opener = clean_text(m.group(1))
        opened_date = clean_text(m.group(2))
        post_count = parse_int_from_text(m.group(3))
        commenter_count = parse_int_from_text(m.group(4))

    return {
        "opener": opener,
        "opened_date": opened_date,
        "post_count": post_count,
        "commenter_count": commenter_count,
        "url": topic_url,
    }


def find_comment_tables(soup: BeautifulSoup) -> List[Tag]:
    tables = soup.select("table.art")
    if tables:
        return tables
    return []


def extract_comment_from_table(table: Tag, topic_page_url: str) -> Optional[Dict]:
    full_text = clean_text(table.get_text("\n", strip=True))
    if not full_text:
        return None

    header_row = table.select_one("tr.art_h")
    header_text = clean_text(header_row.get_text(" ", strip=True)) if header_row else ""

    author = None
    date_text = None
    likes = None
    dislikes = None
    score = None
    comment_id = None
    comment_url = topic_page_url

    author_candidates = []
    if header_row:
        for a in header_row.select("a[href], b, span, div"):
            txt = clean_text(a.get_text(" ", strip=True))
            if txt:
                author_candidates.append(txt)

    for cand in author_candidates:
        if len(cand) > 1 and cand.lower() not in {"cc", "v"} and not re.fullmatch(r"-?\d+", cand):
            author = cand
            break

    if not author:
        m_author = re.match(
            r"([^\d][^#|]{1,80}?)\s+(?:cc\s+)?(?:\d+\s+)?(?:órája|perce|napja|hete|hónapja|éve)",
            header_text,
            flags=re.I,
        )
        if m_author:
            author = clean_text(m_author.group(1))

    date_patterns = [
        r"\b\d+\s+perce\b",
        r"\b\d+\s+órája\b",
        r"\b\d+\s+napja\b",
        r"\b\d+\s+hete\b",
        r"\b\d+\s+hónapja\b",
        r"\b\d+\s+éve\b",
        r"\b[0-9]{4}\.[0-9]{2}\.[0-9]{2}\.? ?[0-9]{0,2}:?[0-9]{0,2}\b",
    ]
    for pat in date_patterns:
        m = re.search(pat, header_text, flags=re.I)
        if m:
            date_text = clean_text(m.group(0))
            break

    nums = re.findall(r"(?<![#\d])-?\d+(?!\d)", header_text)
    small_nums = []
    for n in nums:
        try:
            iv = int(n)
            if -5000 <= iv <= 5000:
                small_nums.append(iv)
        except ValueError:
            pass

    if small_nums:
        negatives = [x for x in small_nums if x < 0]
        positives = [x for x in small_nums if x > 0]
        dislikes = abs(negatives[0]) if negatives else 0
        likes = positives[-1] if positives else 0
        score = likes - dislikes

    id_candidates = re.findall(r"\b\d{4,}\b", header_text)
    if id_candidates:
        comment_id = id_candidates[-1]

    body_candidates: List[str] = []
    for node in table.select("div.art_t, div.art_body, td[colspan='3'] div, p"):
        txt = clean_text(node.get_text("\n", strip=True))
        if not txt or txt == header_text or txt.lower() == "előzmény":
            continue
        body_candidates.append(txt)

    uniq = []
    seen = set()
    for item in body_candidates:
        if item not in seen:
            uniq.append(item)
            seen.add(item)

    body = "\n\n".join(uniq).strip()

    if not body:
        return None

    if comment_id:
        comment_url = topic_page_url.split("#")[0] + f"#msg{comment_id}"

    return {
        "comment_id": comment_id,
        "author": author or "ismeretlen",
        "date": date_text,
        "likes": likes,
        "dislikes": dislikes,
        "score": score,
        "url": comment_url,
        "data": body,
    }


def parse_comments_from_topic_page(html: str, topic_page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    tables = find_comment_tables(soup)

    print(f"[DEBUG] Talált komment-table elemek száma: {len(tables)}")

    comments = []
    for idx, table in enumerate(tables, start=1):
        parsed = extract_comment_from_table(table, topic_page_url)
        if not parsed:
            continue

        preview = parsed["data"][:120].replace("\n", " | ")
        print(
            f"[DEBUG] Komment #{idx} | id={parsed.get('comment_id') or '-'} "
            f"| szerző={parsed['author']} | like={parsed.get('likes')} "
            f"| dislike={parsed.get('dislikes')} | preview={preview}"
        )
        comments.append(parsed)

    return comments


def get_topic_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    candidates = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        full = urljoin(current_url, href)
        if not SHOW_ARTICLE_RE.search(full):
            continue

        txt = clean_text(a.get_text(" ", strip=True))
        img = a.select_one("img[alt]")
        alt = clean_text(img.get("alt", "")) if img else ""

        if alt in {"30>", ">", ">>"} or txt in {">", ">>"}:
            candidates.append(full)

    if candidates:
        return candidates[0]

    current_start = extract_query_param(current_url, "na_start")
    current_t = extract_query_param(current_url, "t")
    current_a = extract_query_param(current_url, "a")
    current_start_int = int(current_start) if current_start and current_start.isdigit() else 0

    possible = []
    for a in soup.select("a[href]"):
        full = urljoin(current_url, a.get("href", ""))
        if not SHOW_ARTICLE_RE.search(full):
            continue

        t_val = extract_query_param(full, "t")
        a_val = extract_query_param(full, "a")
        na_start = extract_query_param(full, "na_start")

        if t_val == current_t and a_val == current_a and na_start and na_start.isdigit():
            na_start_int = int(na_start)
            if na_start_int > current_start_int:
                possible.append((na_start_int, full))

    if possible:
        possible.sort(key=lambda x: x[0])
        return possible[0][1]

    return None


def topic_file_path(subforum_dir: Path, topic_title: str) -> Path:
    return subforum_dir / f"{sanitize_filename(topic_title)}.txt"


def scrape_topic(
    fetcher: BrowserFetcher,
    topic_title: str,
    topic_url: str,
    delay: float,
) -> Dict:
    print(f"[INFO] Topic megnyitása: {topic_title}")
    current_url, html = fetcher.fetch(topic_url, wait_ms=int(delay * 1000))

    resolved_title = extract_topic_title(html, topic_title)
    topic_meta = extract_topic_meta(html, current_url)

    all_comments: List[Dict] = []
    seen_comment_keys: Set[str] = set()
    page_no = 1

    while True:
        print(f"[INFO] Kommentoldal #{page_no}: {current_url}")
        page_comments = parse_comments_from_topic_page(html, current_url)

        for item in page_comments:
            key = f"{item.get('comment_id') or ''}::{item.get('author') or ''}::{item.get('data')[:80]}"
            if key in seen_comment_keys:
                continue
            seen_comment_keys.add(key)
            all_comments.append(item)

        next_url = get_topic_next_page_url(html, current_url)
        if not next_url or next_url == current_url:
            print("[INFO] Nincs több kommentoldal ennél a topicnál.")
            break

        print(f"[INFO] Következő kommentoldal: {next_url}")
        current_url, html = fetcher.fetch(next_url, wait_ms=int(delay * 1000))
        page_no += 1

    opener = topic_meta.get("opener") or ""

    payload = {
        "title": resolved_title,
        "authors": [split_name_like_person(opener)] if opener else [],
        "data": {
            "content": resolved_title,
            "likes": None,
            "dislikes": None,
            "score": None,
            "date": topic_meta.get("opened_date"),
            "url": topic_meta.get("url"),
            "language": "hu",
            "tags": [],
            "rights": "Index Fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "opener_username": opener,
                "post_count": topic_meta.get("post_count"),
                "commenter_count": topic_meta.get("commenter_count"),
            },
            "origin": "index_forum",
        },
        "comments": [
            {
                "authors": [split_name_like_person(c["author"])] if c.get("author") else [],
                "data": c["data"],
                "likes": c.get("likes"),
                "dislikes": c.get("dislikes"),
                "score": c.get("score"),
                "date": c.get("date"),
                "url": c.get("url"),
                "language": "hu",
                "tags": [],
                "extra": {
                    "comment_id": c.get("comment_id"),
                },
            }
            for c in all_comments
        ],
        "origin": "index_forum",
    }

    return payload


def scrape_subforum(
    fetcher: BrowserFetcher,
    category_title: str,
    subforum_title: str,
    subforum_url: str,
    base_index_dir: Path,
    delay: float,
) -> None:
    category_dir = base_index_dir / sanitize_filename(category_title)
    subforum_dir = category_dir / sanitize_filename(subforum_title)
    subforum_dir.mkdir(parents=True, exist_ok=True)

    visited_file = ensure_visited_file(subforum_dir)
    visited_topics = load_visited(visited_file)

    print(f"\n[INFO] Alforum indul: {category_title} -> {subforum_title}")
    print(f"[INFO] Alforum URL: {subforum_url}")

    current_url = subforum_url
    page_no = 1

    while True:
        print(f"\n[INFO] Topiclista oldal #{page_no}: {current_url}")
        final_url, html = fetcher.fetch(current_url, wait_ms=int(delay * 1000))

        resolved_subforum_title = parse_subforum_title(html)
        print(f"[DEBUG] Felismert alforum cím: {resolved_subforum_title}")

        topics = parse_topic_rows_from_subforum_page(html, final_url)
        print(f"[INFO] Talált topicok ezen az oldalon: {len(topics)}")

        for idx, topic in enumerate(topics, start=1):
            topic_title = topic["title"]
            topic_url = topic["url"]

            print(f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title}")
            if topic_url in visited_topics:
                print("[INFO] Már visitedben van, kihagyva.")
                continue

            try:
                payload = scrape_topic(
                    fetcher=fetcher,
                    topic_title=topic_title,
                    topic_url=topic_url,
                    delay=delay,
                )

                resolved_title = payload.get("title") or topic_title
                topic_path = topic_file_path(subforum_dir, resolved_title)

                save_topic_json(topic_path, payload)
                append_visited(visited_file, topic_url)
                visited_topics.add(topic_url)

                print(f"[INFO] Topic mentve: {topic_path}")
                print(f"[INFO] Topic visitedbe írva: {topic_url}")

            except Exception as e:
                print(f"[WARN] Hiba topic feldolgozás közben: {topic_url} | {e}")

        next_url = get_subforum_next_page_url(html, final_url)
        if not next_url or next_url == final_url:
            print(f"[INFO] Nincs több topiclista oldal ennél az alforumnál: {subforum_title}")
            break

        print(f"[INFO] Következő topiclista oldalra lépek ({page_no + 1}. oldal): {next_url}")
        current_url = next_url
        page_no += 1


def scrape_main(
    fetcher: BrowserFetcher,
    output_dir: str,
    delay: float,
    only_category: Optional[str],
    only_subforum: Optional[str],
) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    index_dir = ensure_dirs(base_output)

    print(f"[INFO] Főoldal megnyitása: {MAIN_FORUM_URL}")
    final_url, html = fetcher.fetch(MAIN_FORUM_URL, wait_ms=int(delay * 1000))

    debug_path = base_output / "debug_index_main.html"
    debug_path.write_text(html, encoding="utf-8")
    print(f"[DEBUG] A főoldal HTML-je elmentve: {debug_path}")

    categories = parse_main_categories(html, final_url)
    print(f"[INFO] Feldolgozandó fórumcsoportok száma: {len(categories)}")

    for cat_idx, cat in enumerate(categories, start=1):
        category_title = cat["category_title"]

        if only_category and only_category.lower() not in category_title.lower():
            continue

        print(f"\n[INFO] Fórumcsoport ({cat_idx}/{len(categories)}): {category_title}")

        for sub_idx, sub in enumerate(cat["subforums"], start=1):
            subforum_title = sub["title"]
            subforum_url = sub["url"]

            if only_subforum and only_subforum.lower() not in subforum_title.lower():
                continue

            print(
                f"[INFO] Kis link ({sub_idx}/{len(cat['subforums'])}): "
                f"{subforum_title} | {subforum_url}"
            )

            try:
                scrape_subforum(
                    fetcher=fetcher,
                    category_title=category_title,
                    subforum_title=subforum_title,
                    subforum_url=subforum_url,
                    base_index_dir=index_dir,
                    delay=delay,
                )
            except Exception as e:
                print(f"[WARN] Hiba alforum feldolgozás közben: {subforum_url} | {e}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Index Fórum scraper Playwright + BeautifulSoup alapon."
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre az index mappa.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Várakozás oldalak között másodpercben.",
    )
    parser.add_argument(
        "--only-category",
        default=None,
        help="Csak azokat a nagy fórumcsoportokat dolgozza fel, amelyek címében ez szerepel.",
    )
    parser.add_argument(
        "--only-subforum",
        default=None,
        help="Csak azokat a kis alforumokat dolgozza fel, amelyek címében ez szerepel.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Látható böngészőablakkal fut.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        with BrowserFetcher(headless=not args.headed, slow_mo=50 if args.headed else 0) as fetcher:
            scrape_main(
                fetcher=fetcher,
                output_dir=args.output,
                delay=args.delay,
                only_category=args.only_category,
                only_subforum=args.only_subforum,
            )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva felhasználó által.")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] Végzetes hiba: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()