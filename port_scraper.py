#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://port.hu"
MAIN_FORUM_URL = "https://port.hu/forum"

TOPIC_LIST_RE = re.compile(r"^/forum(?:\?|$)", re.IGNORECASE)
TOPIC_PAGE_RE = re.compile(r"^/forum/[^/?#]+/\d+(?:\?.*)?$", re.IGNORECASE)


# -----------------------------
# Általános segédfüggvények
# -----------------------------

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


def strip_fragment(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))


def extract_query_param(url: str, key: str) -> Optional[str]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    vals = query.get(key)
    return vals[0] if vals else None


def set_query_param(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query[key] = [value]
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query, doseq=True),
            "",
        )
    )


def parse_int_from_text(text: str) -> Optional[int]:
    text = clean_text(text)
    if not text:
        return None
    normalized = text.replace(".", "").replace(" ", "")
    m = re.search(r"-?\d+", normalized)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def split_name_like_person(name: str) -> Dict[str, str]:
    name = clean_text(name)
    if not name:
        return {"name": ""}

    parts = name.split()
    if len(parts) >= 2:
        return {"family": parts[0], "given": " ".join(parts[1:])}
    return {"name": name}


def get_topic_base_url(url: str) -> str:
    parsed = urlparse(strip_fragment(url))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", ""))


def get_topic_page_number(url: str) -> int:
    page_val = extract_query_param(url, "page")
    if page_val and page_val.isdigit():
        return int(page_val)
    return 1


# -----------------------------
# Fájl / output kezelés
# -----------------------------

def ensure_dirs(base_output: Path) -> Tuple[Path, Path, Path]:
    port_dir = base_output / "port"
    topics_dir = port_dir / "topics"
    port_dir.mkdir(parents=True, exist_ok=True)
    topics_dir.mkdir(parents=True, exist_ok=True)

    visited_file = port_dir / "visited.txt"
    if not visited_file.exists():
        visited_file.write_text("", encoding="utf-8")

    return port_dir, topics_dir, visited_file


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


def save_topic_json(topic_file: Path, payload: Dict) -> None:
    topic_file.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_topic_json(topic_file: Path) -> Optional[Dict]:
    if not topic_file.exists():
        return None
    try:
        return json.loads(topic_file.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[WARN] Hibás JSON, újraépítés lesz: {topic_file} | {e}")
        return None


def normalize_topic_url_for_visited(url: str) -> str:
    return get_topic_base_url(url)


def topic_file_path(topics_dir: Path, topic_title: str) -> Path:
    return topics_dir / f"{sanitize_filename(topic_title)}.json"


# -----------------------------
# Playwright wrapper
# -----------------------------

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
            viewport={"width": 1440, "height": 2200},
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
            "button:has-text('Elfogadom')",
            "button:has-text('ELFOGADOM')",
            "button:has-text('Rendben')",
            "button:has-text('OK')",
            "text=Elfogadom",
            "text=ELFOGADOM",
        ]
        for selector in candidates:
            try:
                locator = self.page.locator(selector).first
                if locator.is_visible(timeout=1200):
                    locator.click(timeout=2500)
                    self.page.wait_for_timeout(1200)
                    return
            except Exception:
                pass

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
        print(f"[DEBUG] HTML első 400 karakter:\n{html[:400]}\n")
        return final_url, html


# -----------------------------
# Főoldali topiclista parsing
# -----------------------------

def parse_topic_rows_from_main_page(html: str, page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    topics: List[Dict] = []
    seen = set()

    table = soup.select_one("table.table.table-condensed")
    if not table:
        print("[DEBUG] Nem található a topiclista táblázat.")
        return topics

    rows = table.select("tbody tr[data-key], tbody tr")
    print(f"[DEBUG] Főoldali topic sorok száma: {len(rows)}")

    for row in rows:
        title_a = None
        for a in row.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if TOPIC_PAGE_RE.search(href):
                title_a = a
                break

        if not title_a:
            continue

        topic_title = clean_text(title_a.get_text(" ", strip=True))
        if not topic_title:
            continue

        topic_url = urljoin(page_url, title_a.get("href", ""))
        topic_url_norm = normalize_topic_url_for_visited(topic_url)
        if topic_url_norm in seen:
            continue
        seen.add(topic_url_norm)

        cells = row.find_all("td")
        comment_count = None
        view_count = None
        last_message = None
        last_user = None

        if len(cells) >= 2:
            comment_count = parse_int_from_text(cells[1].get_text(" ", strip=True))
        if len(cells) >= 3:
            view_count = parse_int_from_text(cells[2].get_text(" ", strip=True))
        if len(cells) >= 4:
            cell_text = clean_text(cells[3].get_text(" ", strip=True))
            m = re.match(r"^(.*?\d{1,2}:\d{2})\s+(.+)$", cell_text)
            if m:
                last_message = clean_text(m.group(1))
                last_user = clean_text(m.group(2))
            else:
                last_message = cell_text

        topics.append(
            {
                "title": topic_title,
                "url": topic_url_norm,
                "comment_count": comment_count,
                "view_count": view_count,
                "last_message": last_message,
                "last_user": last_user,
            }
        )

    return topics


def get_main_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    paginations = soup.select("ul.pagination")
    for ul in paginations:
        for li in ul.select("li.next a[href], li.last a[href]"):
            href = (li.get("href") or "").strip()
            if href:
                return urljoin(current_url, href)

        for a in ul.select("a[href]"):
            txt = clean_text(a.get_text(" ", strip=True))
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if txt in {">", "›", "»"}:
                return urljoin(current_url, href)

    current_page = extract_query_param(current_url, "page")
    current_page_no = int(current_page) if current_page and current_page.isdigit() else 1
    next_page_no = current_page_no + 1

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        full = urljoin(current_url, href)
        if not full.startswith(MAIN_FORUM_URL):
            continue
        page_val = extract_query_param(full, "page")
        if page_val and page_val.isdigit() and int(page_val) == next_page_no:
            return full

    return set_query_param(MAIN_FORUM_URL, "page", str(next_page_no))


# -----------------------------
# Topicoldal parsing
# -----------------------------

def extract_topic_title(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "div.main-box h1 a",
        "div.main-box h1",
        "h1 a",
        "h1",
        "title",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            text = re.sub(r"^\s*Téma:\s*", "", text, flags=re.I)
            if text:
                return text

    return fallback


def extract_topic_meta(html: str, topic_url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text("\n", strip=True))

    page_count = get_topic_page_number(topic_url)

    total_comments = None
    m = re.search(r"\((\d+)\s*/\s*(\d+)\)", page_text)
    if m:
        total_comments = parse_int_from_text(m.group(2))

    return {
        "url": get_topic_base_url(topic_url),
        "detected_total_comments": total_comments,
        "fetched_page": page_count,
    }


def find_comment_containers(soup: BeautifulSoup) -> List[Tag]:
    containers = soup.select("div.comment-container")
    if containers:
        return containers
    return soup.select("div.comment-container, div[class*='comment-container']")


def parse_comment_header(header_text: str) -> Dict:
    header_text = clean_text(header_text)

    rating = None
    parent_author = None
    date_text = None
    author = None

    m_parent = re.search(r"\bElőzmény\s+(.+)$", header_text, flags=re.I)
    if m_parent:
        parent_author = clean_text(m_parent.group(1))
        header_text = clean_text(header_text[:m_parent.start()])

    m_rating = re.search(r"\b(\d{1,2}/10)\b", header_text)
    if m_rating:
        rating = clean_text(m_rating.group(1))
        header_text = clean_text(header_text.replace(m_rating.group(1), " "))

    month_names = (
        "jan\\.|febr\\.|márc\\.|ápr\\.|máj\\.|jún\\.|júl\\.|aug\\.|szept\\.|okt\\.|nov\\.|dec\\."
    )
    date_patterns = [
        rf"\b(?:ma|tegnap)\s+\d{{1,2}}:\d{{2}}(?::\d{{2}})?\b",
        rf"\b(?:{month_names})\s+\d{{1,2}}\.\s+\d{{1,2}}:\d{{2}}(?::\d{{2}})?\b",
        rf"\b\d{{4}}\.\d{{2}}\.\d{{2}}\.\s+\d{{1,2}}:\d{{2}}(?::\d{{2}})?\b",
    ]

    for pat in date_patterns:
        m_date = re.search(pat, header_text, flags=re.I)
        if m_date:
            date_text = clean_text(m_date.group(0))
            author = clean_text(header_text[:m_date.start()])
            break

    if not author:
        parts = header_text.split()
        author = parts[0] if parts else "ismeretlen"

    return {
        "author": author or "ismeretlen",
        "date": date_text,
        "rating": rating,
        "parent_author": parent_author,
    }


def parse_comment_index(text: str) -> Tuple[Optional[int], Optional[int]]:
    text = clean_text(text)
    m = re.search(r"\((\d+)\s*/\s*(\d+)\)", text)
    if not m:
        return None, None
    return parse_int_from_text(m.group(1)), parse_int_from_text(m.group(2))


def extract_comment_from_container(container: Tag, topic_page_url: str) -> Optional[Dict]:
    anchor = container.select_one("a[name]")
    comment_id = None
    if anchor and anchor.get("name"):
        m = re.search(r"comment-(\d+)", anchor.get("name", ""))
        if m:
            comment_id = m.group(1)

    header_row = container.select_one("div.row.header")
    header_text = clean_text(header_row.get_text(" ", strip=True)) if header_row else ""

    parsed_header = parse_comment_header(header_text)

    message_node = container.select_one("div.message-text")
    body = clean_text(message_node.get_text("\n", strip=True)) if message_node else ""

    whole_text = clean_text(container.get_text("\n", strip=True))
    comment_no, total_no = parse_comment_index(whole_text)

    is_offtopic = "offtopic" in " ".join(container.get("class", []))
    if not is_offtopic and re.search(r"\bofftopic\b", whole_text, flags=re.I):
        is_offtopic = True

    comment_url = strip_fragment(topic_page_url)
    if comment_id:
        comment_url = f"{comment_url}#comment-{comment_id}"

    return {
        "comment_id": comment_id,
        "author": parsed_header["author"],
        "date": parsed_header["date"],
        "rating": parsed_header["rating"],
        "parent_author": parsed_header["parent_author"],
        "index": comment_no,
        "index_total": total_no,
        "is_offtopic": is_offtopic,
        "url": comment_url,
        "data": body,
    }


def parse_comments_from_topic_page(html: str, topic_page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    containers = find_comment_containers(soup)

    print(f"[DEBUG] Talált komment-container elemek száma: {len(containers)}")

    comments: List[Dict] = []
    for idx, container in enumerate(containers, start=1):
        parsed = extract_comment_from_container(container, topic_page_url)
        if not parsed:
            continue

        preview = (parsed["data"] or "")[:100].replace("\n", " | ")
        print(
            f"[DEBUG] Komment #{idx} | id={parsed.get('comment_id') or '-'} "
            f"| szerző={parsed.get('author')} | dátum={parsed.get('date')} "
            f"| rating={parsed.get('rating')} | preview={preview}"
        )
        comments.append(parsed)

    return comments


def build_comment_signature(comment: Dict) -> str:
    comment_id = str(comment.get("comment_id") or "")
    author = clean_text(comment.get("author") or "")
    date = clean_text(comment.get("date") or "")
    text = clean_text(comment.get("data") or "")
    text = text[:300]
    idx = str(comment.get("index") or "")
    return f"{comment_id}|{author}|{date}|{idx}|{text}"


def build_page_fingerprint(comments: List[Dict]) -> str:
    if not comments:
        return "EMPTY"

    sigs = [build_comment_signature(c) for c in comments]
    raw = "\n".join(sigs)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def topic_has_any_comment_container(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    return bool(find_comment_containers(soup))


def get_topic_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    current_page_no = get_topic_page_number(current_url)

    for ul in soup.select("ul.pagination"):
        for li in ul.select("li.next a[href], li.last a[href]"):
            href = (li.get("href") or "").strip()
            if href:
                full = urljoin(current_url, href)
                if get_topic_page_number(full) > current_page_no:
                    return full

        for a in ul.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue

            full = urljoin(current_url, href)
            full_page_no = get_topic_page_number(full)
            if full_page_no == current_page_no + 1:
                return full

            txt = clean_text(a.get_text(" ", strip=True))
            if txt in {">", "›", "»"} and full_page_no > current_page_no:
                return full

    next_page_no = current_page_no + 1
    return set_query_param(get_topic_base_url(current_url), "page", str(next_page_no))


# -----------------------------
# JSON payload építés / merge
# -----------------------------

def build_topic_payload_base(resolved_title: str, topic_meta: Dict, topic_url: str) -> Dict:
    return {
        "title": resolved_title,
        "authors": [],
        "data": {
            "content": resolved_title,
            "likes": None,
            "dislikes": None,
            "score": None,
            "rating": None,
            "date": None,
            "url": get_topic_base_url(topic_url),
            "language": "hu",
            "tags": [],
            "rights": "port.hu fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "detected_total_comments": topic_meta.get("detected_total_comments"),
                "fetched_page": topic_meta.get("fetched_page"),
            },
            "origin": "port_forum",
        },
        "comments": [],
        "origin": "port_forum",
        "extra": {
            "scrape_status": "in_progress",
            "saved_comment_pages": 0,
            "resume_source": None,
            "downloaded_comments_total": 0,
        },
    }


def merge_existing_payload(existing_payload: Dict, fresh_title: str, topic_meta: Dict, topic_url: str) -> Dict:
    existing_payload["title"] = existing_payload.get("title") or fresh_title
    existing_payload["origin"] = existing_payload.get("origin") or "port_forum"

    if "data" not in existing_payload or not isinstance(existing_payload["data"], dict):
        existing_payload["data"] = {}

    existing_payload["data"]["content"] = existing_payload["data"].get("content") or fresh_title
    existing_payload["data"]["url"] = existing_payload["data"].get("url") or get_topic_base_url(topic_url)
    existing_payload["data"]["language"] = existing_payload["data"].get("language") or "hu"
    existing_payload["data"]["rights"] = existing_payload["data"].get("rights") or "port.hu fórum tartalom"
    existing_payload["data"]["origin"] = existing_payload["data"].get("origin") or "port_forum"
    existing_payload["data"]["date_modified"] = now_iso()

    data_extra = existing_payload["data"].get("extra") or {}
    if topic_meta.get("detected_total_comments") is not None:
        data_extra["detected_total_comments"] = topic_meta.get("detected_total_comments")
    if topic_meta.get("fetched_page") is not None:
        data_extra["fetched_page"] = topic_meta.get("fetched_page")
    existing_payload["data"]["extra"] = data_extra

    if "comments" not in existing_payload or not isinstance(existing_payload["comments"], list):
        existing_payload["comments"] = []

    if "extra" not in existing_payload or not isinstance(existing_payload["extra"], dict):
        existing_payload["extra"] = {}
    existing_payload["extra"]["scrape_status"] = "in_progress"
    existing_payload["extra"]["downloaded_comments_total"] = len(existing_payload["comments"])

    return existing_payload


def get_comment_unique_key(comment_like: Dict) -> str:
    author_name = ""
    if "author" in comment_like:
        author_name = comment_like.get("author") or ""
    else:
        authors = comment_like.get("authors") or []
        if authors:
            a0 = authors[0]
            author_name = a0.get("name") or f"{a0.get('family', '')} {a0.get('given', '')}".strip()

    comment_id = None
    if "comment_id" in comment_like:
        comment_id = comment_like.get("comment_id")
    else:
        comment_id = (comment_like.get("extra") or {}).get("comment_id")

    data = comment_like.get("data", "")
    date = comment_like.get("date", "")
    return f"{comment_id or ''}::{author_name}::{date}::{(data or '')[:120]}"


def append_comments_to_payload(payload: Dict, new_comments: List[Dict]) -> int:
    existing_keys = {get_comment_unique_key(c) for c in payload.get("comments", [])}
    added = 0

    for c in new_comments:
        author_name = c.get("author") or "ismeretlen"
        item = {
            "authors": [split_name_like_person(author_name)] if author_name else [],
            "data": c.get("data", ""),
            "likes": None,
            "dislikes": None,
            "score": None,
            "rating": c.get("rating"),
            "date": c.get("date"),
            "url": c.get("url"),
            "language": "hu",
            "tags": ["offtopic"] if c.get("is_offtopic") else [],
            "extra": {
                "comment_id": c.get("comment_id"),
                "parent_author": c.get("parent_author"),
                "index": c.get("index"),
                "index_total": c.get("index_total"),
                "is_offtopic": c.get("is_offtopic"),
            },
        }

        key = get_comment_unique_key(c)
        if key in existing_keys:
            continue

        payload["comments"].append(item)
        existing_keys.add(key)
        added += 1

    payload["extra"]["downloaded_comments_total"] = len(payload["comments"])
    return added


def mark_payload_finished(payload: Dict) -> None:
    payload["data"]["date_modified"] = now_iso()
    payload["extra"]["scrape_status"] = "finished"
    payload["extra"]["downloaded_comments_total"] = len(payload.get("comments", []))


def derive_resume_url_from_payload(payload: Dict) -> Optional[str]:
    extra = payload.get("extra") or {}
    saved_pages = extra.get("saved_comment_pages")
    base_url = (payload.get("data") or {}).get("url")
    if not base_url or not saved_pages:
        return None

    try:
        next_page = int(saved_pages) + 1
    except Exception:
        return None

    return set_query_param(get_topic_base_url(base_url), "page", str(next_page))


# -----------------------------
# Topic scrape
# -----------------------------

def scrape_topic(
    fetcher: BrowserFetcher,
    topic_title: str,
    topic_url: str,
    topic_file: Path,
    delay: float,
) -> Dict:
    existing_payload = load_topic_json(topic_file)
    resume_url = None
    start_page_no = 1

    if existing_payload:
        resume_url = derive_resume_url_from_payload(existing_payload)
        if resume_url:
            start_page_no = int((existing_payload.get("extra") or {}).get("saved_comment_pages", 0)) + 1
            print(f"[INFO] Meglévő JSON, folytatás innen: {resume_url}")
        else:
            print("[INFO] Meglévő JSON van, de nincs resume URL, topic elejéről indulunk.")

    first_fetch_url = resume_url or set_query_param(get_topic_base_url(topic_url), "page", str(start_page_no))
    print(f"[INFO] Topic megnyitása: {topic_title}")
    current_url, html = fetcher.fetch(first_fetch_url, wait_ms=int(delay * 1000))

    resolved_title = extract_topic_title(html, topic_title)
    topic_meta = extract_topic_meta(html, current_url)

    if existing_payload:
        payload = merge_existing_payload(existing_payload, resolved_title, topic_meta, topic_url)
        payload["extra"]["resume_source"] = "existing_json"
    else:
        payload = build_topic_payload_base(resolved_title, topic_meta, topic_url)

    page_no = get_topic_page_number(current_url)

    seen_page_fingerprints: Set[str] = set()
    previous_page_fingerprint: Optional[str] = None

    while True:
        print(f"[INFO] Kommentoldal #{page_no}: {current_url}")
        page_comments = parse_comments_from_topic_page(html, current_url)

        if page_no > 1 and not page_comments:
            print("[INFO] Üres vagy nem értelmezhető kommentoldal, megállok ennél a topicnál.")
            break

        current_fingerprint = build_page_fingerprint(page_comments)
        print(f"[DEBUG] Oldal fingerprint: {current_fingerprint}")

        if previous_page_fingerprint is not None and current_fingerprint == previous_page_fingerprint:
            print("[INFO] A mostani kommentoldal megegyezik az előzővel, a topic véget ért.")
            break

        if current_fingerprint in seen_page_fingerprints:
            print("[INFO] Már korábban látott kommentoldal-tartalom jött vissza, a topic véget ért.")
            break

        seen_page_fingerprints.add(current_fingerprint)

        added = append_comments_to_payload(payload, page_comments)
        payload["data"]["date_modified"] = now_iso()
        payload["extra"]["saved_comment_pages"] = page_no

        save_topic_json(topic_file, payload)
        print(
            f"[INFO] JSON mentve oldalanként: {topic_file} | "
            f"új kommentek: {added} | összes komment eddig: {len(payload['comments'])}"
        )

        next_url = get_topic_next_page_url(html, current_url)
        if not next_url:
            print("[INFO] Nincs több kommentoldal ennél a topicnál.")
            break

        next_page_no = get_topic_page_number(next_url)
        if next_page_no <= page_no:
            print("[INFO] A következő oldal száma nem nagyobb a mostaninál, leállok.")
            break

        print(f"[INFO] Következő kommentoldal jelölt: {next_url}")

        fetched_next_url, next_html = fetcher.fetch(next_url, wait_ms=int(delay * 1000))
        fetched_next_page_no = get_topic_page_number(fetched_next_url)

        if not topic_has_any_comment_container(next_html):
            print("[INFO] A következő oldal már nem tartalmaz kommenteket, megállok.")
            break

        next_comments = parse_comments_from_topic_page(next_html, fetched_next_url)
        next_fingerprint = build_page_fingerprint(next_comments)
        print(f"[DEBUG] Következő oldal fingerprint: {next_fingerprint}")

        if next_fingerprint == current_fingerprint:
            print("[INFO] A következő oldal kommentjei megegyeznek a mostanival, ezért itt vége a topicnak.")
            break

        if next_fingerprint in seen_page_fingerprints:
            print("[INFO] A következő oldal tartalma már korábban szerepelt, ezért itt vége a topicnak.")
            break

        previous_page_fingerprint = current_fingerprint
        current_url = fetched_next_url
        html = next_html
        page_no = fetched_next_page_no

    mark_payload_finished(payload)
    save_topic_json(topic_file, payload)

    total_downloaded = len(payload.get("comments", []))
    print(f"[DEBUG] Topic letöltés kész: {resolved_title} | összes letöltött komment: {total_downloaded}")
    print(f"[INFO] Topic véglegesítve: {topic_file}")
    return payload


# -----------------------------
# Main fórum scrape
# -----------------------------

def scrape_main(
    fetcher: BrowserFetcher,
    output_dir: str,
    delay: float,
    only_title: Optional[str],
    start_page: int,
    max_pages: Optional[int],
) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    port_dir, topics_dir, visited_file = ensure_dirs(base_output)

    visited_topics = {
        normalize_topic_url_for_visited(x)
        for x in load_visited(visited_file)
    }

    current_url = MAIN_FORUM_URL if start_page <= 1 else set_query_param(MAIN_FORUM_URL, "page", str(start_page))
    page_no = start_page
    processed_main_pages = 0

    while True:
        if max_pages is not None and processed_main_pages >= max_pages:
            print("[INFO] Elértem a max-pages limitet.")
            break

        print(f"\n[INFO] Főoldali topiclista oldal #{page_no}: {current_url}")
        final_url, html = fetcher.fetch(current_url, wait_ms=int(delay * 1000))

        topics = parse_topic_rows_from_main_page(html, final_url)
        print(f"[INFO] Talált topicok ezen az oldalon: {len(topics)}")

        if not topics:
            print("[INFO] Nem találtam topicokat ezen a lapon, leállok.")
            break

        for idx, topic in enumerate(topics, start=1):
            topic_title = topic["title"]
            topic_url = topic["url"]
            topic_url_norm = normalize_topic_url_for_visited(topic_url)

            print(f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title}")

            if only_title and only_title.lower() not in topic_title.lower():
                print("[INFO] Szűrés miatt kihagyva.")
                continue

            topic_json_path = topic_file_path(topics_dir, topic_title)
            existing_payload = load_topic_json(topic_json_path)
            finished_in_json = bool(
                existing_payload and (existing_payload.get("extra") or {}).get("scrape_status") == "finished"
            )

            if topic_url_norm in visited_topics:
                print("[INFO] Már visitedben van, kihagyva.")
                continue

            if finished_in_json:
                print("[INFO] A JSON már kész állapotú, visitedbe írom és kihagyom.")
                append_visited(visited_file, topic_url_norm)
                visited_topics.add(topic_url_norm)
                continue

            try:
                payload = scrape_topic(
                    fetcher=fetcher,
                    topic_title=topic_title,
                    topic_url=topic_url_norm,
                    topic_file=topic_json_path,
                    delay=delay,
                )

                resolved_title = payload.get("title") or topic_title
                final_path = topic_file_path(topics_dir, resolved_title)

                if final_path != topic_json_path:
                    if topic_json_path.exists():
                        topic_json_path.replace(final_path)
                    else:
                        save_topic_json(final_path, payload)
                else:
                    save_topic_json(final_path, payload)

                total_downloaded = len(payload.get("comments", []))
                print(f"[DEBUG] Végső komment darabszám a témához: {resolved_title} | {total_downloaded}")

                append_visited(visited_file, topic_url_norm)
                visited_topics.add(topic_url_norm)

                print(f"[INFO] Topic mentve: {final_path}")
                print(f"[INFO] Topic visitedbe írva: {topic_url_norm}")

            except Exception as e:
                print(f"[WARN] Hiba topic feldolgozás közben: {topic_url} | {e}")

        processed_main_pages += 1

        next_url = get_main_next_page_url(html, final_url)
        if not next_url:
            print("[INFO] Nincs több főoldali topiclista oldal.")
            break

        next_page_val = extract_query_param(next_url, "page")
        next_page_no = int(next_page_val) if next_page_val and next_page_val.isdigit() else page_no + 1
        if next_page_no <= page_no:
            print("[INFO] Nem léptethető tovább a főoldali lapozás.")
            break

        current_url = next_url
        page_no = next_page_no


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="port.hu fórum scraper Playwright + BeautifulSoup alapon"
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre a port/ mappa.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Várakozás oldalak között másodpercben.",
    )
    parser.add_argument(
        "--only-title",
        default=None,
        help="Csak azokat a topicokat dolgozza fel, amelyek címében ez szerepel.",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="A fórum főoldali lapozásának kezdő oldala.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Legfeljebb ennyi főoldali listázóoldalt dolgoz fel.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Látható böngészőablakkal fusson.",
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
                only_title=args.only_title,
                start_page=args.start_page,
                max_pages=args.max_pages,
            )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva felhasználó által.")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] Végzetes hiba: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
    #python port_scraper.py --output ./port --headed