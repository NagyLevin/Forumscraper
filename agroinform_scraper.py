#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import textwrap
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://www.agroinform.hu"
MAIN_FORUM_URL = "https://www.agroinform.hu/forum"
TOPIC_URL_RE = re.compile(r"^/forum/[^?#]+/t\d+(?:\?.*)?$", re.IGNORECASE)
COMMENT_ID_RE = re.compile(r'"comment_id"\s*:\s*(?:"([^"]+)"|(\d+)|null)')
COMMENT_URL_RE = re.compile(r'"url"\s*:\s*"([^"]+)"')
DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\b")
TOPIC_PAGE_SELECT_RE = re.compile(r"(\d+)\s*/\s*(\d+)")
COMMENT_COUNT_HEADER_RE = re.compile(r"(\d+)\s+hozzászólás", re.IGNORECASE)


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


def get_page_number(url: str) -> int:
    page_val = extract_query_param(url, "page")
    if page_val and page_val.isdigit():
        return int(page_val)
    return 1


def parse_comment_page_number_from_comment_url(url: str) -> int:
    return get_page_number(url)


# -----------------------------
# Fájl / output kezelés
# -----------------------------

def ensure_dirs(base_output: Path) -> Tuple[Path, Path, Path]:
    agro_dir = base_output / "agroinform"
    topics_dir = agro_dir / "topics"
    agro_dir.mkdir(parents=True, exist_ok=True)
    topics_dir.mkdir(parents=True, exist_ok=True)

    visited_file = agro_dir / "visited.txt"
    if not visited_file.exists():
        visited_file.write_text("", encoding="utf-8")

    return agro_dir, topics_dir, visited_file


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


def normalize_topic_url_for_visited(url: str) -> str:
    return get_topic_base_url(url)


def topic_file_path(topics_dir: Path, topic_title: str) -> Path:
    return topics_dir / f"{sanitize_filename(topic_title)}.json"


def is_stream_json_finalized(topic_file: Path) -> bool:
    if not topic_file.exists():
        return False

    try:
        with topic_file.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 512)
            f.seek(max(0, size - read_size))
            tail = f.read().decode("utf-8", errors="ignore").strip()
        return tail.endswith("]\n}") or tail.endswith("]\r\n}") or tail.endswith("]}")
    except Exception:
        return False


def count_existing_comments_in_stream_file(topic_file: Path) -> int:
    if not topic_file.exists():
        return 0

    count = 0
    with topic_file.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            count += line.count('"comment_id":')
    return count


def get_last_written_comment_info(topic_file: Path) -> Tuple[Optional[str], Optional[str], int]:
    if not topic_file.exists():
        return None, None, 0

    existing_count = count_existing_comments_in_stream_file(topic_file)

    try:
        with topic_file.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 1024 * 1024)
            f.seek(max(0, size - read_size))
            tail = f.read().decode("utf-8", errors="ignore")
    except Exception:
        return None, None, existing_count

    comment_ids = COMMENT_ID_RE.findall(tail)
    urls = COMMENT_URL_RE.findall(tail)

    last_comment_id = None
    if comment_ids:
        last_pair = comment_ids[-1]
        last_comment_id = last_pair[0] or last_pair[1] or None

    last_comment_url = urls[-1] if urls else None
    return last_comment_id, last_comment_url, existing_count


def write_topic_stream_header(topic_file: Path, resolved_title: str, topic_meta: Dict, topic_url: str) -> None:
    header_obj = {
        "title": resolved_title,
        "authors": [],
        "data": {
            "content": resolved_title,
            "likes": None,
            "dislikes": None,
            "score": None,
            "rating": None,
            "date": topic_meta.get("created_at"),
            "url": get_topic_base_url(topic_url),
            "language": "hu",
            "tags": [],
            "rights": "agroinform.hu fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "topic_creator": topic_meta.get("topic_creator"),
                "detected_total_comments": topic_meta.get("detected_total_comments"),
                "detected_total_comment_pages": topic_meta.get("detected_total_comment_pages"),
                "fetched_page": topic_meta.get("fetched_page"),
            },
            "origin": "agroinform_forum",
        },
        "origin": "agroinform_forum",
    }

    header_json = json.dumps(header_obj, ensure_ascii=False, indent=2)
    text = header_json[:-1] + ',\n  "comments": [\n'
    topic_file.write_text(text, encoding="utf-8")


def append_comment_to_stream_file(topic_file: Path, comment_item: Dict, has_existing_comments: bool) -> None:
    item_json = json.dumps(comment_item, ensure_ascii=False, indent=2)
    item_json = textwrap.indent(item_json, "    ")

    with topic_file.open("a", encoding="utf-8") as f:
        if has_existing_comments:
            f.write(",\n")
        f.write(item_json)


def finalize_stream_json(topic_file: Path) -> None:
    if is_stream_json_finalized(topic_file):
        return
    with topic_file.open("a", encoding="utf-8") as f:
        f.write("\n  ]\n}\n")


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
            viewport={"width": 1600, "height": 2400},
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

def parse_topic_list_page_info(soup: BeautifulSoup) -> Dict[str, Optional[int]]:
    text = clean_text(soup.get_text(" ", strip=True))
    page_current = None
    page_total = None

    candidates = []
    for node in soup.select("select option, .dropdown-menu a, .dropdown-toggle, .row"):
        t = clean_text(node.get_text(" ", strip=True))
        if "/" in t:
            candidates.append(t)

    candidates.append(text)

    for t in candidates:
        for a, b in TOPIC_PAGE_SELECT_RE.findall(t):
            ai = parse_int_from_text(a)
            bi = parse_int_from_text(b)
            if ai and bi and bi <= 1000:
                page_current = ai
                page_total = bi
                return {"page_current": page_current, "page_total": page_total}

    return {"page_current": None, "page_total": None}


def parse_topic_rows_from_main_page(html: str, page_url: str) -> Tuple[List[Dict], Dict[str, Optional[int]]]:
    soup = BeautifulSoup(html, "html.parser")
    topics: List[Dict] = []
    seen = set()

    page_info = parse_topic_list_page_info(soup)

    candidate_rows = soup.select("table.table-bordered tr, table tr, .table-bordered tr")
    print(f"[DEBUG] Főoldali topic sorok száma (nyers): {len(candidate_rows)}")

    for row in candidate_rows:
        topic_link = None
        for a in row.select("a[href]"):
            href = (a.get("href") or "").strip()
            if href and TOPIC_URL_RE.search(href):
                topic_link = a
                break

        if not topic_link:
            continue

        topic_title = clean_text(topic_link.get_text(" ", strip=True))
        if not topic_title or topic_title.lower() == "agroinform.hu fórumszabályzat":
            continue

        topic_url = urljoin(page_url, topic_link.get("href", ""))
        topic_url_norm = normalize_topic_url_for_visited(topic_url)
        if topic_url_norm in seen:
            continue
        seen.add(topic_url_norm)

        row_text = clean_text(row.get_text("\n", strip=True))
        lines = [clean_text(x) for x in row_text.splitlines() if clean_text(x)]
        comment_count = None
        starter = None
        starter_date = None
        last_user = None
        last_activity = None

        numbers = [parse_int_from_text(x) for x in lines if parse_int_from_text(x) is not None]
        if numbers:
            comment_count = numbers[-1]

        dates = DATE_RE.findall(row_text)
        if not dates:
            dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", row_text)

        if len(lines) >= 2:
            try:
                title_idx = lines.index(topic_title)
            except ValueError:
                title_idx = 0

            after = lines[title_idx + 1 :]
            if after:
                starter = after[0]
            if len(after) >= 2 and re.search(r"\d{4}-\d{2}-\d{2}", after[1]):
                starter_date = after[1]
            if len(after) >= 3:
                last_user = after[2]
            if len(after) >= 4 and comment_count is not None:
                last_activity = after[3]

        topics.append(
            {
                "title": topic_title,
                "url": topic_url_norm,
                "comment_count": comment_count,
                "starter": starter,
                "starter_date": starter_date or (dates[0] if dates else None),
                "last_user": last_user,
                "last_activity": last_activity,
            }
        )

    return topics, page_info


def get_main_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    current_page = get_page_number(current_url)

    selectors = [
        "a[title*='Következő oldal'][href]",
        "a[title*='kovetkezo oldal'][href]",
        "a[rel='next'][href]",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            href = (node.get("href") or "").strip()
            if href:
                full = urljoin(current_url, href)
                if get_page_number(full) > current_page:
                    return full

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        title = clean_text(a.get("title") or "")
        txt = clean_text(a.get_text(" ", strip=True))
        if not href:
            continue
        full = urljoin(current_url, href)
        if not full.startswith(MAIN_FORUM_URL):
            continue
        if title.lower().startswith("következő oldal") or txt in {">", "›", "»"}:
            if get_page_number(full) > current_page:
                return full

    guessed = set_query_param(MAIN_FORUM_URL, "page", str(current_page + 1))
    return guessed


# -----------------------------
# Topicoldal parsing
# -----------------------------

def extract_topic_title(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for selector in ["h1", "h2", "title"]:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            if text:
                text = re.sub(r"\s*-\s*Fórum\s*-\s*Agroinform\.hu$", "", text, flags=re.I)
                return text
    return fallback


def extract_topic_meta(html: str, topic_url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text("\n", strip=True))

    topic_creator = None
    created_at = None
    total_comments = None
    total_comment_pages = None

    h = COMMENT_COUNT_HEADER_RE.search(page_text)
    if h:
        total_comments = parse_int_from_text(h.group(1))

    m = re.search(r"Létrehozta:\s*(.+?)\s*,\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", page_text, flags=re.I)
    if m:
        topic_creator = clean_text(m.group(1))
        created_at = clean_text(m.group(2))

    for node in soup.select("select option, .dropdown-toggle, a[href]"):
        t = clean_text(node.get_text(" ", strip=True))
        mm = TOPIC_PAGE_SELECT_RE.search(t)
        if mm:
            a = parse_int_from_text(mm.group(1))
            b = parse_int_from_text(mm.group(2))
            if a and b and b > 50:
                total_comment_pages = b
                break

    return {
        "url": get_topic_base_url(topic_url),
        "topic_creator": topic_creator,
        "created_at": created_at,
        "detected_total_comments": total_comments,
        "detected_total_comment_pages": total_comment_pages,
        "fetched_page": get_page_number(topic_url),
    }


def find_comment_cards(soup: BeautifulSoup) -> List[Tag]:
    cards = []
    for card in soup.select("div.card.card-comment"):
        inner = card.select_one("div[id]")
        if inner:
            cards.append(card)
    return cards


def extract_comment_id(card: Tag) -> Optional[str]:
    node = card.select_one("div[id]")
    if not node:
        return None
    cid = (node.get("id") or "").strip()
    return cid or None


def extract_comment_author(card: Tag) -> Optional[str]:
    selectors = [
        ".comment-author a",
        ".comment-author",
        "a[href*='felhasznaloAdat']",
    ]
    for selector in selectors:
        node = card.select_one(selector)
        if node:
            txt = clean_text(node.get_text(" ", strip=True))
            txt = re.sub(r"^#\d+\s*", "", txt)
            if txt and not DATE_RE.fullmatch(txt):
                return txt

    text = clean_text(card.get_text(" ", strip=True))
    m = re.search(r"#\d+\s+(.+?)\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", text)
    if m:
        return clean_text(m.group(1))
    return None


def extract_comment_date(card: Tag) -> Optional[str]:
    text = clean_text(card.get_text(" ", strip=True))
    m = DATE_RE.search(text)
    return m.group(0) if m else None


def extract_parent_info(card: Tag) -> Tuple[Optional[str], Optional[str]]:
    for a in card.select("a[href]"):
        txt = clean_text(a.get_text(" ", strip=True))
        m = re.search(r"^Válasz\s+(.+?)\s+#(\d+)\.\s*hozzászólására", txt, flags=re.I)
        if m:
            return clean_text(m.group(1)), m.group(2)
    return None, None


def extract_comment_body(card: Tag) -> str:
    card_copy = BeautifulSoup(str(card), "html.parser")

    for selector in [
        "div.float-right.clearfix.header-bar",
        "a.btn",
        "button",
        "script",
        "style",
        "form",
        ".comment-author",
        ".forum-items-header",
        ".comment-img",
        ".header-bar",
    ]:
        for n in card_copy.select(selector):
            n.decompose()

    root = card_copy.select_one("div[id]") or card_copy
    text = clean_text(root.get_text("\n", strip=True))
    lines = [clean_text(x) for x in text.splitlines() if clean_text(x)]

    cleaned_lines: List[str] = []
    for line in lines:
        if DATE_RE.fullmatch(line):
            continue
        if re.fullmatch(r"#\d+", line):
            continue
        if line == "Válasz erre":
            continue
        cleaned_lines.append(line)

    return clean_text("\n".join(cleaned_lines))


def extract_comment_from_card(card: Tag, topic_page_url: str) -> Optional[Dict]:
    comment_id = extract_comment_id(card)
    author = extract_comment_author(card) or "ismeretlen"
    date_text = extract_comment_date(card)
    parent_author, parent_comment_id = extract_parent_info(card)
    body = extract_comment_body(card)

    if not comment_id and not body:
        return None

    comment_url = strip_fragment(topic_page_url)
    if comment_id:
        comment_url = f"{comment_url}#comment-{comment_id}"

    return {
        "comment_id": comment_id,
        "author": author,
        "date": date_text,
        "rating": None,
        "parent_author": parent_author,
        "parent_comment_id": parent_comment_id,
        "url": comment_url,
        "data": body,
    }


def parse_comments_from_topic_page(html: str, topic_page_url: str) -> Tuple[List[Dict], Dict[str, Optional[int]]]:
    soup = BeautifulSoup(html, "html.parser")
    cards = find_comment_cards(soup)
    meta = extract_topic_meta(html, topic_page_url)

    print(
        f"[DEBUG] Komment kártyák száma: {len(cards)} | "
        f"oldal={meta.get('fetched_page')} / {meta.get('detected_total_comment_pages')} | "
        f"összes hozzászólás={meta.get('detected_total_comments')}"
    )

    comments: List[Dict] = []
    for idx, card in enumerate(cards, start=1):
        parsed = extract_comment_from_card(card, topic_page_url)
        if not parsed:
            continue

        preview = (parsed["data"] or "")[:120].replace("\n", " | ")
        print(
            f"[DEBUG] Komment #{idx} | id={parsed.get('comment_id') or '-'} | "
            f"szerző={parsed.get('author')} | dátum={parsed.get('date')} | preview={preview}"
        )
        comments.append(parsed)

    return comments, meta


def build_comment_signature(comment: Dict) -> str:
    comment_id = str(comment.get("comment_id") or "")
    author = clean_text(comment.get("author") or "")
    date = clean_text(comment.get("date") or "")
    text = clean_text(comment.get("data") or "")[:300]
    return f"{comment_id}|{author}|{date}|{text}"


def build_page_fingerprint(comments: List[Dict]) -> str:
    if not comments:
        return "EMPTY"
    sigs = [build_comment_signature(c) for c in comments]
    raw = "\n".join(sigs)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def topic_has_any_comment_card(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    return bool(find_comment_cards(soup))


def get_topic_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    current_page = get_page_number(current_url)

    selectors = [
        "a[title*='Következő oldal'][href]",
        "a[title*='kovetkezo oldal'][href]",
        "a[rel='next'][href]",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            href = (node.get("href") or "").strip()
            if href:
                full = urljoin(current_url, href)
                if get_page_number(full) > current_page:
                    return full

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        title = clean_text(a.get("title") or "")
        txt = clean_text(a.get_text(" ", strip=True))
        if not href:
            continue
        full = urljoin(current_url, href)
        if get_page_number(full) <= current_page:
            continue
        if title.lower().startswith("következő oldal") or txt in {">", "›", "»"}:
            return full

    return set_query_param(get_topic_base_url(current_url), "page", str(current_page + 1))


# -----------------------------
# Streamelt komment JSON item
# -----------------------------

def comment_to_output_item(c: Dict) -> Dict:
    author_name = c.get("author") or "ismeretlen"
    return {
        "authors": [split_name_like_person(author_name)] if author_name else [],
        "data": c.get("data", ""),
        "likes": None,
        "dislikes": None,
        "score": None,
        "rating": c.get("rating"),
        "date": c.get("date"),
        "url": c.get("url"),
        "language": "hu",
        "tags": [],
        "extra": {
            "comment_id": c.get("comment_id"),
            "parent_author": c.get("parent_author"),
            "parent_comment_id": c.get("parent_comment_id"),
        },
    }


# -----------------------------
# Topic scrape
# -----------------------------

def scrape_topic(
    fetcher: BrowserFetcher,
    topic_title: str,
    topic_url: str,
    topic_file: Path,
    delay: float,
) -> int:
    existing_comments = 0
    resume_page_no = 1
    resume_after_comment_id = None
    need_init_file = True

    if topic_file.exists():
        if is_stream_json_finalized(topic_file):
            print("[INFO] A topic fájl már lezárt JSON, ezt késznek vesszük.")
            return count_existing_comments_in_stream_file(topic_file)

        last_comment_id, last_comment_url, existing_comments = get_last_written_comment_info(topic_file)
        if last_comment_url:
            resume_page_no = parse_comment_page_number_from_comment_url(last_comment_url)
            resume_after_comment_id = last_comment_id
            need_init_file = False
            print(
                f"[INFO] Meglévő félkész topicfájl, folytatás: page={resume_page_no}, "
                f"utolsó comment_id={resume_after_comment_id}, meglévő kommentek={existing_comments}"
            )

    first_fetch_url = set_query_param(get_topic_base_url(topic_url), "page", str(resume_page_no))
    print(f"[INFO] Topic megnyitása: {topic_title}")
    current_url, html = fetcher.fetch(first_fetch_url, wait_ms=int(delay * 1000))

    resolved_title = extract_topic_title(html, topic_title)
    topic_meta = extract_topic_meta(html, current_url)

    print(
        f"[DEBUG] Topic meta | cím={resolved_title} | létrehozó={topic_meta.get('topic_creator')} | "
        f"létrehozva={topic_meta.get('created_at')} | hozzászólások={topic_meta.get('detected_total_comments')} | "
        f"kommentoldalak={topic_meta.get('detected_total_comment_pages')}"
    )

    if need_init_file:
        write_topic_stream_header(topic_file, resolved_title, topic_meta, topic_url)
        print(f"[INFO] Új streamelt topicfájl létrehozva: {topic_file}")

    page_no = get_page_number(current_url)
    total_downloaded = existing_comments
    has_existing_comments = existing_comments > 0

    seen_page_fingerprints: Set[str] = set()
    previous_page_fingerprint: Optional[str] = None
    first_page_after_resume = True

    while True:
        print(f"[INFO] Kommentoldal #{page_no}: {current_url}")
        page_comments, current_meta = parse_comments_from_topic_page(html, current_url)

        if page_no > 1 and not page_comments:
            print("[INFO] Üres vagy nem értelmezhető kommentoldal, megállok ennél a topicnál.")
            break

        if first_page_after_resume and resume_after_comment_id:
            original_len = len(page_comments)
            seen_last = False
            filtered: List[Dict] = []

            for c in page_comments:
                if not seen_last:
                    if str(c.get("comment_id") or "") == str(resume_after_comment_id):
                        seen_last = True
                    continue
                filtered.append(c)

            if seen_last:
                print(
                    f"[INFO] Resume szűrés: az első oldalon {original_len} kommentből "
                    f"{len(filtered)} új maradt az utolsó mentett comment_id után."
                )
                page_comments = filtered
            else:
                print("[INFO] Resume: az utolsó mentett comment_id nincs ezen az oldalon, teljes oldalt újként kezelem.")

            first_page_after_resume = False
            resume_after_comment_id = None

        current_fingerprint = build_page_fingerprint(page_comments)
        print(f"[DEBUG] Oldal fingerprint: {current_fingerprint}")

        if previous_page_fingerprint is not None and current_fingerprint == previous_page_fingerprint:
            print("[INFO] A mostani kommentoldal megegyezik az előzővel, a topic véget ért.")
            break

        if current_fingerprint in seen_page_fingerprints:
            print("[INFO] Már korábban látott kommentoldal-tartalom jött vissza, a topic véget ért.")
            break

        seen_page_fingerprints.add(current_fingerprint)

        added_on_this_page = 0
        for c in page_comments:
            item = comment_to_output_item(c)
            append_comment_to_stream_file(topic_file, item, has_existing_comments)
            has_existing_comments = True
            total_downloaded += 1
            added_on_this_page += 1
            print(
                f"[DEBUG] Mentve | id={c.get('comment_id')} | szerző={c.get('author')} | "
                f"dátum={c.get('date')} | fájl={topic_file.name}"
            )

        print(
            f"[INFO] Oldal hozzáfűzve a topicfájlhoz: {topic_file} | "
            f"új kommentek ezen az oldalon: {added_on_this_page} | "
            f"összes letöltött komment eddig: {total_downloaded} | "
            f"oldal={page_no}/{current_meta.get('detected_total_comment_pages')}"
        )

        if current_meta.get("detected_total_comment_pages") and page_no >= int(current_meta["detected_total_comment_pages"]):
            print("[INFO] Elértem az utolsó kommentoldalt a lapozó alapján.")
            break

        next_url = get_topic_next_page_url(html, current_url)
        if not next_url:
            print("[INFO] Nincs több kommentoldal ennél a topicnál.")
            break

        next_page_no = get_page_number(next_url)
        if next_page_no <= page_no:
            print("[INFO] A következő oldal száma nem nagyobb a mostaninál, leállok.")
            break

        print(f"[INFO] Következő kommentoldal jelölt: {next_url}")

        fetched_next_url, next_html = fetcher.fetch(next_url, wait_ms=int(delay * 1000))
        if not topic_has_any_comment_card(next_html):
            print("[INFO] A következő oldal már nem tartalmaz kommenteket, megállok.")
            break

        next_comments, _ = parse_comments_from_topic_page(next_html, fetched_next_url)
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
        page_no = get_page_number(current_url)

    finalize_stream_json(topic_file)
    print(f"[DEBUG] Topic letöltés kész: {resolved_title} | összes letöltött komment: {total_downloaded}")
    print(f"[INFO] Topic JSON lezárva: {topic_file}")
    return total_downloaded


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
    agro_dir, topics_dir, visited_file = ensure_dirs(base_output)

    visited_topics = {normalize_topic_url_for_visited(x) for x in load_visited(visited_file)}

    current_url = MAIN_FORUM_URL if start_page <= 1 else set_query_param(MAIN_FORUM_URL, "page", str(start_page))
    page_no = start_page
    processed_main_pages = 0

    while True:
        if max_pages is not None and processed_main_pages >= max_pages:
            print("[INFO] Elértem a max-pages limitet.")
            break

        print(f"\n[INFO] Főoldali topiclista oldal #{page_no}: {current_url}")
        final_url, html = fetcher.fetch(current_url, wait_ms=int(delay * 1000))

        topics, page_info = parse_topic_rows_from_main_page(html, final_url)
        print(
            f"[INFO] Talált topicok ezen az oldalon: {len(topics)} | "
            f"fórum oldal={page_info.get('page_current')} / {page_info.get('page_total')}"
        )

        if not topics:
            print("[INFO] Nem találtam topicokat ezen a lapon, leállok.")
            break

        for idx, topic in enumerate(topics, start=1):
            topic_title = topic["title"]
            topic_url = topic["url"]
            topic_url_norm = normalize_topic_url_for_visited(topic_url)

            print(
                f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title} | "
                f"hozzászólás db={topic.get('comment_count')} | nyitotta={topic.get('starter')} | "
                f"utolsó={topic.get('last_user')} / {topic.get('last_activity')}"
            )

            if only_title and only_title.lower() not in topic_title.lower():
                print("[INFO] Szűrés miatt kihagyva.")
                continue

            if topic_url_norm in visited_topics:
                print("[INFO] Már visitedben van, kihagyva.")
                continue

            topic_json_path = topic_file_path(topics_dir, topic_title)

            try:
                total_downloaded = scrape_topic(
                    fetcher=fetcher,
                    topic_title=topic_title,
                    topic_url=topic_url_norm,
                    topic_file=topic_json_path,
                    delay=delay,
                )

                print(f"[DEBUG] Végső komment darabszám a témához: {topic_title} | {total_downloaded}")

                append_visited(visited_file, topic_url_norm)
                visited_topics.add(topic_url_norm)

                print(f"[INFO] Topic mentve: {topic_json_path}")
                print(f"[INFO] Topic visitedbe írva: {topic_url_norm}")

            except Exception as e:
                print(f"[WARN] Hiba topic feldolgozás közben: {topic_url} | {e}")

        processed_main_pages += 1

        if page_info.get("page_total") and page_info.get("page_current"):
            if int(page_info["page_current"]) >= int(page_info["page_total"]):
                print("[INFO] Elértem az utolsó főoldali topiclistát a lapozó alapján.")
                break

        next_url = get_main_next_page_url(html, final_url)
        if not next_url:
            print("[INFO] Nincs több főoldali topiclista oldal.")
            break

        next_page_no = get_page_number(next_url)
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
        description="agroinform.hu fórum scraper Playwright + BeautifulSoup alapon, streamelt komment-append módban"
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre az agroinform/ mappa.",
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
    # python agroinform_scraper.py --output ./out --headed --start-page 1 --max-pages 1
