#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import re
import sys
import textwrap
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

BASE_URL = "https://sg.hu"
FORUM_URL = f"{BASE_URL}/forum"

TARGET_SECTIONS = {
    "Általános fórumok",
    "Számítástechnikai fórumok",
    "Hírek fórumai",
}

SKIP_MAIN_SECTIONS = {
    "Cikkeink",
    "Legújabb témák",
    "Legutóbbi hozzászólások",
}

COMMENT_ID_RE = re.compile(r"#(\d+)")
TOPIC_ID_FROM_URL_RE = re.compile(r"/forum/tema/(\d+)")
CATEGORY_ID_FROM_URL_RE = re.compile(r"/forum/temak/(\d+)")
PAGE_X_OF_Y_RE = re.compile(r"(\d+)\s*/\s*(\d+)")


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


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def sanitize_filename(name: str, max_len: int = 180) -> str:
    name = clean_text(name)
    if not name:
        return "ismeretlen"

    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))

    replacements = [
        ("/", " - "),
        ("\\", " - "),
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


def parse_int(text: str) -> Optional[int]:
    text = clean_text(text)
    if not text:
        return None
    normalized = text.replace(" ", "").replace(".", "")
    m = re.search(r"\d+", normalized)
    return int(m.group(0)) if m else None


def strip_fragment(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))


def set_query_param(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    q[key] = [str(value)]
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(q, doseq=True), ""))


def get_query_param(url: str, key: str) -> Optional[str]:
    parsed = urlparse(url)
    q = parse_qs(parsed.query)
    vals = q.get(key)
    return vals[0] if vals else None


def get_page_no_from_url(url: str) -> int:
    page = get_query_param(url, "page")
    if page and page.isdigit():
        return int(page)
    return 1


def normalize_topic_url(url: str) -> str:
    parsed = urlparse(strip_fragment(url))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", ""))


def normalize_category_url(url: str) -> str:
    parsed = urlparse(strip_fragment(url))
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", "", ""))


def build_topic_page_url(topic_url: str, page_no: int) -> str:
    base = normalize_topic_url(topic_url)
    if page_no <= 1:
        return base
    return set_query_param(base, "page", str(page_no))


def build_category_page_url(category_url: str, page_no: int) -> str:
    base = normalize_category_url(category_url)
    if page_no <= 1:
        return base
    return set_query_param(base, "page", str(page_no))


def split_name_like_person(name: str) -> Dict[str, str]:
    name = clean_text(name)
    if not name:
        return {"name": ""}
    parts = name.split()
    if len(parts) >= 2:
        return {"family": parts[0], "given": " ".join(parts[1:])}
    return {"name": name}


def stable_comment_signature(comment: Dict) -> str:
    raw = "|".join(
        [
            str(comment.get("comment_id") or ""),
            clean_text(comment.get("author") or ""),
            clean_text(comment.get("date") or ""),
            clean_text(comment.get("data") or "")[:300],
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


# -----------------------------
# Adatmodellek
# -----------------------------

@dataclass
class CategoryRef:
    section_title: str
    category_title: str
    category_url: str
    topic_count: Optional[int] = None


@dataclass
class TopicRef:
    section_title: str
    category_title: str
    topic_title: str
    topic_url: str
    message_count: Optional[int] = None
    last_message: Optional[str] = None
    last_user: Optional[str] = None


# -----------------------------
# Fájl / visited kezelés
# -----------------------------

def ensure_dirs(output_root: Path) -> Tuple[Path, Path, Path]:
    base_dir = output_root / "sg_forum"
    data_dir = base_dir / "data"
    state_dir = base_dir / "state"
    data_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    visited_topics = state_dir / "visited_topics.txt"
    visited_categories = state_dir / "visited_categories.txt"
    if not visited_topics.exists():
        visited_topics.write_text("", encoding="utf-8")
    if not visited_categories.exists():
        visited_categories.write_text("", encoding="utf-8")

    return base_dir, data_dir, state_dir


def load_visited_set(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def append_visited(path: Path, value: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(value.strip() + "\n")


def category_dir_for(data_dir: Path, section_title: str, category_title: str) -> Path:
    path = data_dir / sanitize_filename(section_title) / sanitize_filename(category_title)
    path.mkdir(parents=True, exist_ok=True)
    return path


def topic_json_path(data_dir: Path, section_title: str, category_title: str, topic_title: str) -> Path:
    return category_dir_for(data_dir, section_title, category_title) / f"{sanitize_filename(topic_title)}.json"


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
        return tail.endswith("]\n}") or tail.endswith("]}")
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

    ids = re.findall(r'"comment_id"\s*:\s*"([^"]+)"', tail)
    urls = re.findall(r'"url"\s*:\s*"([^"]+)"', tail)
    return (ids[-1] if ids else None, urls[-1] if urls else None, existing_count)


def write_topic_stream_header(topic_file: Path, topic: TopicRef, topic_meta: Dict) -> None:
    header = {
        "title": topic.topic_title,
        "authors": [],
        "data": {
            "content": topic.topic_title,
            "likes": None,
            "dislikes": None,
            "score": None,
            "rating": None,
            "date": None,
            "url": normalize_topic_url(topic.topic_url),
            "language": "hu",
            "tags": [],
            "rights": "sg.hu fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "section_title": topic.section_title,
                "category_title": topic.category_title,
                "message_count_on_listing": topic.message_count,
                "detected_total_pages": topic_meta.get("detected_total_pages"),
                "detected_total_posts": topic_meta.get("detected_total_posts"),
                "fetched_page": topic_meta.get("fetched_page"),
            },
            "origin": "sg_forum",
        },
        "origin": "sg_forum",
    }
    header_json = json.dumps(header, ensure_ascii=False, indent=2)
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
    def __init__(
        self,
        headless: bool = True,
        slow_mo: int = 0,
        timeout_ms: int = 90000,
        retries: int = 4,
        auto_reset_fetches: int = 120,
        block_resources: bool = True,
    ):
        self.headless = headless
        self.slow_mo = slow_mo
        self.timeout_ms = timeout_ms
        self.retries = retries
        self.auto_reset_fetches = auto_reset_fetches
        self.block_resources = block_resources

        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.fetch_counter = 0

    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless, slow_mo=self.slow_mo)
        self._create_context_and_page()
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

    def _create_context_and_page(self) -> None:
        self.context = self.browser.new_context(
            locale="hu-HU",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 2600},
        )

        if self.block_resources:
            def route_handler(route):
                try:
                    req = route.request
                    if req.resource_type in {"image", "media", "font"}:
                        route.abort()
                    else:
                        route.continue_()
                except Exception:
                    try:
                        route.continue_()
                    except Exception:
                        pass
            self.context.route("**/*", route_handler)

        self.page = self.context.new_page()
        self.page.set_default_timeout(self.timeout_ms)
        self.page.set_default_navigation_timeout(self.timeout_ms)

    def reset_page(self) -> None:
        try:
            if self.page:
                self.page.close()
        except Exception:
            pass
        self.page = self.context.new_page()
        self.page.set_default_timeout(self.timeout_ms)
        self.page.set_default_navigation_timeout(self.timeout_ms)
        print("[INFO] Böngészőoldal újranyitva.")

    def reset_context(self) -> None:
        try:
            if self.page:
                self.page.close()
        except Exception:
            pass
        self.page = None
        try:
            if self.context:
                self.context.close()
        except Exception:
            pass
        self.context = None
        self._create_context_and_page()
        gc.collect()
        print("[INFO] Browser context újranyitva memória-kíméléshez.")

    def ensure_page_alive(self) -> None:
        if self.browser is None:
            raise RuntimeError("A böngésző nincs inicializálva.")

        if self.context is None:
            self._create_context_and_page()
            return

        try:
            if self.page is None or self.page.is_closed():
                self.page = self.context.new_page()
                self.page.set_default_timeout(self.timeout_ms)
                self.page.set_default_navigation_timeout(self.timeout_ms)
        except Exception:
            self.reset_context()

    def reject_cookie_popup_if_present(self) -> bool:
        selectors = [
            "button:has-text('ÖSSZES ELUTASÍTÁSA')",
            "button:has-text('Összes elutasítása')",
            "text=ÖSSZES ELUTASÍTÁSA",
            "text=Összes elutasítása",
        ]

        for _ in range(4):
            try:
                self.page.wait_for_timeout(400)
            except Exception:
                pass

            for selector in selectors:
                try:
                    locator = self.page.locator(selector).first
                    if locator.count() > 0 and locator.is_visible(timeout=1200):
                        locator.click(timeout=2500, force=True)
                        self.page.wait_for_timeout(900)
                        print("[INFO] Cookie popup elutasítva.")
                        return True
                except Exception:
                    pass

            try:
                self.page.keyboard.press("Escape")
            except Exception:
                pass

        return False

    def close_overlay_if_present(self) -> bool:
        selectors = [
            "button[aria-label='Close']",
            "button[aria-label='Bezárás']",
            "button[title='Bezárás']",
            "button[title='Close']",
            "button:has-text('×')",
            "button:has-text('✕')",
            "button:has-text('✖')",
        ]

        for selector in selectors:
            try:
                locator = self.page.locator(selector).first
                if locator.count() > 0 and locator.is_visible(timeout=800):
                    locator.click(timeout=2000, force=True)
                    self.page.wait_for_timeout(500)
                    print("[INFO] Overlay bezárva.")
                    return True
            except Exception:
                pass
        return False

    def accept_cookies_if_present(self) -> None:
        return

    def fetch(self, url: str, wait_ms: int = 1500) -> Tuple[str, str]:
        last_exc = None

        if self.auto_reset_fetches > 0 and self.fetch_counter > 0 and self.fetch_counter % self.auto_reset_fetches == 0:
            print("[INFO] Automatikus context reset fetch számláló alapján.")
            self.reset_context()

        for attempt in range(1, self.retries + 1):
            try:
                self.ensure_page_alive()
                print(f"[DEBUG] LETÖLTÉS ({attempt}/{self.retries}): {url}")
                self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                self.page.wait_for_timeout(wait_ms)

                self.reject_cookie_popup_if_present()

                try:
                    self.page.wait_for_load_state("networkidle", timeout=5000)
                except PlaywrightTimeoutError:
                    pass

                self.reject_cookie_popup_if_present()
                self.close_overlay_if_present()

                self.fetch_counter += 1
                return self.page.url, self.page.content()
            except PlaywrightTimeoutError as e:
                last_exc = e
                print(f"[WARN] Timeout: {url}")
            except Exception as e:
                last_exc = e
                print(f"[WARN] Fetch hiba: {url} | {e}")

            if attempt < self.retries:
                backoff_ms = 3000 * attempt
                try:
                    self.page.wait_for_timeout(backoff_ms)
                except Exception:
                    pass
                try:
                    self.reset_page()
                except Exception:
                    self.reset_context()

        raise last_exc
# -----------------------------
# Főoldal parsing
# -----------------------------

def extract_primary_link_text(a: Tag) -> str:
    preferred_selectors = [
        "span.line-clamp-1",
        "span.flex",
        "span.text-sm",
        "span",
    ]
    for selector in preferred_selectors:
        for node in a.select(selector):
            txt = clean_text(node.get_text(" ", strip=True))
            if not txt:
                continue
            if re.search(r"\b\d[\d .]*\s*db\b", txt, flags=re.I):
                continue
            if re.search(r"(?:ma|tegnap|tegnapelőtt|\d{4}\.\d{2}\.\d{2}\.?)", txt, flags=re.I):
                continue
            return txt

    txt = clean_text(a.get_text(" ", strip=True))
    txt = re.sub(r"\s+\d[\d .]*\s*db\b.*$", "", txt, flags=re.I)
    txt = re.sub(r"\s+(?:ma|tegnap|tegnapelőtt)\s*,?\s*\d{1,2}:\d{2}.*$", "", txt, flags=re.I)
    txt = re.sub(r"\s+\d{4}\.\s*\d{2}\.\s*\d{2}\.?\s*\d{1,2}:\d{2}.*$", "", txt, flags=re.I)
    return clean_text(txt)


def parse_main_sections(html: str, page_url: str) -> List[CategoryRef]:
    soup = BeautifulSoup(html, "html.parser")
    categories: List[CategoryRef] = []
    seen_urls: Set[str] = set()

    for h2 in soup.find_all("h2"):
        section_title = clean_text(h2.get_text(" ", strip=True))
        if section_title in SKIP_MAIN_SECTIONS:
            continue
        if section_title not in TARGET_SECTIONS:
            continue

        node = h2
        while True:
            node = node.find_next_sibling()
            if node is None:
                break
            if isinstance(node, Tag) and node.name == "h2":
                break

            anchors = node.select("a[href*='/forum/temak/']") if isinstance(node, Tag) else []
            for a in anchors:
                href = clean_text(a.get("href") or "")
                if not href:
                    continue
                title = extract_primary_link_text(a)
                if not title:
                    continue

                full_url = normalize_category_url(urljoin(page_url, href))
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                row = a.find_parent(["a", "div"])
                row_text = clean_text(row.get_text(" ", strip=True)) if row else title
                counts = re.findall(r"\d[\d .]*\s*db", row_text, flags=re.I)
                topic_count = parse_int(counts[-1]) if counts else parse_int(row_text)

                categories.append(
                    CategoryRef(
                        section_title=section_title,
                        category_title=title,
                        category_url=full_url,
                        topic_count=topic_count,
                    )
                )

    del soup
    gc.collect()
    return categories


# -----------------------------
# Témacsoport oldal parsing
# -----------------------------

def extract_category_title(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for selector in ["h1", "title"]:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            text = re.sub(r"^SG\.hu\s*-\s*Fórum\s*-\s*", "", text, flags=re.I)
            text = re.sub(r"^SG\.hu\s*-\s*", "", text, flags=re.I)
            if text:
                del soup
                gc.collect()
                return text
    del soup
    gc.collect()
    return fallback


def parse_category_pagination_info(html: str, current_url: str) -> Tuple[int, Optional[int], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    current_page = get_page_no_from_url(current_url)
    total_pages = None
    next_url = None

    text = clean_text(soup.get_text(" ", strip=True))
    matches = PAGE_X_OF_Y_RE.findall(text)
    if matches:
        for a, b in matches:
            a_i, b_i = int(a), int(b)
            if a_i == current_page or current_page == 1:
                total_pages = max(total_pages or 0, b_i)

    for a in soup.select("a[href]"):
        href = clean_text(a.get("href") or "")
        txt = clean_text(a.get_text(" ", strip=True))
        if not href:
            continue
        full = urljoin(current_url, href)
        if "/forum/temak/" not in full:
            continue
        page_no = get_page_no_from_url(full)
        if txt.startswith("Következő") or txt in {">", "›", "»"}:
            if page_no > current_page or (page_no == 1 and current_page == 1 and "page=" in full):
                next_url = full
                break
        if page_no == current_page + 1:
            next_url = full
            break

    del soup
    gc.collect()

    if not next_url and (total_pages is None or current_page < total_pages):
        next_url = build_category_page_url(current_url, current_page + 1)

    return current_page, total_pages, next_url


def parse_topics_from_category_page(html: str, page_url: str, section_title: str, category_title: str) -> List[TopicRef]:
    soup = BeautifulSoup(html, "html.parser")
    topics: List[TopicRef] = []
    seen = set()

    forum_topics_heading = None
    for h in soup.find_all(["h2", "h3"]):
        txt = clean_text(h.get_text(" ", strip=True))
        if txt == "A fórum témái":
            forum_topics_heading = h
            break

    topic_links: List[Tag] = []
    if forum_topics_heading:
        for node in forum_topics_heading.find_all_next():
            if node.name in {"h1", "h2", "h3"} and node is not forum_topics_heading:
                break
            if node.name == "a":
                href = clean_text(node.get("href") or "")
                if "/forum/tema/" not in href:
                    continue
                title = extract_primary_link_text(node) or clean_text(node.get_text(" ", strip=True))
                if not title:
                    continue
                topic_links.append(node)

    if not topic_links:
        print("[WARN] Nem találtam 'A fórum témái' blokkot, fallback keresés indul az oldalon.")
        main = soup.select_one("main") or soup
        for a in main.select("a[href*='/forum/tema/']"):
            href = clean_text(a.get("href") or "")
            title = extract_primary_link_text(a) or clean_text(a.get_text(" ", strip=True))
            if not href or not title:
                continue
            parent = a.find_parent(["nav", "header", "footer", "aside"])
            if parent is not None:
                continue
            topic_links.append(a)

    for a in topic_links:
        href = clean_text(a.get("href") or "")
        title = extract_primary_link_text(a) or clean_text(a.get_text(" ", strip=True))
        if not href or not title:
            continue
        full_url = normalize_topic_url(urljoin(page_url, href))
        if full_url in seen:
            continue
        seen.add(full_url)

        row = a.find_parent(["a", "div"])
        row_text = clean_text(row.get_text(" ", strip=True)) if row else title
        message_count = None
        counts = re.findall(r"\d[\d .]*\s*db", row_text, flags=re.I)
        if counts:
            message_count = parse_int(counts[0])

        topics.append(
            TopicRef(
                section_title=section_title,
                category_title=category_title,
                topic_title=title,
                topic_url=full_url,
                message_count=message_count,
                last_message=None,
                last_user=None,
            )
        )

    del soup
    gc.collect()
    return topics


# -----------------------------
# Topicoldal parsing
# -----------------------------

def extract_topic_title(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for selector in ["h1", "title"]:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            text = re.sub(r"^SG\.hu\s*-\s*Fórum\s*-\s*", "", text, flags=re.I)
            if text:
                del soup
                gc.collect()
                return text
    del soup
    gc.collect()
    return fallback


def extract_topic_meta_and_pagination(html: str, current_url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text(" ", strip=True))
    page_no = get_page_no_from_url(current_url)

    total_pages = None
    total_posts = None

    page_matches = PAGE_X_OF_Y_RE.findall(page_text)
    if page_matches:
        # topic page-en ez jellemzően komment sorszámnál is lehet, ezért a legnagyobb második számot vesszük,
        # de a lapozó linkekből is próbálunk később megerősíteni.
        total_pages = max(int(b) for _, b in page_matches)

    for a in soup.select("a[href*='/forum/tema/']"):
        href = clean_text(a.get("href") or "")
        page_val = get_query_param(urljoin(current_url, href), "page")
        if page_val and page_val.isdigit():
            total_pages = max(total_pages or 0, int(page_val))

    # A parsed textben gyakran külön sorban ott a teljes hozzászólásszám is.
    ints = [int(x.replace(" ", "")) for x in re.findall(r"\b\d[\d ]{0,8}\b", page_text)]
    if ints:
        bigger = [x for x in ints if x >= 50]
        if bigger:
            total_posts = max(bigger)

    del soup
    gc.collect()

    return {
        "fetched_page": page_no,
        "detected_total_pages": total_pages,
        "detected_total_posts": total_posts,
    }


def parse_topic_pagination_info(html: str, current_url: str) -> Tuple[int, Optional[int], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    current_page = get_page_no_from_url(current_url)
    total_pages = None
    next_url = None

    for a in soup.select("a[href]"):
        href = clean_text(a.get("href") or "")
        txt = clean_text(a.get_text(" ", strip=True))
        if not href:
            continue
        full = urljoin(current_url, href)
        if "/forum/tema/" not in full:
            continue
        page_val = get_query_param(full, "page")
        if page_val and page_val.isdigit():
            total_pages = max(total_pages or 0, int(page_val))
        if txt.startswith("Következő") or txt in {">", "›", "»"}:
            if (page_val and page_val.isdigit() and int(page_val) > current_page) or (current_page == 1 and "page=" in full):
                next_url = full
                break
        if page_val and page_val.isdigit() and int(page_val) == current_page + 1:
            next_url = full
            break

    del soup
    gc.collect()

    if not next_url and (total_pages is None or current_page < total_pages):
        next_url = build_topic_page_url(current_url, current_page + 1)

    return current_page, total_pages, next_url


def find_comment_blocks(soup: BeautifulSoup) -> List[Tag]:
    blocks: List[Tag] = []

    # 1) Ha a DOM-ban egyértelmű poszt-wrapper van
    for selector in [
        "div[id^='msg-']",
        "article[id^='msg-']",
        "div.message-item",
    ]:
        found = soup.select(selector)
        if found:
            return found

    # 2) Fallback: az összes olyan blokk, amiben van #12345 jellegű link/szöveg
    for div in soup.find_all(["div", "article", "section", "li"]):
        txt = clean_text(div.get_text(" ", strip=True))
        if COMMENT_ID_RE.search(txt):
            blocks.append(div)
    return blocks


def dedupe_comment_blocks(blocks: Iterable[Tag]) -> List[Tag]:
    result: List[Tag] = []
    seen = set()
    for block in blocks:
        txt = clean_text(block.get_text(" ", strip=True))
        cid_match = COMMENT_ID_RE.search(txt)
        key = cid_match.group(1) if cid_match else hashlib.sha1(txt[:500].encode("utf-8")).hexdigest()
        if key in seen:
            continue
        seen.add(key)
        result.append(block)
    return result


def extract_comment_from_block(block: Tag, topic_page_url: str) -> Optional[Dict]:
    text_full = clean_text(block.get_text("\n", strip=True))
    if not text_full:
        return None

    cid_match = COMMENT_ID_RE.search(text_full)
    comment_id = cid_match.group(1) if cid_match else None

    lines = [clean_text(x) for x in text_full.split("\n") if clean_text(x)]
    if not lines:
        return None

    author = "ismeretlen"
    date = None
    parent_author = None

    # Dátum minták
    for line in lines[:8]:
        if re.search(r"(ma|tegnap|tegnapelőtt|\d{4}\.\d{2}\.\d{2}\.)", line, flags=re.I):
            date = line
            break
        if re.search(r"\d{4}\.\d{2}\.\d{2}\.?\s+\d{2}:\d{2}", line):
            date = line
            break

    # Szerző: első rövid, nem számozott sor a blokk tetején
    for line in lines[:6]:
        if line.startswith("#"):
            continue
        if "Válasz" in line:
            continue
        if len(line) > 60:
            continue
        if re.search(r"\d{4}|ma|tegnap", line, flags=re.I):
            continue
        author = line
        break

    for line in lines:
        m = re.search(r"Válasz ['\"]?(.+?)['\"]? üzenetére", line, flags=re.I)
        if m:
            parent_author = clean_text(m.group(1))
            break

    content_lines: List[str] = []
    started = False
    for line in lines:
        if comment_id and f"#{comment_id}" in line:
            started = True
            continue
        if not started:
            continue
        if re.match(r"^Utoljára szerkesztette:", line, flags=re.I):
            continue
        content_lines.append(line)

    if not content_lines:
        # fallback: dobjuk le a fejlécszerű sorokat
        content_lines = lines[2:] if len(lines) > 2 else lines

    data = clean_text("\n".join(content_lines))
    if not data and len(lines) >= 1:
        data = lines[-1]

    comment_url = strip_fragment(topic_page_url)
    if comment_id:
        comment_url += f"#comment-{comment_id}"

    return {
        "comment_id": comment_id or stable_comment_signature({"data": data})[:16],
        "author": author,
        "date": date,
        "rating": None,
        "parent_author": parent_author,
        "index": None,
        "index_total": None,
        "is_offtopic": False,
        "url": comment_url,
        "data": data,
    }


def parse_comments_from_topic_page(html: str, topic_page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    blocks = dedupe_comment_blocks(find_comment_blocks(soup))
    comments: List[Dict] = []
    seen_ids = set()

    print(f"[DEBUG] Talált hozzászólás-blokkok száma: {len(blocks)}")

    for idx, block in enumerate(blocks, start=1):
        item = extract_comment_from_block(block, topic_page_url)
        if not item:
            continue
        cid = str(item.get("comment_id") or "")
        if cid in seen_ids:
            continue
        seen_ids.add(cid)
        comments.append(item)
        preview = clean_text(item.get("data") or "")[:120].replace("\n", " | ")
        print(
            f"[DEBUG] Hozzászólás #{idx} | id={item.get('comment_id')} | szerző={item.get('author')} "
            f"| dátum={item.get('date')} | preview={preview}"
        )

    del soup
    gc.collect()
    return comments


def comment_to_output_item(comment: Dict) -> Dict:
    author_name = comment.get("author") or "ismeretlen"
    return {
        "authors": [split_name_like_person(author_name)] if author_name else [],
        "data": comment.get("data", ""),
        "likes": None,
        "dislikes": None,
        "score": None,
        "rating": comment.get("rating"),
        "date": comment.get("date"),
        "url": comment.get("url"),
        "language": "hu",
        "tags": ["offtopic"] if comment.get("is_offtopic") else [],
        "extra": {
            "comment_id": comment.get("comment_id"),
            "parent_author": comment.get("parent_author"),
            "index": comment.get("index"),
            "index_total": comment.get("index_total"),
            "is_offtopic": comment.get("is_offtopic"),
        },
    }


# -----------------------------
# Scrape logika
# -----------------------------

def scrape_topic(
    fetcher: BrowserFetcher,
    topic: TopicRef,
    topic_file: Path,
    delay: float,
    topic_reset_interval: int = 25,
) -> int:
    existing_comments = 0
    resume_page_no = 1
    resume_after_comment_id = None
    need_init_file = True

    if topic_file.exists():
        if is_stream_json_finalized(topic_file):
            print("[INFO] A topic JSON már lezárt, késznek tekintem.")
            return count_existing_comments_in_stream_file(topic_file)

        last_comment_id, last_comment_url, existing_comments = get_last_written_comment_info(topic_file)
        if last_comment_url:
            resume_page_no = get_page_no_from_url(last_comment_url)
            resume_after_comment_id = last_comment_id
            need_init_file = False
            print(
                f"[INFO] Resume: page={resume_page_no}, utolsó comment_id={resume_after_comment_id}, "
                f"meglévő kommentek={existing_comments}"
            )

    fetcher.reset_context()

    current_url, html = fetcher.fetch(build_topic_page_url(topic.topic_url, resume_page_no), wait_ms=int(delay * 1000))
    resolved_title = extract_topic_title(html, topic.topic_title)
    topic.topic_title = resolved_title
    topic_meta = extract_topic_meta_and_pagination(html, current_url)

    if need_init_file:
        write_topic_stream_header(topic_file, topic, topic_meta)
        print(f"[INFO] Új topic JSON létrehozva: {topic_file}")

    total_downloaded = existing_comments
    has_existing_comments = existing_comments > 0
    first_page_after_resume = True
    seen_page_fingerprints: Set[str] = set()
    previous_page_fingerprint: Optional[str] = None
    page_hops = 0

    while True:
        current_page_no, total_pages, next_url = parse_topic_pagination_info(html, current_url)
        print(
            f"[INFO] Téma: {topic.topic_title} | kommentoldal: {current_page_no}"
            + (f" / {total_pages}" if total_pages else "")
            + f" | URL: {current_url}"
        )

        page_comments = parse_comments_from_topic_page(html, current_url)
        if current_page_no > 1 and not page_comments:
            print("[INFO] Üres kommentoldal, topic vége.")
            break

        if first_page_after_resume and resume_after_comment_id:
            original_len = len(page_comments)
            filtered = []
            seen_last = False
            for c in page_comments:
                if not seen_last:
                    if str(c.get("comment_id") or "") == str(resume_after_comment_id):
                        seen_last = True
                    continue
                filtered.append(c)
            if seen_last:
                print(
                    f"[INFO] Resume szűrés: {original_len} hozzászólásból {len(filtered)} maradt újként."
                )
                page_comments = filtered
            else:
                print("[INFO] Resume comment_id nem volt meg az oldalon, az oldalt újként kezelem.")
            first_page_after_resume = False
            resume_after_comment_id = None

        page_fingerprint = hashlib.sha1(
            "\n".join(stable_comment_signature(c) for c in page_comments).encode("utf-8")
        ).hexdigest()
        print(f"[DEBUG] Kommentoldal fingerprint: {page_fingerprint}")

        if previous_page_fingerprint is not None and page_fingerprint == previous_page_fingerprint:
            print("[INFO] A mostani kommentoldal megegyezik az előzővel, megállok.")
            break
        if page_fingerprint in seen_page_fingerprints:
            print("[INFO] Már látott kommentoldal tartalom jött vissza, megállok.")
            break
        seen_page_fingerprints.add(page_fingerprint)

        added_on_page = 0
        for comment in page_comments:
            item = comment_to_output_item(comment)
            append_comment_to_stream_file(topic_file, item, has_existing_comments)
            has_existing_comments = True
            total_downloaded += 1
            added_on_page += 1
            print(
                f"[SAVE] {topic.topic_title} | comment_id={comment.get('comment_id')} | szerző={comment.get('author')}"
            )

        print(
            f"[INFO] Oldal kész: új hozzászólások={added_on_page} | összes letöltött={total_downloaded}"
        )

        if not next_url:
            print("[INFO] Nincs több kommentoldal ennél a témánál.")
            break

        next_page_no = get_page_no_from_url(next_url)
        if total_pages and current_page_no >= total_pages:
            print("[INFO] Elértük a téma utolsó oldalát.")
            break
        if next_page_no <= current_page_no:
            print("[INFO] A következő kommentoldal száma nem nagyobb, megállok.")
            break

        print(f"[INFO] Következő kommentoldal: {next_page_no}" + (f" / {total_pages}" if total_pages else ""))

        page_hops += 1
        if topic_reset_interval > 0 and page_hops % topic_reset_interval == 0:
            print("[INFO] Hosszú téma közbeni context reset.")
            fetcher.reset_context()

        fallback_url = build_topic_page_url(topic.topic_url, next_page_no)
        try:
            current_url, html = fetcher.fetch(next_url, wait_ms=int(delay * 1000))
        except Exception:
            print(f"[WARN] DOM lapozás nem sikerült, fallback URL: {fallback_url}")
            current_url, html = fetcher.fetch(fallback_url, wait_ms=int(delay * 1000))

        previous_page_fingerprint = page_fingerprint
        del page_comments
        gc.collect()

    finalize_stream_json(topic_file)
    print(f"[INFO] Topic JSON lezárva: {topic_file}")
    return total_downloaded


def scrape_category(
    fetcher: BrowserFetcher,
    category: CategoryRef,
    data_dir: Path,
    visited_topics_file: Path,
    delay: float,
    topic_reset_interval: int,
    only_category: Optional[str] = None,
    only_topic: Optional[str] = None,
) -> None:
    if only_category and only_category.lower() not in category.category_title.lower():
        print(f"[INFO] Kategória szűrés miatt kihagyva: {category.category_title}")
        return

    visited_topics = load_visited_set(visited_topics_file)
    current_url = category.category_url

    fetcher.reset_context()

    while True:
        final_url, html = fetcher.fetch(current_url, wait_ms=int(delay * 1000))
        category_title = extract_category_title(html, category.category_title)
        category.category_title = category_title

        current_page_no, total_pages, next_url = parse_category_pagination_info(html, final_url)
        print(
            f"\n[INFO] Témacsoport: {category.section_title} / {category.category_title} | oldal: {current_page_no}"
            + (f" / {total_pages}" if total_pages else "")
        )

        topics = parse_topics_from_category_page(html, final_url, category.section_title, category.category_title)
        print(f"[INFO] Talált témák ezen az oldalon (csak 'A fórum témái' rész): {len(topics)}")

        if not topics:
            print("[INFO] Nem találtam témákat ezen a témacsoport oldalon.")
            break

        for idx, topic in enumerate(topics, start=1):
            print(f"\n[INFO] ({idx}/{len(topics)}) Témanév: {topic.topic_title}")

            if only_topic and only_topic.lower() not in topic.topic_title.lower():
                print("[INFO] Téma szűrés miatt kihagyva.")
                continue

            topic_url_norm = normalize_topic_url(topic.topic_url)
            if topic_url_norm in visited_topics:
                print("[INFO] Ez a téma már a visited_topics-ban van, kihagyom.")
                continue

            topic_file = topic_json_path(data_dir, topic.section_title, topic.category_title, topic.topic_title)
            try:
                total_downloaded = scrape_topic(
                    fetcher=fetcher,
                    topic=topic,
                    topic_file=topic_file,
                    delay=delay,
                    topic_reset_interval=topic_reset_interval,
                )
                append_visited(visited_topics_file, topic_url_norm)
                visited_topics.add(topic_url_norm)
                print(
                    f"[INFO] Téma kész: {topic.topic_title} | letöltött hozzászólások: {total_downloaded} | visited OK"
                )
            except Exception as e:
                print(f"[WARN] Téma feldolgozási hiba: {topic.topic_title} | {e}")

            fetcher.reset_context()
            gc.collect()

        if not next_url:
            print(f"[INFO] Nincs több oldal a témacsoportban: {category.category_title}")
            break

        next_page_no = get_page_no_from_url(next_url)
        if total_pages and current_page_no >= total_pages:
            print(f"[INFO] Elértük a témacsoport utolsó oldalát: {category.category_title}")
            break
        if next_page_no <= current_page_no:
            print(f"[INFO] A témacsoport következő oldalszáma hibás, megállok: {category.category_title}")
            break

        fallback_url = build_category_page_url(category.category_url, next_page_no)
        print(f"[INFO] Következő témacsoport oldal: {next_page_no}" + (f" / {total_pages}" if total_pages else ""))

        try:
            current_url = next_url
        except Exception:
            current_url = fallback_url

        del topics
        gc.collect()


def scrape_all(
    fetcher: BrowserFetcher,
    output_dir: str,
    delay: float,
    only_section: Optional[str],
    only_category: Optional[str],
    only_topic: Optional[str],
    topic_reset_interval: int,
) -> None:
    output_root = Path(output_dir).expanduser().resolve()
    base_dir, data_dir, state_dir = ensure_dirs(output_root)
    visited_topics_file = state_dir / "visited_topics.txt"
    visited_categories_file = state_dir / "visited_categories.txt"

    fetcher.reset_context()
    main_url, main_html = fetcher.fetch(FORUM_URL, wait_ms=int(delay * 1000))

    categories = parse_main_sections(main_html, main_url)
    print(f"[INFO] Főoldalon talált cél témacsoportok száma: {len(categories)}")

    if only_section:
        categories = [c for c in categories if only_section.lower() in c.section_title.lower()]

    visited_categories = load_visited_set(visited_categories_file)

    for idx, category in enumerate(categories, start=1):
        category_key = normalize_category_url(category.category_url)
        print(
            f"\n[INFO] ({idx}/{len(categories)}) Következő témacsoport: {category.section_title} / {category.category_title}"
        )

        if only_category and only_category.lower() not in category.category_title.lower():
            print("[INFO] Kategória szűrés miatt kihagyva.")
            continue

        if category_key in visited_categories:
            print("[INFO] Ez a témacsoport már végig lett futtatva, kihagyom.")
            continue

        try:
            scrape_category(
                fetcher=fetcher,
                category=category,
                data_dir=data_dir,
                visited_topics_file=visited_topics_file,
                delay=delay,
                topic_reset_interval=topic_reset_interval,
                only_category=only_category,
                only_topic=only_topic,
            )
            append_visited(visited_categories_file, category_key)
            visited_categories.add(category_key)
            print(f"[INFO] Témacsoport kész, visited_categories OK: {category.category_title}")
        except Exception as e:
            print(f"[WARN] Témacsoport feldolgozási hiba: {category.category_title} | {e}")

        fetcher.reset_context()
        # A kérésed szerint témánként mindig vissza tud indulni a fő fórum oldalról.
        try:
            fetcher.fetch(FORUM_URL, wait_ms=int(delay * 1000))
        except Exception:
            pass
        gc.collect()

    print(f"\n[INFO] Minden kész. Kimeneti mappa: {base_dir}")


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SG.hu fórum scraper témacsoportokra bontva, streamelt JSON mentéssel")
    parser.add_argument("--output", default=".", help="Kimeneti alapmappa")
    parser.add_argument("--delay", type=float, default=1.5, help="Várakozás oldalak között másodpercben")
    parser.add_argument("--headed", action="store_true", help="Látható böngészőablakkal fusson")
    parser.add_argument("--timeout-ms", type=int, default=90000, help="Navigációs timeout ms")
    parser.add_argument("--retries", type=int, default=4, help="Fetch újrapróbálások száma")
    parser.add_argument("--auto-reset-fetches", type=int, default=120, help="Ennyi fetch után context reset")
    parser.add_argument("--topic-reset-interval", type=int, default=25, help="Ennyi kommentoldalanként context reset")
    parser.add_argument("--only-section", default=None, help="Csak ilyen fő szekciót dolgozzon fel")
    parser.add_argument("--only-category", default=None, help="Csak ilyen témacsoportot dolgozzon fel")
    parser.add_argument("--only-topic", default=None, help="Csak ilyen témát dolgozzon fel")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        with BrowserFetcher(
            headless=not args.headed,
            slow_mo=50 if args.headed else 0,
            timeout_ms=args.timeout_ms,
            retries=args.retries,
            auto_reset_fetches=args.auto_reset_fetches,
            block_resources=True,
        ) as fetcher:
            scrape_all(
                fetcher=fetcher,
                output_dir=args.output,
                delay=args.delay,
                only_section=args.only_section,
                only_category=args.only_category,
                only_topic=args.only_topic,
                topic_reset_interval=args.topic_reset_interval,
            )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva felhasználó által.")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] Végzetes hiba: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


# python sg_forum_scraper.py --output ./SG --headed
# python sg_forum_scraper.py --output ./SG --headed --only-category "Általános eszmecsere"
# python sg_forum_scraper.py --output ./SG --headed --only-topic "Garfield képregény"