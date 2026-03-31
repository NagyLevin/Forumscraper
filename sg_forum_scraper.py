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
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://sg.hu"
FORUM_URL = "https://sg.hu/forum"

ALLOWED_SECTION_TITLES = {
    "Általános fórumok",
    "Számítástechnikai fórumok",
    "Hírek fórumai",
}

SKIP_SECTION_TITLES = {
    "Cikkeink",
    "Legújabb témák",
    "Legutóbbi hozzászólások",
}

CATEGORY_PATH_RE = re.compile(r"^/forum/temak/\d+(?:\?.*)?$", re.IGNORECASE)
TOPIC_PATH_RE = re.compile(r"^/forum/tema/\d+(?:\?.*)?$", re.IGNORECASE)
MSG_ID_RE = re.compile(r"^msg-(\d+)$", re.IGNORECASE)


# --------------------------------------------------
# Segédfüggvények
# --------------------------------------------------

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_ws_inline(text: str) -> str:
    return clean_text(text).replace("\n", " ").strip()


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
            parsed.fragment,
        )
    )


def remove_query_param(url: str, key: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    if key in query:
        del query[key]
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query, doseq=True),
            parsed.fragment,
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


def short_preview(text: str, max_len: int = 120) -> str:
    txt = normalize_ws_inline(text)
    if len(txt) <= max_len:
        return txt
    return txt[: max_len - 3].rstrip() + "..."


def normalize_topic_url_for_visited(url: str) -> str:
    url = strip_fragment(url)
    return remove_query_param(url, "page")


def normalize_category_url_for_visited(url: str) -> str:
    url = strip_fragment(url)
    return remove_query_param(url, "page")


def get_page_no_from_url(url: str) -> int:
    page_val = extract_query_param(url, "page")
    if page_val and page_val.isdigit():
        return int(page_val)
    return 1


def build_topic_page_url(topic_url: str, page_no: int) -> str:
    base = normalize_topic_url_for_visited(topic_url)
    if page_no <= 1:
        return base
    return set_query_param(base, "page", str(page_no))


def build_category_page_url(category_url: str, page_no: int) -> str:
    base = normalize_category_url_for_visited(category_url)
    if page_no <= 1:
        return base
    return set_query_param(base, "page", str(page_no))


def stable_comment_signature(comment: Dict) -> str:
    comment_id = str(comment.get("comment_id") or "")
    author = clean_text(comment.get("author") or "")
    date = clean_text(comment.get("date") or "")
    idx = str(comment.get("index") or "")
    text = clean_text(comment.get("data") or "")[:300]
    return f"{comment_id}|{author}|{date}|{idx}|{text}"


def cleanup_topic_title_for_filename(title: str) -> str:
    title = clean_text(title)
    if not title:
        return "ismeretlen"

    patterns = [
        r"\s+\d[\d .]*\s*db\b.*$",
        r"\s+(?:ma|tegnap|tegnapelőtt)\s*,?\s*\d{1,2}:\d{2}.*$",
        r"\s+\d{4}\.\s*\d{2}\.\s*\d{2}\.?\s*\d{1,2}:\d{2}.*$",
        r"\s+\d{4}\.\s*\d{2}\.\s*\d{2}\.?.*$",
        r"\s+-\s+\d[\d .]*\s*üzenet.*$",
    ]

    cleaned = title
    for pat in patterns:
        cleaned = re.sub(pat, "", cleaned, flags=re.I)

    meta_markers = [
        r"\s+\b(?:ma|tegnap|tegnapelőtt)\b",
        r"\s+\b\d[\d .]*\s*db\b",
        r"\s+\b\d[\d .]*\s*üzenet\b",
        r"\s+\b\d{4}\.\s*\d{2}\.\s*\d{2}\.?\b",
    ]
    cut_positions = []
    for marker in meta_markers:
        m = re.search(marker, cleaned, flags=re.I)
        if m:
            cut_positions.append(m.start())

    if cut_positions:
        cleaned = cleaned[: min(cut_positions)]

    cleaned = clean_text(cleaned)
    return cleaned or clean_text(title) or "ismeretlen"


def is_reasonable_topic_title(title: str) -> bool:
    title = clean_text(title)
    if not title:
        return False
    cleaned = cleanup_topic_title_for_filename(title)
    return len(cleaned) >= 2


# --------------------------------------------------
# Adatmodellek
# --------------------------------------------------

@dataclass
class CategoryInfo:
    section_title: str
    category_title: str
    category_url: str
    topics_count_text: Optional[str] = None
    last_message_text: Optional[str] = None


@dataclass
class TopicInfo:
    section_title: str
    category_title: str
    category_url: str
    topic_title: str
    topic_url: str
    last_user: Optional[str] = None
    message_count_text: Optional[str] = None
    last_message_text: Optional[str] = None


# --------------------------------------------------
# Állapot / output
# --------------------------------------------------

def ensure_dirs(base_output: Path) -> Dict[str, Path]:
    root = base_output / "sg_forum"
    data_dir = root / "data"
    state_dir = root / "state"

    data_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    visited_topics = state_dir / "visited_topics.txt"
    visited_categories = state_dir / "visited_categories.txt"

    if not visited_topics.exists():
        visited_topics.write_text("", encoding="utf-8")
    if not visited_categories.exists():
        visited_categories.write_text("", encoding="utf-8")

    return {
        "root": root,
        "data": data_dir,
        "state": state_dir,
        "visited_topics": visited_topics,
        "visited_categories": visited_categories,
    }


def load_visited(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def append_visited(path: Path, value: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(value.strip() + "\n")


def topic_file_path(data_dir: Path, topic: TopicInfo) -> Path:
    clean_topic_name = cleanup_topic_title_for_filename(topic.topic_title)
    return (
        data_dir
        / sanitize_filename(topic.section_title)
        / sanitize_filename(topic.category_title)
        / f"{sanitize_filename(clean_topic_name)}.json"
    )


def topic_file_path_by_parts(data_dir: Path, section_title: str, category_title: str, topic_title: str) -> Path:
    return (
        data_dir
        / sanitize_filename(section_title)
        / sanitize_filename(category_title)
        / f"{sanitize_filename(cleanup_topic_title_for_filename(topic_title))}.json"
    )


def ensure_parent_dir(file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)


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

    comment_ids = re.findall(r'"comment_id"\s*:\s*"([^"]+)"', tail)
    urls = re.findall(r'"url"\s*:\s*"([^"]+)"', tail)

    last_comment_id = comment_ids[-1] if comment_ids else None
    last_comment_url = urls[-1] if urls else None

    return last_comment_id, last_comment_url, existing_count


def write_topic_stream_header(topic_file: Path, topic: TopicInfo, topic_meta: Dict) -> None:
    header_obj = {
        "title": cleanup_topic_title_for_filename(topic.topic_title),
        "authors": [],
        "data": {
            "content": cleanup_topic_title_for_filename(topic.topic_title),
            "likes": None,
            "dislikes": None,
            "score": None,
            "rating": None,
            "date": None,
            "url": normalize_topic_url_for_visited(topic.topic_url),
            "language": "hu",
            "tags": [],
            "rights": "sg.hu fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "section_title": topic.section_title,
                "category_title": topic.category_title,
                "category_url": normalize_category_url_for_visited(topic.category_url),
                "detected_total_pages": topic_meta.get("detected_total_pages"),
                "detected_total_comments": topic_meta.get("detected_total_comments"),
            },
            "origin": "sg_forum",
        },
        "origin": "sg_forum",
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


# --------------------------------------------------
# Playwright wrapper
# --------------------------------------------------

class BrowserFetcher:
    def __init__(
        self,
        headless: bool = True,
        slow_mo: int = 0,
        timeout_ms: int = 90000,
        retries: int = 4,
        block_resources: bool = True,
        auto_reset_fetches: int = 120,
    ):
        self.headless = headless
        self.slow_mo = slow_mo
        self.timeout_ms = timeout_ms
        self.retries = retries
        self.block_resources = block_resources
        self.auto_reset_fetches = auto_reset_fetches

        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.fetch_counter = 0

    def __enter__(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            slow_mo=self.slow_mo,
        )
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
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 2200},
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

    def _try_click_selector(self, selector: str, timeout: int = 1200) -> bool:
        try:
            locator = self.page.locator(selector).first
            if locator.is_visible(timeout=timeout):
                locator.click(timeout=2500, force=True)
                self.page.wait_for_timeout(600)
                return True
        except Exception:
            pass
        return False

    def dismiss_overlays_if_present(self) -> bool:
        clicked_any = False

        close_selectors = [
            "button[aria-label='Close']",
            "button[aria-label='Bezárás']",
            "button[title='Bezárás']",
            "button[title='Close']",
            "button:has-text('×')",
            "button:has-text('✕')",
            "button:has-text('✖')",
            "button:has-text('x')",
            "button:has-text('X')",
            "[role='dialog'] button",
            "div[role='dialog'] button",
        ]

        reject_selectors = [
            "button:has-text('ÖSSZES ELUTASÍTÁSA')",
            "button:has-text('Összes elutasítása')",
            "button:has-text('ELUTASÍTÁS')",
            "button:has-text('Elutasítás')",
            "button:has-text('Reject all')",
            "button:has-text('REJECT ALL')",
        ]

        accept_selectors = [
            "button:has-text('ÖSSZES ELFOGADÁSA')",
            "button:has-text('Összes elfogadása')",
            "button:has-text('ELFOGADOM')",
            "button:has-text('Elfogadom')",
            "button:has-text('Accept all')",
            "button:has-text('ACCEPT ALL')",
        ]

        for selector in reject_selectors:
            if self._try_click_selector(selector):
                #print("[INFO] Overlay/cookie ablak bezárva: elutasítás gomb.")
                clicked_any = True
                break

        if not clicked_any:
            for selector in close_selectors:
                if self._try_click_selector(selector):
                    #print("[INFO] Overlay/cookie ablak bezárva: X/close gomb.")
                    clicked_any = True
                    break

        if not clicked_any:
            for selector in accept_selectors:
                if self._try_click_selector(selector):
                    #print("[INFO] Overlay/cookie ablak bezárva: elfogadás gomb.")
                    clicked_any = True
                    break

        if clicked_any:
            try:
                self.page.keyboard.press("Escape")
                self.page.wait_for_timeout(300)
            except Exception:
                pass

        return clicked_any

    def accept_cookies_if_present(self) -> None:
        candidates = [
            "button:has-text('Elfogadom')",
            "button:has-text('ELFOGADOM')",
            "button:has-text('Rendben')",
            "button:has-text('OK')",
            "text=Elfogadom",
            "text=ELFOGADOM",
            "button:has-text('ÖSSZES ELUTASÍTÁSA')",
            "button:has-text('ÖSSZES ELFOGADÁSA')",
        ]
        for selector in candidates:
            try:
                locator = self.page.locator(selector).first
                if locator.is_visible(timeout=1200):
                    locator.click(timeout=2500, force=True)
                    self.page.wait_for_timeout(1200)
                    return
            except Exception:
                pass

    def fetch(self, url: str, wait_ms: int = 1500) -> Tuple[str, str]:
        last_exc = None

        if self.auto_reset_fetches > 0 and self.fetch_counter > 0 and self.fetch_counter % self.auto_reset_fetches == 0:
            print("[INFO] Automatikus context reset a fetch számláló alapján.")
            self.reset_context()

        for attempt in range(1, self.retries + 1):
            try:
                self.ensure_page_alive()

                print(f"[DEBUG] LETÖLTVE ({attempt}/{self.retries}): {url}")
                self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                self.page.wait_for_timeout(wait_ms)

                self.dismiss_overlays_if_present()
                self.accept_cookies_if_present()

                try:
                    self.page.wait_for_load_state("networkidle", timeout=5000)
                except PlaywrightTimeoutError:
                    pass

                for _ in range(3):
                    changed = self.dismiss_overlays_if_present()
                    self.accept_cookies_if_present()
                    try:
                        self.page.keyboard.press("Escape")
                    except Exception:
                        pass
                    if not changed:
                        break
                    self.page.wait_for_timeout(600)

                final_url = self.page.url
                html = self.page.content()

                self.fetch_counter += 1
                return final_url, html

            except PlaywrightTimeoutError as e:
                last_exc = e
                print(f"[WARN] Timeout ({attempt}/{self.retries}) -> {url}")

            except Exception as e:
                last_exc = e
                print(f"[WARN] Fetch hiba ({attempt}/{self.retries}) -> {url} | {e}")

                msg = str(e).lower()
                if "target page, context or browser has been closed" in msg or "has been closed" in msg:
                    try:
                        print("[WARN] A page/context bezáródott, context újranyitása...")
                        self.reset_context()
                    except Exception:
                        pass

            if attempt < self.retries:
                backoff_ms = 3000 * attempt
                print(f"[WARN] Újrapróbálás {backoff_ms / 1000:.1f} mp múlva...")
                try:
                    self.page.wait_for_timeout(backoff_ms)
                except Exception:
                    pass
                try:
                    self.reset_page()
                except Exception:
                    try:
                        self.reset_context()
                    except Exception:
                        pass

        raise last_exc


# --------------------------------------------------
# Főoldal: témacsoportok
# --------------------------------------------------

def find_section_heading_nodes(soup: BeautifulSoup) -> List[Tag]:
    nodes = []
    for node in soup.select("h1, h2, h3"):
        txt = clean_text(node.get_text(" ", strip=True))
        if txt in ALLOWED_SECTION_TITLES or txt in SKIP_SECTION_TITLES:
            nodes.append(node)
    return nodes


def parse_categories_from_section_block(section_node: Tag, page_url: str) -> List[CategoryInfo]:
    section_title = clean_text(section_node.get_text(" ", strip=True))
    if section_title in SKIP_SECTION_TITLES or section_title not in ALLOWED_SECTION_TITLES:
        return []

    results: List[CategoryInfo] = []
    seen: Set[str] = set()

    parent = section_node.parent
    search_root = parent if parent else section_node

    anchors = search_root.select("a[href]")
    for a in anchors:
        href = (a.get("href") or "").strip()
        if not href or not CATEGORY_PATH_RE.search(href):
            continue

        category_title = clean_text(a.get_text(" ", strip=True))
        if not category_title:
            continue

        category_url = normalize_category_url_for_visited(urljoin(page_url, href))
        if category_url in seen:
            continue

        row = a.find_parent(["a", "div"])
        row_text = clean_text(row.get_text(" ", strip=True)) if row else ""

        topics_count_text = None
        last_message_text = None

        nums = re.findall(r"\d[\d .]*\s*db", row_text, flags=re.I)
        if nums:
            topics_count_text = nums[-1]

        date_like = re.findall(
            r"(?:ma|tegnap|tegnapelőtt|\d{4}\.\s*\d{2}\.\s*\d{2}\.?)\s*,?\s*\d{1,2}:\d{2}",
            row_text,
            flags=re.I,
        )
        if date_like:
            last_message_text = date_like[-1]

        seen.add(category_url)
        results.append(
            CategoryInfo(
                section_title=section_title,
                category_title=category_title,
                category_url=category_url,
                topics_count_text=topics_count_text,
                last_message_text=last_message_text,
            )
        )

    return results


def parse_categories_from_forum_main(html: str, page_url: str) -> List[CategoryInfo]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[CategoryInfo] = []
    seen: Set[str] = set()

    section_nodes = find_section_heading_nodes(soup)

    for section_node in section_nodes:
        section_title = clean_text(section_node.get_text(" ", strip=True))
        if section_title in SKIP_SECTION_TITLES:
            continue
        if section_title not in ALLOWED_SECTION_TITLES:
            continue

        block_results = parse_categories_from_section_block(section_node, page_url)
        for item in block_results:
            key = normalize_category_url_for_visited(item.category_url)
            if key in seen:
                continue
            seen.add(key)
            results.append(item)

    if not results:
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href or not CATEGORY_PATH_RE.search(href):
                continue

            category_title = clean_text(a.get_text(" ", strip=True))
            if not category_title:
                continue

            prev_section_title = None
            prev = a
            for _ in range(50):
                prev = prev.find_previous(["h1", "h2", "h3"])
                if not prev:
                    break
                candidate = clean_text(prev.get_text(" ", strip=True))
                if candidate in ALLOWED_SECTION_TITLES or candidate in SKIP_SECTION_TITLES:
                    prev_section_title = candidate
                    break

            if prev_section_title not in ALLOWED_SECTION_TITLES:
                continue

            category_url = normalize_category_url_for_visited(urljoin(page_url, href))
            if category_url in seen:
                continue

            row = a.find_parent(["a", "div"])
            row_text = clean_text(row.get_text(" ", strip=True)) if row else ""

            topics_count_text = None
            last_message_text = None

            nums = re.findall(r"\d[\d .]*\s*db", row_text, flags=re.I)
            if nums:
                topics_count_text = nums[-1]

            date_like = re.findall(
                r"(?:ma|tegnap|tegnapelőtt|\d{4}\.\s*\d{2}\.\s*\d{2}\.?)\s*,?\s*\d{1,2}:\d{2}",
                row_text,
                flags=re.I,
            )
            if date_like:
                last_message_text = date_like[-1]

            seen.add(category_url)
            results.append(
                CategoryInfo(
                    section_title=prev_section_title,
                    category_title=category_title,
                    category_url=category_url,
                    topics_count_text=topics_count_text,
                    last_message_text=last_message_text,
                )
            )

    del soup
    gc.collect()
    return results


# --------------------------------------------------
# Témacsoport oldal
# --------------------------------------------------

def find_topics_section_container(soup: BeautifulSoup) -> Optional[Tag]:
    headers = soup.select("h1, h2, h3")
    for h in headers:
        title = clean_text(h.get_text(" ", strip=True)).lower()
        if title == "a fórum témái":
            parent = h.parent
            while parent:
                if parent.name == "div":
                    return parent
                parent = parent.parent
    return None


def parse_category_title_from_page(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for sel in ["h1", "title"]:
        node = soup.select_one(sel)
        if node:
            txt = clean_text(node.get_text(" ", strip=True))
            if txt and txt.lower() != "sg.hu":
                del soup
                gc.collect()
                return txt
    del soup
    gc.collect()
    return fallback


def is_probably_bad_topic_anchor(a: Tag) -> bool:
    txt = clean_text(a.get_text(" ", strip=True))
    href = (a.get("href") or "").strip()

    if not txt:
        return True

    if txt in {"Előző", "Következő", "← Előző", "Következő →", "←", "→"}:
        return True

    if not TOPIC_PATH_RE.search(href):
        return True

    for parent in a.parents:
        if not isinstance(parent, Tag):
            continue

        if parent.name in {"header", "footer", "nav", "aside"}:
            return True

        cls = " ".join(parent.get("class", []))
        if any(bad in cls.lower() for bad in ["pagination", "cookie", "consent", "dialog", "modal"]):
            return True

    return False


def collect_fallback_topic_anchors(soup: BeautifulSoup) -> List[Tag]:
    results: List[Tag] = []
    seen = set()

    roots = []

    main_tag = soup.select_one("main")
    if main_tag:
        roots.append(main_tag)

    roots.extend(soup.select("div.flex-1, div.min-w-0"))

    if not roots:
        roots = [soup]

    for root in roots:
        for a in root.select("a[href]"):
            href = (a.get("href") or "").strip()
            txt = clean_text(a.get_text(" ", strip=True))

            if is_probably_bad_topic_anchor(a):
                continue

            key = (href, txt)
            if key in seen:
                continue
            seen.add(key)
            results.append(a)

    return results


def parse_topics_from_category_page(
    html: str,
    page_url: str,
    section_title: str,
    category_title: str,
    category_url: str,
) -> List[TopicInfo]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[TopicInfo] = []
    seen: Set[str] = set()

    topics_container = find_topics_section_container(soup)
    candidate_anchors: List[Tag] = []

    if topics_container is not None:
        candidate_anchors = [
            a for a in topics_container.select("a[href]")
            if (a.get("href") or "").strip() and TOPIC_PATH_RE.search((a.get("href") or "").strip())
        ]
        print(f"[DEBUG] Topic anchorok az 'A fórum témái' blokkból: {len(candidate_anchors)}")
    else:
        print("[WARN] Nem találtam 'A fórum témái' blokkot, fallback keresés indul az oldalon.")
        candidate_anchors = collect_fallback_topic_anchors(soup)
        print(f"[DEBUG] Topic anchorok fallback kereséssel: {len(candidate_anchors)}")

    for a in candidate_anchors:
        href = (a.get("href") or "").strip()
        if not href or not TOPIC_PATH_RE.search(href):
            continue

        topic_title_raw = clean_text(a.get_text(" ", strip=True))
        topic_title = cleanup_topic_title_for_filename(topic_title_raw)
        if not topic_title:
            continue

        topic_url = normalize_topic_url_for_visited(urljoin(page_url, href))
        if topic_url in seen:
            continue

        row = a.find_parent(["a", "div"])
        row_text = clean_text(row.get_text(" ", strip=True)) if row else ""

        last_user = None
        message_count_text = None
        last_message_text = None

        nums = re.findall(r"\d[\d .]*\s*db", row_text, flags=re.I)
        if nums:
            message_count_text = nums[0]

        date_like = re.findall(
            r"(?:ma|tegnap|tegnapelőtt|\d{4}\.\s*\d{2}\.\s*\d{2}\.?)\s*,?\s*\d{1,2}:\d{2}",
            row_text,
            flags=re.I,
        )
        if date_like:
            last_message_text = date_like[-1]

        if row:
            row_full = row.get_text("\n", strip=True)
            lines = [clean_text(x) for x in row_full.splitlines() if clean_text(x)]
            if len(lines) >= 2:
                for line in lines[1:]:
                    if line == topic_title_raw or line == topic_title:
                        continue
                    if "db" in line.lower():
                        continue
                    if re.search(r"\d{1,2}:\d{2}", line):
                        continue
                    if len(line) > 60:
                        continue
                    last_user = line
                    break

        seen.add(topic_url)
        results.append(
            TopicInfo(
                section_title=section_title,
                category_title=category_title,
                category_url=category_url,
                topic_title=topic_title,
                topic_url=topic_url,
                last_user=last_user,
                message_count_text=message_count_text,
                last_message_text=last_message_text,
            )
        )

    del soup
    gc.collect()
    return results


# --------------------------------------------------
# Lapozás
# --------------------------------------------------

def parse_pagination_info_generic(html: str, current_url: str) -> Tuple[int, Optional[int], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    current_page = get_page_no_from_url(current_url)
    total_pages = None
    next_url = None

    page_text = clean_text(soup.get_text("\n", strip=True))
    m = re.search(r"\b(?:Oldal\s*)?(\d+)\s*/\s*(\d+)\b", page_text, flags=re.I)
    if m:
        try:
            current_page = int(m.group(1))
            total_pages = int(m.group(2))
        except Exception:
            pass

    for a in soup.select("a[href]"):
        txt = clean_text(a.get_text(" ", strip=True))
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(current_url, href)
        page_no = get_page_no_from_url(full)

        if txt in {"Következő", "Következő →", "Következő->", "Következő >", "→", ">"} and page_no > current_page:
            next_url = full
            break

    if next_url is None and (total_pages is None or current_page < total_pages):
        next_url = set_query_param(strip_fragment(current_url), "page", str(current_page + 1))

    del soup
    gc.collect()
    return current_page, total_pages, next_url


def parse_category_pagination_info(html: str, current_url: str) -> Tuple[int, Optional[int], Optional[str]]:
    return parse_pagination_info_generic(html, current_url)


def parse_topic_pagination_info(html: str, current_url: str) -> Tuple[int, Optional[int], Optional[str]]:
    return parse_pagination_info_generic(html, current_url)


# --------------------------------------------------
# Témaoldal
# --------------------------------------------------

def parse_topic_title_from_page(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    for sel in ["h1", "title"]:
        node = soup.select_one(sel)
        if node:
            txt = clean_text(node.get_text(" ", strip=True))
            if txt and txt.lower() != "sg.hu":
                candidates.append(txt)

    del soup
    gc.collect()

    for cand in candidates:
        cleaned = cleanup_topic_title_for_filename(cand)
        if is_reasonable_topic_title(cleaned):
            return cleaned

    fallback_clean = cleanup_topic_title_for_filename(fallback)
    return fallback_clean or "ismeretlen"


def extract_topic_meta(html: str, topic_url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text("\n", strip=True))

    current_page, total_pages, _ = parse_topic_pagination_info(html, topic_url)

    detected_total_comments = None
    nums = re.findall(r"\b\d[\d .]*\s*üzenet\b", page_text, flags=re.I)
    if nums:
        detected_total_comments = nums[-1]

    del soup
    gc.collect()

    return {
        "fetched_page": current_page,
        "detected_total_pages": total_pages,
        "detected_total_comments": detected_total_comments,
    }


def find_message_blocks(soup: BeautifulSoup) -> List[Tag]:
    blocks: List[Tag] = []
    for div in soup.select("div[id]"):
        div_id = (div.get("id") or "").strip()
        if MSG_ID_RE.match(div_id):
            blocks.append(div)
    return blocks


def extract_comment_id(block: Tag) -> Optional[str]:
    div_id = (block.get("id") or "").strip()
    m = MSG_ID_RE.match(div_id)
    if m:
        return m.group(1)

    txt = clean_text(block.get_text(" ", strip=True))
    m2 = re.search(r"#?(\d{4,})", txt)
    if m2:
        return m2.group(1)
    return None


def extract_author_date_header(block: Tag) -> Tuple[str, Optional[str]]:
    text = block.get_text("\n", strip=True)
    lines = [clean_text(x) for x in text.splitlines() if clean_text(x)]
    if not lines:
        return "ismeretlen", None

    author = "ismeretlen"
    date_text = None

    for i, line in enumerate(lines[:8]):
        if re.search(r"(?:\d{4}\.\d{2}\.\d{2}\.?\s*\d{1,2}:\d{2})|(?:\d{4}\.\d{2}\.\d{2}\.)|(?:\d{1,2}:\d{2})", line):
            date_text = line
            if i > 0:
                prev = clean_text(lines[i - 1])
                if prev and len(prev) < 100:
                    author = prev
            break

    if author == "ismeretlen":
        for line in lines[:5]:
            if re.search(r"\büzenet\b", line, flags=re.I):
                continue
            if re.search(r"\d{1,2}:\d{2}", line):
                continue
            if len(line) <= 80:
                author = line
                break

    return author, date_text


def extract_message_text(block: Tag) -> str:
    candidates = [
        "div.prose",
        "div[class*='prose']",
        "div.max-w-none",
        "div.break-words",
    ]
    for sel in candidates:
        node = block.select_one(sel)
        if node:
            txt = clean_text(node.get_text("\n", strip=True))
            if txt:
                return txt

    full = block.get_text("\n", strip=True)
    lines = [clean_text(x) for x in full.splitlines() if clean_text(x)]
    if not lines:
        return ""

    body_lines = lines[2:] if len(lines) > 2 else lines
    return clean_text("\n".join(body_lines))


def parse_comments_from_topic_page(html: str, topic_page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    blocks = find_message_blocks(soup)
    results: List[Dict] = []

    print(f"[DEBUG] Talált üzenetblokkok száma: {len(blocks)}")

    for idx, block in enumerate(blocks, start=1):
        comment_id = extract_comment_id(block)
        author, date_text = extract_author_date_header(block)
        body = extract_message_text(block)

        if not body and not author and not date_text:
            continue

        comment_url = strip_fragment(topic_page_url)
        if comment_id:
            comment_url = f"{comment_url}#msg-{comment_id}"

        item = {
            "comment_id": comment_id,
            "author": author or "ismeretlen",
            "date": date_text,
            "rating": None,
            "parent_author": None,
            "index": None,
            "index_total": None,
            "is_offtopic": False,
            "url": comment_url,
            "data": body,
        }
        '''
        print(
            f"[DEBUG] Komment #{idx} | id={comment_id or '-'} | szerző={item['author']} | "
            f"dátum={date_text or '-'} | preview={short_preview(body, 100)}"
        )
        '''
        results.append(item)

    del soup
    gc.collect()
    return results


def topic_has_any_comment_blocks(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    has_any = bool(find_message_blocks(soup))
    del soup
    gc.collect()
    return has_any


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
        "tags": ["offtopic"] if c.get("is_offtopic") else [],
        "extra": {
            "comment_id": c.get("comment_id"),
            "parent_author": c.get("parent_author"),
            "index": c.get("index"),
            "index_total": c.get("index_total"),
            "is_offtopic": c.get("is_offtopic"),
        },
    }


# --------------------------------------------------
# Topic scrape
# --------------------------------------------------

def scrape_topic(
    fetcher: BrowserFetcher,
    data_dir: Path,
    topic: TopicInfo,
    delay: float,
    topic_reset_interval: int = 25,
) -> int:
    fetcher.reset_context()

    initial_url = normalize_topic_url_for_visited(topic.topic_url)
    print(f"[INFO] Téma megnyitása: {topic.topic_title} | URL: {initial_url}")
    current_url, html = fetcher.fetch(initial_url, wait_ms=int(delay * 1000))

    resolved_title = parse_topic_title_from_page(html, topic.topic_title)
    if is_reasonable_topic_title(resolved_title):
        topic.topic_title = cleanup_topic_title_for_filename(resolved_title)
    else:
        topic.topic_title = cleanup_topic_title_for_filename(topic.topic_title)

    topic_file = topic_file_path_by_parts(
        data_dir=data_dir,
        section_title=topic.section_title,
        category_title=topic.category_title,
        topic_title=topic.topic_title,
    )
    ensure_parent_dir(topic_file)

    existing_comments = 0
    resume_after_comment_id = None
    resume_url = None
    need_init_file = True

    if topic_file.exists():
        if is_stream_json_finalized(topic_file):
            print(f"[INFO] A topic fájl már lezárt JSON, késznek vesszük: {topic_file}")
            return count_existing_comments_in_stream_file(topic_file)

        last_comment_id, last_comment_url, existing_comments = get_last_written_comment_info(topic_file)

        if last_comment_url:
            resume_url = last_comment_url
            resume_after_comment_id = last_comment_id
            need_init_file = False
            print(
                f"[INFO] Meglévő topic fájl megtalálva: {topic_file}\n"
                f"[INFO] Folytatás az utolsó komment URL-jéről: {resume_url}\n"
                f"[INFO] Utolsó comment_id: {resume_after_comment_id} | meglévő kommentek: {existing_comments}"
            )

            try:
                current_url, html = fetcher.fetch(resume_url, wait_ms=int(delay * 1000))
            except Exception as e:
                print(f"[WARN] Resume URL megnyitása sikertelen ({e}), fallback a topic elejére.")
                current_url, html = fetcher.fetch(initial_url, wait_ms=int(delay * 1000))
        else:
            print("[INFO] Létező, de nem lezárt topic fájl van, viszont nincs benne resume URL. A topic elejéről folytatok.")
    else:
        print(f"[INFO] Új topic fájl lesz: {topic_file}")

    topic_meta = extract_topic_meta(html, current_url)

    if need_init_file:
        write_topic_stream_header(topic_file, topic, topic_meta)
        print(f"[INFO] Új topic JSON létrehozva: {topic_file}")

    total_downloaded = existing_comments
    has_existing_comments = existing_comments > 0
    first_page_after_resume = resume_after_comment_id is not None

    seen_page_fingerprints: Set[str] = set()
    previous_page_fingerprint: Optional[str] = None
    page_hops = 0

    while True:
        current_page_no, total_pages, next_url = parse_topic_pagination_info(html, current_url)

        

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
                    f"[INFO] Resume szűrés: {original_len} kommentből "
                    f"{len(filtered)} maradt az utolsó mentett comment_id után."
                )
                page_comments = filtered
            else:
                print(
                    "[INFO] Az utolsó mentett comment_id nem található a resume oldalon. "
                    "Az oldalt újként kezelem."
                )

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
                f"[SAVE] {topic.topic_title} | comment_id={comment.get('comment_id')} | "
                f"szerző={comment.get('author') or '-'} | "
                f"dátum={comment.get('date') or '-'} | "
                f"szöveg={short_preview(comment.get('data') or '', 160)}"
            )

        print(f"[INFO] Oldal kész: új hozzászólások={added_on_page} | összes letöltött={total_downloaded}")

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
        print(
            f"[INFO] Téma: {topic.topic_title} | kommentoldal: {current_page_no}"
            + (f" / {total_pages}" if total_pages else "")
            + f" | URL: {current_url}"
        )
        print(f"[INFO] Következő kommentoldal száma: {next_page_no}")
       
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

        if not topic_has_any_comment_blocks(html):
            print("[INFO] A következő oldal már nem tartalmaz hozzászólást, megállok.")
            break

        previous_page_fingerprint = page_fingerprint
        del page_comments
        gc.collect()

    finalize_stream_json(topic_file)
    print(f"[INFO] Topic JSON lezárva: {topic_file}")
    return total_downloaded


# --------------------------------------------------
# Témacsoport scrape
# --------------------------------------------------

def scrape_category(
    fetcher: BrowserFetcher,
    data_dir: Path,
    visited_topics_file: Path,
    visited_categories_file: Path,
    visited_topics: Set[str],
    visited_categories: Set[str],
    category: CategoryInfo,
    delay: float,
    only_topic: Optional[str],
    topic_reset_interval: int,
) -> None:
    category_key = normalize_category_url_for_visited(category.category_url)

    if category_key in visited_categories:
        print(f"[INFO] Témacsoport már visitedben van, kihagyva: {category.category_title}")
        return

    fetcher.reset_context()
    current_url = category.category_url

    print(
        f"\n[INFO] Témacsoport feldolgozása: {category.section_title} / {category.category_title} | URL: {category.category_url}"
    )

    while True:
        current_url, html = fetcher.fetch(current_url, wait_ms=int(delay * 1000))

        resolved_category_title = parse_category_title_from_page(html, category.category_title)
        if resolved_category_title:
            category.category_title = resolved_category_title

        current_page_no, total_pages, next_url = parse_category_pagination_info(html, current_url)

        print(
            f"[INFO] Témacsoport oldal: {category.category_title} | oldal: {current_page_no}"
            + (f" / {total_pages}" if total_pages else "")
            + f" | URL: {current_url}"
        )

        topics = parse_topics_from_category_page(
            html=html,
            page_url=current_url,
            section_title=category.section_title,
            category_title=category.category_title,
            category_url=category.category_url,
        )

        print(f"[INFO] Talált témák az oldalon: {len(topics)}")

        if not topics:
            print("[WARN] Nem találtam témákat ezen az oldalon.")
            if total_pages and current_page_no >= total_pages:
                break

        for idx, topic in enumerate(topics, start=1):
            topic_key = normalize_topic_url_for_visited(topic.topic_url)

            print(f"\n[INFO] ({idx}/{len(topics)}) Téma: {topic.topic_title}")

            if only_topic and only_topic.lower() not in topic.topic_title.lower():
                print("[INFO] only-topic szűrés miatt kihagyva.")
                continue

            if topic_key in visited_topics:
                print("[INFO] Már visitedben van, kihagyva.")
                continue

            try:
                total_downloaded = scrape_topic(
                    fetcher=fetcher,
                    data_dir=data_dir,
                    topic=topic,
                    delay=delay,
                    topic_reset_interval=topic_reset_interval,
                )

                print(f"[INFO] Téma kész: {topic.topic_title} | letöltött hozzászólások: {total_downloaded}")
                print(
                    f"[INFO] Témacsoport oldal: {category.category_title} | oldal: {current_page_no}"
                    + (f" / {total_pages}" if total_pages else "")
                    + f" | URL: {current_url}"
                )
                print(f"\n[INFO] ({idx}/{len(topics)}) Téma: {topic.topic_title}")

                append_visited(visited_topics_file, topic_key)
                visited_topics.add(topic_key)

                print(f"[INFO] Topic visitedbe írva: {topic_key}")

            except Exception as e:
                print(f"[WARN] Hiba a téma feldolgozása közben: {topic.topic_url} | {e}")

            fetcher.reset_context()
            gc.collect()

        if total_pages and current_page_no >= total_pages:
            print("[INFO] Elértük a témacsoport utolsó oldalát.")
            break

        if not next_url:
            print("[INFO] Nincs következő témacsoport oldal.")
            break

        next_page_no = get_page_no_from_url(next_url)
        if next_page_no <= current_page_no:
            print("[INFO] A következő témacsoport oldal száma nem nagyobb, megállok.")
            break

        print(f"[INFO] Következő témacsoport oldal száma: {next_page_no}")

        fallback_url = build_category_page_url(category.category_url, next_page_no)
        current_url = fallback_url

    append_visited(visited_categories_file, category_key)
    visited_categories.add(category_key)
    print(f"[INFO] Témacsoport visitedbe írva: {category_key}")


# --------------------------------------------------
# Fő vezérlés
# --------------------------------------------------

def scrape_forum(
    fetcher: BrowserFetcher,
    output_dir: str,
    delay: float,
    only_section: Optional[str],
    only_category: Optional[str],
    only_topic: Optional[str],
    topic_reset_interval: int,
) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    paths = ensure_dirs(base_output)

    data_dir = paths["data"]
    visited_topics_file = paths["visited_topics"]
    visited_categories_file = paths["visited_categories"]

    visited_topics = {normalize_topic_url_for_visited(x) for x in load_visited(visited_topics_file)}
    visited_categories = {normalize_category_url_for_visited(x) for x in load_visited(visited_categories_file)}

    fetcher.reset_context()

    print(f"[INFO] Fő fórum megnyitása: {FORUM_URL}")
    final_url, html = fetcher.fetch(FORUM_URL, wait_ms=int(delay * 1000))

    categories = parse_categories_from_forum_main(html, final_url)
    print(f"[INFO] Talált feldolgozandó témacsoportok: {len(categories)}")

    if not categories:
        print("[WARN] Nem találtam feldolgozható témacsoportokat.")
        return

    for idx, category in enumerate(categories, start=1):
        print(f"\n[INFO] ({idx}/{len(categories)}) Témacsoport: {category.section_title} / {category.category_title}")

        if only_section and only_section.lower() not in category.section_title.lower():
            print("[INFO] only-section szűrés miatt kihagyva.")
            continue

        if only_category and only_category.lower() not in category.category_title.lower():
            print("[INFO] only-category szűrés miatt kihagyva.")
            continue

        scrape_category(
            fetcher=fetcher,
            data_dir=data_dir,
            visited_topics_file=visited_topics_file,
            visited_categories_file=visited_categories_file,
            visited_topics=visited_topics,
            visited_categories=visited_categories,
            category=category,
            delay=delay,
            only_topic=only_topic,
            topic_reset_interval=topic_reset_interval,
        )

        fetcher.reset_context()
        gc.collect()

    print("[INFO] Minden feldolgozható témacsoport végigment.")


# --------------------------------------------------
# CLI
# --------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="sg.hu fórum scraper Playwright + BeautifulSoup alapon, streamelt JSON-append módban"
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre az sg_forum/ mappa.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Várakozás oldalak között másodpercben.",
    )
    parser.add_argument(
        "--only-section",
        default=None,
        help="Csak azokat a fő szekciókat dolgozza fel, amelyek címében ez szerepel.",
    )
    parser.add_argument(
        "--only-category",
        default=None,
        help="Csak azokat a témacsoportokat dolgozza fel, amelyek címében ez szerepel.",
    )
    parser.add_argument(
        "--only-topic",
        default=None,
        help="Csak azokat a témákat dolgozza fel, amelyek címében ez szerepel.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Látható böngészőablakkal fusson.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=90000,
        help="Navigációs timeout ezredmásodpercben.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="Ennyiszer próbálja újra a fetch műveleteket.",
    )
    parser.add_argument(
        "--topic-reset-interval",
        type=int,
        default=25,
        help="Ennyi kommentoldalanként teljes context reset hosszú témáknál.",
    )
    parser.add_argument(
        "--auto-reset-fetches",
        type=int,
        default=120,
        help="Ennyi fetch után automatikus context reset.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        with BrowserFetcher(
            headless=not args.headed,
            slow_mo=50 if args.headed else 0,
            timeout_ms=args.timeout_ms,
            retries=args.retries,
            block_resources=True,
            auto_reset_fetches=args.auto_reset_fetches,
        ) as fetcher:
            scrape_forum(
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