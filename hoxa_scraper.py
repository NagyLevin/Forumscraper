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
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


BASE_URL = "https://www.hoxa.hu"
MAIN_FORUM_URL = f"{BASE_URL}/forumok"

MAIN_PAGE_RE = re.compile(r"(?:^|/)forumok(?:-oldal-(\d+))?(?:[/?#].*)?$", re.I)
TOPIC_RE = re.compile(r"^https?://(?:www\.)?hoxa\.hu/[^/?#]+-forum(?:-oldal-(\d+))?(?:[?#].*)?$", re.I)
TOPIC_PATH_RE = re.compile(r"^/[^/?#]+-forum(?:-oldal-(\d+))?(?:[?#].*)?$", re.I)
TOPIC_BASE_PATH_RE = re.compile(r"^(?P<base>/[^/?#]+-forum)(?:-oldal-(?P<page>\d+))?$", re.I)

COMMENT_ID_RE = re.compile(r'"comment_id"\s*:\s*(?:"([^"]+)"|(\d+)|null)')
COMMENT_URL_RE = re.compile(r'"url"\s*:\s*"([^"]+)"')


# -----------------------------
# Kivételek
# -----------------------------

class CaptchaDetectedError(RuntimeError):
    pass


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


def normalize_hoxa_url(url: str) -> str:
    parsed = urlparse(strip_fragment(url))
    path = re.sub(r"/+", "/", parsed.path.rstrip("/"))
    return urlunparse((parsed.scheme or "https", parsed.netloc or "www.hoxa.hu", path, "", "", ""))


def get_topic_page_number(url: str) -> int:
    path = urlparse(strip_fragment(url)).path.rstrip("/")
    m = TOPIC_BASE_PATH_RE.match(path)
    if not m:
        return 1
    page = m.group("page")
    if page and page.isdigit():
        return int(page)
    return 1


def get_topic_base_url(url: str) -> str:
    parsed = urlparse(normalize_hoxa_url(url))
    path = parsed.path.rstrip("/")
    m = TOPIC_BASE_PATH_RE.match(path)
    if m:
        return urlunparse((parsed.scheme, parsed.netloc, m.group("base"), "", "", ""))
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def build_topic_page_url(topic_url: str, page_no: int) -> str:
    base = get_topic_base_url(topic_url)
    if page_no <= 1:
        return base
    parsed = urlparse(base)
    return urlunparse((parsed.scheme, parsed.netloc, f"{parsed.path}-oldal-{page_no}", "", "", ""))


def get_main_page_number(url: str) -> int:
    path = urlparse(strip_fragment(url)).path.rstrip("/")
    m = MAIN_PAGE_RE.search(path)
    if not m:
        return 1
    page = m.group(1)
    if page and page.isdigit():
        return int(page)
    return 1


def build_main_page_url(page_no: int) -> str:
    if page_no <= 1:
        return MAIN_FORUM_URL
    return f"{MAIN_FORUM_URL}-oldal-{page_no}"


def parse_comment_page_number_from_comment_url(url: str) -> int:
    return get_topic_page_number(url)


def page_looks_like_captcha(html: str, url: Optional[str] = None) -> bool:
    html_low = (html or "").lower()

    if not html_low.strip():
        return False

    direct_markers = [
        "<title>captcha",
        '"title": "captcha!"',
        ">captcha!<",
        "captcha!",
        "captcha",
        "cloudflare",
        "cf-challenge",
        "challenge-platform",
        "/cdn-cgi/challenge-platform/",
        "g-recaptcha",
        "h-captcha",
        "recaptcha",
        "verify you are human",
        "ellenőrizze, hogy ember",
        "igazolja, hogy nem robot",
        "nem vagy robot",
    ]
    if any(marker in html_low for marker in direct_markers):
        return True

    soup = BeautifulSoup(html, "html.parser")
    try:
        title_text = ""
        if soup.title:
            title_text = clean_text(soup.title.get_text(" ", strip=True)).lower()

        body_text = clean_text(soup.get_text(" ", strip=True)).lower()

        if "captcha" in title_text:
            return True

        text_markers = [
            "captcha",
            "nem vagy robot",
            "igazolja, hogy nem robot",
            "verify you are human",
            "are you human",
            "robot vagy",
            "cloudflare",
            "security check",
        ]
        if any(marker in body_text for marker in text_markers):
            return True

        if url:
            normalized_url = normalize_hoxa_url(url).lower()
            if "captcha" in normalized_url:
                return True

        return False
    finally:
        del soup
        gc.collect()


def ensure_not_captcha(html: str, url: Optional[str] = None) -> None:
    if page_looks_like_captcha(html, url):
        raise CaptchaDetectedError(f"CAPTCHA detected at: {url or 'unknown url'}")


# -----------------------------
# Fájl / output kezelés
# -----------------------------

def ensure_dirs(base_output: Path) -> Tuple[Path, Path, Path]:
    hoxa_dir = base_output / "hoxa"
    topics_dir = hoxa_dir / "topics"
    hoxa_dir.mkdir(parents=True, exist_ok=True)
    topics_dir.mkdir(parents=True, exist_ok=True)

    visited_file = hoxa_dir / "visited.txt"
    if not visited_file.exists():
        visited_file.write_text("", encoding="utf-8")

    return hoxa_dir, topics_dir, visited_file


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

    ids_raw = COMMENT_ID_RE.findall(tail)
    ids = []
    for a, b in ids_raw:
        val = a or b
        if val and val.lower() != "null":
            ids.append(val)

    urls = COMMENT_URL_RE.findall(tail)

    last_comment_id = ids[-1] if ids else None
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
            "date": None,
            "url": get_topic_base_url(topic_url),
            "language": "hu",
            "tags": [],
            "rights": "hoxa.hu fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "detected_total_comments": topic_meta.get("detected_total_comments"),
                "fetched_page": topic_meta.get("fetched_page"),
            },
            "origin": "hoxa_forum",
        },
        "origin": "hoxa_forum",
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
            args=[
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
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
        print("[INFO] Browser context teljesen újranyitva.")

    def _iter_frames(self):
        try:
            return self.page.frames
        except Exception:
            return [self.page]

    def _click_first_visible_in_frame(self, frame, selectors, timeout_ms: int = 1200) -> bool:
        for selector in selectors:
            try:
                locator = frame.locator(selector).first
                if locator.count() > 0 and locator.is_visible(timeout=timeout_ms):
                    try:
                        locator.scroll_into_view_if_needed(timeout=2000)
                    except Exception:
                        pass
                    try:
                        locator.click(timeout=3000)
                    except Exception:
                        locator.click(timeout=3000, force=True)
                    self.page.wait_for_timeout(1200)
                    return True
            except Exception:
                pass
        return False

    def _remove_cookie_overlays_with_js(self) -> None:
        js = r"""
        () => {
          const selectors = [
            '#qc-cmp2-container', '.qc-cmp2-container',
            '[aria-modal="true"]', '[role="dialog"]',
            '#didomi-host', '.didomi-popup-container',
            'div[class*="consent"]', 'div[id*="consent"]',
            'div[class*="cookie"]', 'div[id*="cookie"]',
            'iframe[src*="consent"]', 'iframe[title*="consent"]'
          ];
          for (const sel of selectors) {
            for (const el of document.querySelectorAll(sel)) {
              try { el.remove(); } catch (e) {}
            }
          }
          document.documentElement.style.overflow = 'auto';
          document.body.style.overflow = 'auto';
        }
        """
        try:
            self.page.evaluate(js)
        except Exception:
            pass

    def accept_cookies_if_present(self) -> None:
        primary_accept = [
            "button:has-text('ELFOGADOM')",
            "button:has-text('Elfogadom')",
            "input[type=button][value*='ELFOGADOM']",
            "input[type=submit][value*='ELFOGADOM']",
            "text=ELFOGADOM",
            "text=Elfogadom",
        ]
        secondary_open = [
            "button:has-text('TOVÁBBI LEHETŐSÉGEK')",
            "button:has-text('További lehetőségek')",
            "text=TOVÁBBI LEHETŐSÉGEK",
            "text=További lehetőségek",
            "button:has-text('További információ')",
            "text=További információ",
        ]
        secondary_accept = [
            "button:has-text('Elfogadom')",
            "button:has-text('ELFOGADOM')",
            "button:has-text('Egyetértek')",
            "button:has-text('Összes elfogadása')",
            "button:has-text('Accept')",
            "text=Elfogadom",
            "text=ELFOGADOM",
            "text=Egyetértek",
            "text=Összes elfogadása",
            "text=Accept",
        ]
        close_selectors = [
            "button[aria-label='Close']",
            "button[aria-label='Bezárás']",
            "button[title='Bezárás']",
            "button:has-text('×')",
            "button:has-text('✕')",
            "text=×",
            "text=✕",
        ]

        for _ in range(3):
            clicked = False
            for frame in self._iter_frames():
                if self._click_first_visible_in_frame(frame, primary_accept, timeout_ms=800):
                    clicked = True
                    break
            if clicked:
                continue

            for frame in self._iter_frames():
                if self._click_first_visible_in_frame(frame, secondary_open, timeout_ms=800):
                    clicked = True
                    break
            if clicked:
                for frame in self._iter_frames():
                    if self._click_first_visible_in_frame(frame, secondary_accept, timeout_ms=1200):
                        clicked = True
                        break
                continue

            for frame in self._iter_frames():
                if self._click_first_visible_in_frame(frame, close_selectors, timeout_ms=500):
                    clicked = True
                    break
            if not clicked:
                break

        self._remove_cookie_overlays_with_js()
        try:
            self.page.keyboard.press('Escape')
            self.page.wait_for_timeout(300)
        except Exception:
            pass

    def fetch(self, url: str, wait_ms: int = 1500) -> Tuple[str, str]:
        last_exc = None

        if self.auto_reset_fetches > 0 and self.fetch_counter > 0 and self.fetch_counter % self.auto_reset_fetches == 0:
            print("[INFO] Automatikus context-reset a fetch számláló alapján.")
            self.reset_context()

        for attempt in range(1, self.retries + 1):
            try:
                print(f"[DEBUG] LETÖLTVE ({attempt}/{self.retries}): {url}")
                self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                self.page.wait_for_timeout(wait_ms)
                self.accept_cookies_if_present()
                try:
                    self.page.wait_for_load_state("networkidle", timeout=5000)
                except PlaywrightTimeoutError:
                    pass
                self.accept_cookies_if_present()
                try:
                    self.page.wait_for_timeout(1200)
                except Exception:
                    pass
                final_url = self.page.url
                html = self.page.content()

                ensure_not_captcha(html, final_url)

                self.fetch_counter += 1
                return final_url, html
            except CaptchaDetectedError:
                raise
            except PlaywrightTimeoutError as e:
                last_exc = e
                print(f"[WARN] Timeout ({attempt}/{self.retries}) -> {url}")
            except Exception as e:
                last_exc = e
                print(f"[WARN] Fetch hiba ({attempt}/{self.retries}) -> {url} | {e}")

            if attempt < self.retries:
                backoff_ms = 3000 * attempt
                print(f"[WARN] Újrapróbálás {backoff_ms / 1000:.1f} mp múlva...")
                try:
                    self.page.wait_for_timeout(backoff_ms)
                except Exception:
                    pass
                try:
                    self.page.goto("about:blank", timeout=10000)
                except Exception:
                    pass
                try:
                    self.reset_page()
                except Exception:
                    pass

        raise last_exc


# -----------------------------
# Főoldali topiclista parsing
# -----------------------------

def topic_url_from_row(row: Tag, page_url: str) -> Optional[str]:
    href = None

    onclick = row.get("onclick", "") or ""
    m = re.search(r"window\.location\.href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
    if m:
        href = m.group(1)

    if not href:
        a = row.select_one("div.forumlista1 a[href]")
        if a:
            href = a.get("href")

    if not href:
        for a in row.select("a[href]"):
            candidate = (a.get("href") or "").strip()
            if candidate and TOPIC_PATH_RE.match(candidate):
                href = candidate
                break

    if not href:
        return None

    full = urljoin(page_url, href)
    full = normalize_hoxa_url(full)
    if not TOPIC_RE.match(full):
        return None
    return get_topic_base_url(full)


def page_looks_like_cookie_wall(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    try:
        text = clean_text(soup.get_text(" ", strip=True))
    finally:
        del soup
        gc.collect()

    markers = [
        "A(z) hoxa.hu a hozzájárulását kéri",
        "Adataid védelme fontos számunkra",
        "személyes adatainak következő célokra",
        "cookie-kat tárolunk",
    ]
    txt_low = text.lower()
    return any(m.lower() in txt_low for m in markers)


def page_has_topic_rows(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    try:
        return bool(soup.select("div.forumlista.lista.flex"))
    finally:
        del soup
        gc.collect()


def parse_topic_rows_from_main_page(html: str, page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    topics: List[Dict] = []
    seen = set()

    rows = soup.select("div.forumlista.lista.flex")
    print(f"[DEBUG] Főoldali topic sorok száma: {len(rows)}")

    for row in rows:
        title = clean_text(row.get("title", ""))
        title_a = row.select_one("div.forumlista1 a[href]")
        if not title and title_a:
            title = clean_text(title_a.get_text(" ", strip=True))
        if not title:
            continue

        topic_url = topic_url_from_row(row, page_url)
        if not topic_url:
            continue

        topic_url_norm = normalize_topic_url_for_visited(topic_url)
        if topic_url_norm in seen:
            continue
        seen.add(topic_url_norm)

        comment_count = None
        last_user = None
        last_message = None

        stat_node = row.select_one("div.forumlista2")
        if stat_node:
            comment_count = parse_int_from_text(stat_node.get_text(" ", strip=True))

        last_node = row.select_one("div.forumlista3")
        if last_node:
            last_text = clean_text(last_node.get_text(" ", strip=True))
            m_last = re.match(r"^(.*?)\s+((?:ma|tegnapelőtt|tegnap|\d{4}\.\d{2}\.\d{2}\.?|\d{4}-\d{2}-\d{2}).*)$", last_text, re.I)
            if m_last:
                last_user = clean_text(m_last.group(1))
                last_message = clean_text(m_last.group(2))
            else:
                last_message = last_text

        topics.append(
            {
                "title": title,
                "url": topic_url_norm,
                "comment_count": comment_count,
                "view_count": None,
                "last_message": last_message,
                "last_user": last_user,
            }
        )

    del rows
    del soup
    gc.collect()
    return topics


def get_main_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    current_page_no = get_main_page_number(current_url)

    for a in soup.select("div.oldalszamok a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = normalize_hoxa_url(urljoin(current_url, href))
        next_page_no = get_main_page_number(full)
        txt = clean_text(a.get_text(" ", strip=True))
        if next_page_no == current_page_no + 1:
            del soup
            gc.collect()
            return full
        if txt in {">", "›", "»"} and next_page_no > current_page_no:
            del soup
            gc.collect()
            return full

    del soup
    gc.collect()
    return build_main_page_url(current_page_no + 1)


# -----------------------------
# Topicoldal parsing
# -----------------------------

def extract_topic_title(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for selector in ["h1", "title"]:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            text = re.sub(r"\s*\(beszélgetés\)\s*$", "", text, flags=re.I)
            text = re.sub(r"\s*-\s*Hoxa.*$", "", text, flags=re.I)
            if text:
                del soup
                gc.collect()
                return text
    del soup
    gc.collect()
    return fallback


def extract_topic_meta(html: str, topic_url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text("\n", strip=True))
    page_count = get_topic_page_number(topic_url)

    total_comments = None
    max_comment_id = None
    for row in soup.select("div.forumhsz.lista.flex"):
        c = extract_comment_from_container(row, topic_url)
        if c and c.get("comment_id"):
            try:
                cid = int(str(c["comment_id"]).replace(".", ""))
                max_comment_id = max(max_comment_id or cid, cid)
            except Exception:
                pass

    if max_comment_id is not None:
        total_comments = max_comment_id
    else:
        nums = [parse_int_from_text(x) for x in re.findall(r"\b\d{1,3}(?:\.\d{3})*\b", page_text)]
        nums = [x for x in nums if x is not None]
        if nums:
            total_comments = max(nums)

    del soup
    gc.collect()
    return {
        "url": get_topic_base_url(topic_url),
        "detected_total_comments": total_comments,
        "fetched_page": page_count,
    }


def find_comment_containers(soup: BeautifulSoup) -> List[Tag]:
    containers = soup.select("div.forumhsz.lista.flex")
    if containers:
        return containers
    return soup.select("div[id^='hsz'][class*='forumhsz']")


def extract_comment_header_info(header: Tag) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    author = None
    date_text = None
    comment_id = None
    dom_id = None

    if header:
        spans = header.select("span")
        for sp in spans:
            txt = clean_text(sp.get_text(" ", strip=True))
            if txt:
                author = txt
                break

        first_text = clean_text(header.get_text(" ", strip=True))
        m_id = re.search(r"\b(\d{1,3}(?:\.\d{3})*)\.\b", first_text)
        if m_id:
            comment_id = str(parse_int_from_text(m_id.group(1)))

        date_candidates = header.select("div")
        for node in reversed(date_candidates):
            txt = clean_text(node.get_text(" ", strip=True))
            if txt and (re.search(r"\b\d{1,2}:\d{2}\b", txt) or re.search(r"\b(ma|tegnap|tegnapelőtt)\b", txt, re.I)):
                date_text = txt
                break

        if not author:
            parts = [clean_text(x.get_text(" ", strip=True)) for x in header.select("a, span, div")]
            for part in parts:
                if part and not re.search(r"\b\d{1,2}:\d{2}\b", part) and not re.fullmatch(r"\d{1,3}(?:\.\d{3})*\.?", part):
                    author = part
                    break

    parent_author = None
    if author:
        m_parent = re.search(r"\(válaszként erre:\s*\d+\.?\s*-\s*(.+?)\)$", author, re.I)
        if m_parent:
            author = clean_text(author.split("(")[0])
            parent_author = clean_text(m_parent.group(1))

    return author, date_text, comment_id, parent_author


def extract_comment_from_container(container: Tag, topic_page_url: str) -> Optional[Dict]:
    dom_id = container.get("id")
    dom_comment_id = None
    if dom_id:
        m_dom = re.search(r"hsz(\d+)", dom_id)
        if m_dom:
            dom_comment_id = m_dom.group(1)

    header = container.select_one("div.forumhsz1")
    body_node = container.select_one("div.forumhsz2")

    author, date_text, visible_comment_id, parent_author = extract_comment_header_info(header) if header else (None, None, None, None)

    comment_id = visible_comment_id or dom_comment_id
    author = author or "ismeretlen"
    body = clean_text(body_node.get_text("\n", strip=True)) if body_node else ""

    if not body:
        return None

    comment_url = strip_fragment(topic_page_url)
    if comment_id:
        comment_url = f"{comment_url}#comment-{comment_id}"

    return {
        "comment_id": str(comment_id) if comment_id is not None else None,
        "author": author,
        "date": date_text,
        "rating": None,
        "parent_author": parent_author,
        "index": parse_int_from_text(comment_id or ""),
        "index_total": None,
        "is_offtopic": False,
        "url": comment_url,
        "data": body,
        "dom_id": dom_id,
        "dom_comment_id": dom_comment_id,
    }


def parse_comments_from_topic_page(html: str, topic_page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    containers = find_comment_containers(soup)
    print(f"[DEBUG] Talált komment-container elemek száma: {len(containers)}")

    comments: List[Dict] = []
    seen_signatures = set()
    for idx, container in enumerate(containers, start=1):
        parsed = extract_comment_from_container(container, topic_page_url)
        if not parsed:
            continue

        sig = build_comment_signature(parsed)
        if sig in seen_signatures:
            continue
        seen_signatures.add(sig)

        preview = (parsed["data"] or "")[:100].replace("\n", " | ")
        print(
            f"[DEBUG] Komment #{idx} | id={parsed.get('comment_id') or '-'} "
            f"| szerző={parsed.get('author')} | dátum={parsed.get('date')} "
            f"| preview={preview}"
        )
        comments.append(parsed)

    del containers
    del soup
    gc.collect()
    return comments


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


def topic_has_any_comment_container(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    has_any = bool(find_comment_containers(soup))
    del soup
    gc.collect()
    return has_any


def get_topic_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    current_page_no = get_topic_page_number(current_url)
    current_base = get_topic_base_url(current_url)

    for a in soup.select("div.oldalszamok a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = normalize_hoxa_url(urljoin(current_url, href))
        if get_topic_base_url(full) != current_base:
            continue
        full_page_no = get_topic_page_number(full)
        txt = clean_text(a.get_text(" ", strip=True))
        if full_page_no == current_page_no + 1:
            del soup
            gc.collect()
            return full
        if txt in {">", "›", "»"} and full_page_no > current_page_no:
            del soup
            gc.collect()
            return full

    del soup
    gc.collect()
    return build_topic_page_url(current_base, current_page_no + 1)


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
        "rating": None,
        "date": c.get("date"),
        "url": c.get("url"),
        "language": "hu",
        "tags": [],
        "extra": {
            "comment_id": c.get("comment_id"),
            "dom_comment_id": c.get("dom_comment_id"),
            "dom_id": c.get("dom_id"),
            "parent_author": c.get("parent_author"),
            "index": c.get("index"),
            "index_total": c.get("index_total"),
            "is_offtopic": c.get("is_offtopic"),
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
    topic_reset_interval: int = 25,
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
                f"[INFO] Meglévő félkész topicfájl, folytatás ugyanerről az oldalról: "
                f"page={resume_page_no}, utolsó comment_id={resume_after_comment_id}, "
                f"meglévő kommentek={existing_comments}"
            )

    fetcher.reset_context()

    first_fetch_url = build_topic_page_url(topic_url, resume_page_no)
    print(f"[INFO] Topic megnyitása: {topic_title}")
    current_url, html = fetcher.fetch(first_fetch_url, wait_ms=int(delay * 1000))

    resolved_title = extract_topic_title(html, topic_title)
    topic_meta = extract_topic_meta(html, current_url)

    if need_init_file:
        write_topic_stream_header(topic_file, resolved_title, topic_meta, topic_url)
        print(f"[INFO] Új streamelt topicfájl létrehozva: {topic_file}")

    page_no = get_topic_page_number(current_url)
    total_downloaded = existing_comments
    has_existing_comments = existing_comments > 0

    seen_page_fingerprints: Set[str] = set()
    previous_page_fingerprint: Optional[str] = None
    first_page_after_resume = True
    page_hops = 0

    while True:
        print(f"[INFO] Kommentoldal #{page_no}: {current_url}")
        page_comments = parse_comments_from_topic_page(html, current_url)

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
                print(
                    "[INFO] Resume módban az utolsó mentett comment_id nem található ezen az oldalon, "
                    "ezért ezt az oldalt újként kezelem."
                )

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
            f"[INFO] Oldal hozzáfűzve a topicfájlhoz: {topic_file} | "
            f"új kommentek ezen az oldalon: {added_on_this_page} | "
            f"összes letöltött komment eddig: {total_downloaded}"
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

        page_hops += 1
        if topic_reset_interval > 0 and page_hops % topic_reset_interval == 0:
            print("[INFO] Hosszú topic közbeni memória-kímélő context reset.")
            fetcher.reset_context()

        try:
            current_url, html = fetcher.fetch(next_url, wait_ms=int(delay * 1000))
        except Exception as e:
            print(f"[WARN] Hiba a következő kommentoldal megnyitásakor: {e}")
            break

        if not topic_has_any_comment_container(html):
            print("[INFO] A következő oldal már nem tartalmaz kommenteket, megállok.")
            break

        previous_page_fingerprint = current_fingerprint
        page_no = get_topic_page_number(current_url)

        del page_comments
        gc.collect()

    finalize_stream_json(topic_file)
    print(f"[DEBUG] Topic letöltés kész: {resolved_title} | összes letöltött komment: {total_downloaded}")
    print(f"[INFO] Topic JSON lezárva: {topic_file}")

    gc.collect()
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
    end_page: Optional[int],
    max_pages: Optional[int],
    topic_reset_interval: int,
) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    hoxa_dir, topics_dir, visited_file = ensure_dirs(base_output)

    visited_topics = {
        normalize_topic_url_for_visited(x)
        for x in load_visited(visited_file)
    }

    current_url = build_main_page_url(start_page)
    page_no = start_page
    processed_main_pages = 0

    fetcher.reset_context()

    while True:
        if max_pages is not None and processed_main_pages >= max_pages:
            print("[INFO] Elértem a max-pages limitet.")
            break
        if end_page is not None and page_no > end_page:
            print("[INFO] Elértem az end-page limitet.")
            break

        print(f"\n[INFO] Főoldali topiclista oldal #{page_no}: {current_url}")
        final_url, html = fetcher.fetch(current_url, wait_ms=int(delay * 1000))

        if page_looks_like_cookie_wall(html) or not page_has_topic_rows(html):
            print("[INFO] A főoldal még cookie/consent réteget vagy hiányos tartalmat mutat, újratöltöm tiszta contextben.")
            try:
                fetcher.reset_context()
                final_url, html = fetcher.fetch(current_url, wait_ms=max(int(delay * 1000), 2200))
            except Exception as e:
                print(f"[WARN] Consent fallback utáni újratöltés sem sikerült: {e}")

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
                    topic_reset_interval=topic_reset_interval,
                )

                print(f"[DEBUG] Végső komment darabszám a témához: {topic_title} | {total_downloaded}")

                append_visited(visited_file, topic_url_norm)
                visited_topics.add(topic_url_norm)

                print(f"[INFO] Topic mentve: {topic_json_path}")
                print(f"[INFO] Topic visitedbe írva: {topic_url_norm}")

            except CaptchaDetectedError:
                raise
            except Exception as e:
                print(f"[WARN] Hiba topic feldolgozás közben: {topic_url} | {e}")

            fetcher.reset_context()
            gc.collect()

        processed_main_pages += 1

        next_url = get_main_next_page_url(html, final_url)
        if not next_url:
            print("[INFO] Nincs több főoldali topiclista oldal.")
            break

        next_page_no = get_main_page_number(next_url)
        if next_page_no <= page_no:
            print("[INFO] Nem léptethető tovább a főoldali lapozás.")
            break
        if end_page is not None and next_page_no > end_page:
            print("[INFO] A következő főoldali lap már túl lenne az end-page limiten.")
            break

        current_url = next_url
        page_no = next_page_no

        del topics
        gc.collect()


# -----------------------------
# CLI
# -----------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="hoxa.hu fórum scraper Playwright + BeautifulSoup alapon, streamelt komment-append módban"
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre a hoxa/ mappa.",
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
        "--end-page",
        type=int,
        default=None,
        help="A fórum főoldali lapozásának utolsó feldolgozandó oldala.",
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
        help="Ennyi kommentoldalanként teljes context reset hosszú topicoknál.",
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

    if args.end_page is not None and args.end_page < args.start_page:
        print("[FATAL] Az end-page nem lehet kisebb a start-page-nél.")
        sys.exit(1)

    try:
        with BrowserFetcher(
            headless=not args.headed,
            slow_mo=50 if args.headed else 0,
            timeout_ms=args.timeout_ms,
            retries=args.retries,
            block_resources=True,
            auto_reset_fetches=args.auto_reset_fetches,
        ) as fetcher:
            scrape_main(
                fetcher=fetcher,
                output_dir=args.output,
                delay=args.delay,
                only_title=args.only_title,
                start_page=args.start_page,
                end_page=args.end_page,
                max_pages=args.max_pages,
                topic_reset_interval=args.topic_reset_interval,
            )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva felhasználó által.")
        sys.exit(1)
    except CaptchaDetectedError:
        print("[FATAL] CAPTCHA detected, stopping.")
        sys.exit(2)
    except Exception as e:
        print(f"[FATAL] Végzetes hiba: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

# python3 hoxa_scraper.py --output ./hoxa
# python3 hoxa_scraper.py --output ./hoxa --start-page 1 --end-page 10 --max-pages 10 --headed
# python3 hoxa_scraper.py --output ./hoxa --only-title "segély" --headed
# python3 hoxa_scraper.py --output ./hoxa --start-page 3 --max-pages 5 --headed
# python3 hoxa_scraper.py --output ./hoxa --start-page 1 --end-page 10 --headed