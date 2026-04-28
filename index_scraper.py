
from __future__ import annotations

import argparse
import gc
import json
import os
import re
import sys
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
SHOW_ARTICLE_RE = re.compile(
    r"/Article/showArticle2?(?:\?|$)|/Article/showArticle(?:\?|$)",
    re.IGNORECASE,
)

URL_FIELD_RE = re.compile(r'"url"\s*:\s*"([^"]+)"')

ABSOLUTE_DATE_RE = re.compile(
    r"\b[0-9]{4}\.[0-9]{2}\.[0-9]{2}\.?(?:\s+[0-9]{1,2}:[0-9]{2})?\b",
    re.IGNORECASE,
)

RELATIVE_DATE_RE = re.compile(
    r"\b\d+\s+(?:perce|órája|napja|hete|hónapja|éve)\b",
    re.IGNORECASE,
)


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


def ensure_visited_file(index_dir: Path, filename: str) -> Path:
    visited = index_dir / filename
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


def append_visited(visited_file: Path, item: str) -> None:
    with visited_file.open("a", encoding="utf-8") as f:
        f.write(item.strip() + "\n")


def append_visited_if_missing(visited_file: Path, visited_set: Set[str], item: str) -> None:
    item = item.strip()
    if not item or item in visited_set:
        return
    append_visited(visited_file, item)
    visited_set.add(item)


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


def strip_fragment(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))


def normalize_topic_url_for_visited(url: str) -> str:
    return strip_fragment(url).rstrip("/")


def normalize_url_for_dedup(url: str) -> str:
    parsed = urlparse(strip_fragment(url))
    query = parse_qs(parsed.query)

    keep = {}
    for key in ("t", "a", "nt_start", "nt_step", "na_start", "na_step"):
        vals = query.get(key)
        if vals:
            keep[key] = vals

    normalized = urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip("/"),
            parsed.params,
            urlencode(keep, doseq=True),
            "",
        )
    )
    return normalized


def topic_file_path(subforum_dir: Path, topic_title: str) -> Path:
    return subforum_dir / f"{sanitize_filename(topic_title)}.json"


def read_tail_text(path: Path, max_bytes: int = 1024 * 1024) -> str:
    if not path.exists():
        return ""

    size = path.stat().st_size
    with path.open("rb") as f:
        if size > max_bytes:
            f.seek(size - max_bytes)
        data = f.read()

    return data.decode("utf-8", errors="ignore")


def file_looks_closed_json(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False

    tail = read_tail_text(path, max_bytes=65536).rstrip()
    if not tail.endswith("}"):
        return False

    required_tail_markers = [
        '"origin": "index_forum"',
        '"scrape_status": "finished"',
        '"date_modified":',
    ]
    return all(marker in tail for marker in required_tail_markers)


def file_has_any_written_comment(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False

    tail = read_tail_text(path, max_bytes=2 * 1024 * 1024)
    return '"comment_id"' in tail


def find_last_comment_url_from_file(path: Path) -> Optional[str]:
    tail = read_tail_text(path, max_bytes=2 * 1024 * 1024)

    comment_blocks = re.findall(
        r'"comment_id"\s*:\s*"[^"]+"\s*.*?"url"\s*:\s*"([^"]+)"',
        tail,
        flags=re.S,
    )
    if comment_blocks:
        return comment_blocks[-1]

    return None

def count_comments_in_file(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0

    count = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            count += line.count('"comment_id"')
    return count



def init_open_json_file_if_needed(
    topic_file: Path,
    resolved_title: str,
    topic_meta: Dict,
    topic_url: str,
) -> None:
    if topic_file.exists() and topic_file.stat().st_size > 0:
        return

    opener = topic_meta.get("opener") or ""

    header_obj = {
        "title": resolved_title,
        "authors": [split_name_like_person(opener)] if opener else [],
        "data": {
            "content": resolved_title,
            "likes": None,
            "dislikes": None,
            "score": None,
            "date": topic_meta.get("opened_date"),
            "url": topic_meta.get("url") or topic_url,
            "language": "hu",
            "tags": [],
            "rights": "Index Fórum tartalom",
            "extra": {
                "opener_username": opener,
                "post_count": topic_meta.get("post_count"),
                "commenter_count": topic_meta.get("commenter_count"),
            },
            "origin": "index_forum",
        },
    }

    with topic_file.open("w", encoding="utf-8") as f:
        f.write("{\n")
        f.write(f'  "title": {json.dumps(header_obj["title"], ensure_ascii=False)},\n')
        f.write(f'  "authors": {json.dumps(header_obj["authors"], ensure_ascii=False, indent=2)},\n')
        f.write(f'  "data": {json.dumps(header_obj["data"], ensure_ascii=False, indent=2)},\n')
        f.write('  "comments": [\n')
        f.flush()
        os.fsync(f.fileno())


def append_comments_page_to_open_json(
    topic_file: Path,
    comments: List[Dict],
    first_comment_already_written: bool,
) -> bool:
    if not comments:
        return first_comment_already_written

    with topic_file.open("a", encoding="utf-8") as f:
        for comment in comments:
            json_comment = {
                "authors": [split_name_like_person(comment.get("author") or "ismeretlen")],
                "data": comment.get("data"),
                "likes": comment.get("likes"),
                "dislikes": comment.get("dislikes"),
                "score": comment.get("score"),
                "date": comment.get("date"),
                "url": comment.get("url"),
                "language": "hu",
                "tags": [],
                "extra": {
                    "comment_id": comment.get("comment_id"),
                },
            }

            if first_comment_already_written:
                f.write(",\n")
            f.write("    ")
            f.write(json.dumps(json_comment, ensure_ascii=False, indent=4))
            first_comment_already_written = True

        f.flush()
        os.fsync(f.fileno())

    return first_comment_already_written


def close_topic_json_file(
    topic_file: Path,
    saved_comment_pages: int,
    resume_source: Optional[str],
    total_comments_downloaded: int,
) -> None:
    with topic_file.open("a", encoding="utf-8") as f:
        f.write("\n  ],\n")
        f.write('  "origin": "index_forum",\n')
        f.write('  "extra": {\n')
        f.write('    "scrape_status": "finished",\n')
        f.write(f'    "saved_comment_pages": {saved_comment_pages},\n')
        f.write(f'    "resume_source": {json.dumps(resume_source, ensure_ascii=False)},\n')
        f.write(f'    "total_comments_downloaded": {total_comments_downloaded},\n')
        f.write(f'    "date_modified": {json.dumps(now_iso(), ensure_ascii=False)}\n')
        f.write("  }\n")
        f.write("}\n")
        f.flush()
        os.fsync(f.fileno())


def derive_next_page_from_comment_url(comment_url: str) -> Optional[str]:
    if not comment_url:
        return None

    base_url = strip_fragment(comment_url)
    na_start = extract_query_param(base_url, "na_start")
    na_step = extract_query_param(base_url, "na_step")

    if not na_start or not na_step or not na_start.isdigit() or not na_step.isdigit():
        return None

    next_start = int(na_start) + int(na_step)
    resumed = set_query_param(base_url, "na_start", str(next_start))
    resumed = set_query_param(resumed, "na_step", str(int(na_step)))
    return strip_fragment(resumed)


def parse_votes_from_header_row(header_row: Optional[Tag]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    if not header_row:
        return None, None, None

    rat = header_row.select_one("span.art_rat")
    if not rat:
        return None, None, None

    left_span = rat.select_one("span.art_rat_lft")
    dislikes = parse_int_from_text(left_span.get_text(" ", strip=True)) if left_span else 0

    positive_anchor = rat.select_one("a.art_rat_pl-i")
    likes = 0

    if positive_anchor:
        next_text_parts = []
        for sib in positive_anchor.next_siblings:
            if isinstance(sib, str):
                txt = clean_text(sib)
                if txt:
                    next_text_parts.append(txt)
            elif isinstance(sib, Tag):
                if sib.name == "a":
                    continue
                txt = clean_text(sib.get_text(" ", strip=True))
                if txt:
                    next_text_parts.append(txt)

        if next_text_parts:
            likes = parse_int_from_text(" ".join(next_text_parts))

    if likes is None:
        likes = 0
    if dislikes is None:
        dislikes = 0

    score = likes - dislikes
    return likes, dislikes, score


def is_bad_resolved_topic_title(title: str) -> bool:
    title_norm = clean_text(title).strip().lower()

    if not title_norm:
        return True

    bad_exact = {
        "keresés",
        "kereses",
        "index fórum",
        "index forum",
        "forum",
        "fórum",
    }

    if title_norm in bad_exact:
        return True

    bad_contains = [
        "keresés",
        "kereses",
        "találat",
        "search",
    ]

    if any(x in title_norm for x in bad_contains):
        return True

    return False


def topic_page_looks_valid(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    try:
        if soup.select("table.art"):
            return True
        text = clean_text(soup.get_text(" ", strip=True)).lower()
        if "hozzászólás" in text and ("nyitotta:" in text or "hozzászólók:" in text):
            return True
        return False
    finally:
        del soup


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
        print("[INFO] Böngészőoldal újranyitva a stabilabb működéshez.")

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
        print("[INFO] Browser context teljesen újranyitva memória-kíméléshez.")

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
                if locator.is_visible(timeout=1200):
                    print(f"[DEBUG] Sütigomb megtalálva: {selector}")
                    locator.click(timeout=3000)
                    self.page.wait_for_timeout(1200)
                    return
            except Exception:
                pass

    def fetch(self, url: str, wait_ms: int = 1200) -> Tuple[str, str]:
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
                    self.page.wait_for_load_state("networkidle", timeout=4000)
                except PlaywrightTimeoutError:
                    pass

                final_url = self.page.url
                html = self.page.content()

                self.fetch_counter += 1
                print(f"[DEBUG] Végső URL: {final_url}")
                return final_url, html

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
                    if attempt >= 2:
                        self.reset_context()
                    else:
                        self.reset_page()
                except Exception:
                    pass

        raise last_exc


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

        print(f"[DEBUG] Fórumcsoport #{idx}: {category_title} | kis linkek: {len(sublinks)}")

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




def extract_subforum_links_from_page(
    html: str,
    page_url: str,
    *,
    parent_url: Optional[str] = None,
) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[Dict] = []
    seen: Set[str] = set()

    parent_norm = normalize_url_for_dedup(parent_url or page_url)

    for a in soup.select("a[href]"):
        href = clean_text(a.get("href", ""))
        title = clean_text(a.get_text(" ", strip=True))
        if not href or not title:
            continue

        full_url = urljoin(page_url, href)
        if not SHOW_TOPIC_LIST_RE.search(full_url):
            continue

        norm_url = normalize_url_for_dedup(full_url)
        if norm_url == parent_norm:
            continue
        if norm_url in seen:
            continue

        lower_title = title.lower()
        if lower_title in {"előző", "következő", "next", "prev", ">", ">>", "10>", "<<", "<"}:
            continue
        if re.fullmatch(r"\d+", title):
            continue

        seen.add(norm_url)
        results.append(
            {
                "title": title,
                "url": full_url,
            }
        )

    del soup
    return results


def page_looks_like_subforum_hub(html: str, page_url: str, *, parent_url: Optional[str] = None) -> bool:
    topic_count = len(parse_topic_rows_from_subforum_page(html, page_url))
    if topic_count > 0:
        return False

    nested = extract_subforum_links_from_page(html, page_url, parent_url=parent_url)
    return len(nested) > 0

def get_subforum_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    candidates = []
    possible = []

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
        del soup
        return candidates[0]

    current_nt_start = extract_query_param(current_url, "nt_start")
    current_t = extract_query_param(current_url, "t")
    if current_t is not None:
        current_start_int = int(current_nt_start) if current_nt_start and current_nt_start.isdigit() else 0

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

    del soup

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
        text = clean_text(text)

        if not text:
            continue

        if is_bad_resolved_topic_title(text):
            continue

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

    m_abs_date = ABSOLUTE_DATE_RE.search(header_text)
    if m_abs_date:
        date_text = clean_text(m_abs_date.group(0))
    else:
        m_rel_date = RELATIVE_DATE_RE.search(header_text)
        if m_rel_date:
            date_text = clean_text(m_rel_date.group(0))

    likes, dislikes, score = parse_votes_from_header_row(header_row)

    nr_span = header_row.select_one("span.art_nr") if header_row else None
    if nr_span:
        comment_id = clean_text(nr_span.get_text(" ", strip=True))
    else:
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
        comment_url = strip_fragment(topic_page_url) + f"#msg{comment_id}"

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
            f"| szerző={parsed['author']} | dátum={parsed.get('date') or '-'} "
            f"| like={parsed.get('likes')} | dislike={parsed.get('dislikes')} "
            f"| score={parsed.get('score')} | preview={preview}"
        )
        comments.append(parsed)

    del soup
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
        del soup
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

    del soup

    if possible:
        possible.sort(key=lambda x: x[0])
        return possible[0][1]

    return None


def scrape_topic(
    fetcher: BrowserFetcher,
    topic_title: str,
    topic_url: str,
    topic_file: Path,
    delay: float,
    topic_reset_interval: int,
) -> Tuple[str, int, bool]:
    resume_source = None
    first_comment_already_written = False
    start_page_no = 1
    total_downloaded = 0

    if topic_file.exists() and topic_file.stat().st_size > 0:
        if file_looks_closed_json(topic_file):
            print("[INFO] A topic fájl már lezárt JSON-nak tűnik, kihagyva.")
            return topic_title, 0, True

        total_downloaded = count_comments_in_file(topic_file)

        last_comment_url = find_last_comment_url_from_file(topic_file)
        if last_comment_url:
            next_url = derive_next_page_from_comment_url(last_comment_url)
            if next_url:
                topic_url = next_url
                first_comment_already_written = True
                resume_source = "existing_json"
                print(
                    f"[INFO] Resume: utolsó komment URL alapján innen folytatva: {topic_url} | "
                    f"eddig letöltött kommentek: {total_downloaded}"
                )
        else:
            print(
                "[INFO] Van meglévő félkész fájl, de nem találtam benne komment URL-t. "
                f"Topic elejéről indul. Eddig letöltött kommentek: {total_downloaded}"
            )

    fetcher.reset_context()

    print(f"[INFO] Topic megnyitása: {topic_title}")
    current_url, html = fetcher.fetch(topic_url, wait_ms=int(delay * 1000))

    if not topic_page_looks_valid(html):
        print(f"[WARN] A letöltött oldal nem tűnik valódi topic-oldalnak: {current_url}")

    resolved_title = extract_topic_title(html, topic_title)
    if is_bad_resolved_topic_title(resolved_title):
        print(f"[WARN] Gyanús feloldott topic cím, fallback az eredeti címre: {resolved_title!r}")
        resolved_title = topic_title

    topic_meta = extract_topic_meta(html, topic_url)

    init_open_json_file_if_needed(topic_file, resolved_title, topic_meta, topic_url)

    if topic_file.stat().st_size > 0 and not first_comment_already_written:
        if file_has_any_written_comment(topic_file):
            first_comment_already_written = True
            if total_downloaded == 0:
                total_downloaded = count_comments_in_file(topic_file)

    page_no = start_page_no
    finished = False
    topic_page_hops = 0

    while True:
        print(f"[INFO] Kommentoldal #{page_no}: {current_url}")
        page_comments = parse_comments_from_topic_page(html, current_url)
        added_on_this_page = len(page_comments)

        if page_comments:
            first_comment_already_written = append_comments_page_to_open_json(
                topic_file=topic_file,
                comments=page_comments,
                first_comment_already_written=first_comment_already_written,
            )
            total_downloaded += added_on_this_page

        print(
            f"[INFO] Oldal appendelve a JSON végére: {topic_file} | "
            f"új kommentek: {added_on_this_page} | "
            f"összes letöltött komment eddig: {total_downloaded}"
        )

        next_url = get_topic_next_page_url(html, current_url)
        del page_comments
        del html
        gc.collect()

        if not next_url or next_url == current_url:
            print("[INFO] Nincs több kommentoldal ennél a topicnál.")
            finished = True
            break

        topic_page_hops += 1
        if topic_reset_interval > 0 and topic_page_hops % topic_reset_interval == 0:
            print("[INFO] Hosszú topic közbeni memória-kímélő context reset.")
            fetcher.reset_context()

        print(f"[INFO] Következő kommentoldal: {next_url}")
        current_url, html = fetcher.fetch(next_url, wait_ms=int(delay * 1000))
        page_no += 1

    if finished:
        print(f"[INFO] Topic összes letöltött komment: {total_downloaded}")
        close_topic_json_file(
            topic_file=topic_file,
            saved_comment_pages=page_no,
            resume_source=resume_source,
            total_comments_downloaded=total_downloaded,
        )
        really_closed = file_looks_closed_json(topic_file)
        print(f"[INFO] Topic végleg lezárva: {topic_file} | lezárt={really_closed}")
        return resolved_title, page_no, really_closed

    return resolved_title, page_no, False


def scrape_subforum(
    fetcher: BrowserFetcher,
    category_title: str,
    subforum_title: str,
    subforum_url: str,
    base_index_dir: Path,
    visited_file: Path,
    visited_topics: Set[str],
    delay: float,
    topic_reset_interval: int,
    subforum_reset_interval: int,
) -> bool:
    category_dir = base_index_dir / sanitize_filename(category_title)
    subforum_dir = category_dir / sanitize_filename(subforum_title)
    subforum_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[INFO] Alforum indul: {category_title} -> {subforum_title}")
    print(f"[INFO] Alforum URL: {subforum_url}")

    fetcher.reset_context()

    current_url = subforum_url
    page_no = 1
    all_topics_finished = True

    while True:
        if subforum_reset_interval > 0 and page_no > 1 and (page_no - 1) % subforum_reset_interval == 0:
            print("[INFO] Alforum oldalak közbeni memória-kímélő context reset.")
            fetcher.reset_context()

        print(f"\n[INFO] Topiclista oldal #{page_no}: {current_url}")
        final_url, html = fetcher.fetch(current_url, wait_ms=int(delay * 1000))

        resolved_subforum_title = parse_subforum_title(html)
        print(f"[DEBUG] Felismert alforum cím: {resolved_subforum_title}")

        topics = parse_topic_rows_from_subforum_page(html, final_url)
        print(f"[INFO] Talált topicok ezen az oldalon: {len(topics)}")

        for idx, topic in enumerate(topics, start=1):
            topic_title = topic["title"]
            topic_url = topic["url"]
            topic_url_norm = normalize_url_for_dedup(topic_url)

            print(f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title}")

            initial_path = topic_file_path(subforum_dir, topic_title)

            if topic_url_norm in visited_topics:
                print("[INFO] Már visitedben van, kihagyva.")
                continue

            if initial_path.exists() and file_looks_closed_json(initial_path):
                print("[INFO] A JSON fájl már lezárt, visitedbe írom és kihagyom.")
                append_visited(visited_file, topic_url_norm)
                visited_topics.add(topic_url_norm)
                continue

            try:
                resolved_title, saved_pages, fully_finished = scrape_topic(
                    fetcher=fetcher,
                    topic_title=topic_title,
                    topic_url=topic_url,
                    topic_file=initial_path,
                    delay=delay,
                    topic_reset_interval=topic_reset_interval,
                )

                safe_final_title = resolved_title
                if is_bad_resolved_topic_title(safe_final_title):
                    print(f"[WARN] Hibásnak tűnő végső cím miatt nincs átnevezés: {safe_final_title!r}")
                    safe_final_title = topic_title

                final_path = topic_file_path(subforum_dir, safe_final_title)

                if final_path != initial_path and initial_path.exists():
                    initial_path.replace(final_path)
                else:
                    final_path = initial_path

                if fully_finished and file_looks_closed_json(final_path):
                    append_visited(visited_file, topic_url_norm)
                    visited_topics.add(topic_url_norm)
                    print(f"[INFO] Topic mentve: {final_path}")
                    print(f"[INFO] Mentett kommentoldalak: {saved_pages}")
                    print(f"[INFO] Topic visitedbe írva: {topic_url_norm}")
                else:
                    all_topics_finished = False
                    print(f"[INFO] Topic NINCS teljesen kész, ezért NEM kerül visitedbe: {topic_url_norm}")

            except Exception as e:
                all_topics_finished = False
                print(f"[WARN] Hiba topic feldolgozás közben: {topic_url} | {e}")

        next_url = get_subforum_next_page_url(html, final_url)
        del html
        gc.collect()

        if not next_url or next_url == final_url:
            print(f"[INFO] Nincs több topiclista oldal ennél az alforumnál: {subforum_title}")
            break

        print(f"[INFO] Következő topiclista oldalra lépek ({page_no + 1}. oldal): {next_url}")
        current_url = next_url
        page_no += 1

    return all_topics_finished


def scrape_main(
    fetcher: BrowserFetcher,
    output_dir: str,
    delay: float,
    only_category: Optional[str],
    only_subforum: Optional[str],
    topic_reset_interval: int,
    subforum_reset_interval: int,
) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    index_dir = ensure_dirs(base_output)
    visited_file = ensure_visited_file(index_dir, "visited_topics.txt")
    visited_forumgroups_file = ensure_visited_file(index_dir, "visited_forumgroups.txt")

    visited_topics = load_visited(visited_file)
    visited_topics = {normalize_url_for_dedup(x) for x in visited_topics}

    visited_forumgroups = load_visited(visited_forumgroups_file)
    visited_forumgroups = {normalize_url_for_dedup(x) for x in visited_forumgroups}

    print(f"[INFO] Főoldal megnyitása: {MAIN_FORUM_URL}")
    final_url, html = fetcher.fetch(MAIN_FORUM_URL, wait_ms=int(delay * 1000))

    debug_path = base_output / "debug_index_main.html"
    debug_path.write_text(html, encoding="utf-8")
    print(f"[DEBUG] A főoldal HTML-je elmentve: {debug_path}")

    categories = parse_main_categories(html, final_url)
    print(f"[INFO] Feldolgozandó fórumcsoportok száma: {len(categories)}")

    for cat_idx, cat in enumerate(categories, start=1):
        category_title = cat["category_title"]
        category_url_norm = normalize_url_for_dedup(cat.get("category_url") or category_title)

        if only_category and only_category.lower() not in category_title.lower():
            continue

        if not only_subforum and category_url_norm in visited_forumgroups:
            print(f"\n[INFO] Fórumcsoport már teljesen feldolgozva, kihagyva: {category_title}")
            continue

        print(f"\n[INFO] Fórumcsoport ({cat_idx}/{len(categories)}): {category_title}")
        forumgroup_fully_finished = True
        processed_subforums = 0

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
                processed_subforums += 1
                subforum_finished = scrape_subforum(
                    fetcher=fetcher,
                    category_title=category_title,
                    subforum_title=subforum_title,
                    subforum_url=subforum_url,
                    base_index_dir=index_dir,
                    visited_file=visited_file,
                    visited_topics=visited_topics,
                    delay=delay,
                    topic_reset_interval=topic_reset_interval,
                    subforum_reset_interval=subforum_reset_interval,
                )
                if not subforum_finished:
                    forumgroup_fully_finished = False
            except Exception as e:
                forumgroup_fully_finished = False
                print(f"[WARN] Hiba alforum feldolgozás közben: {subforum_url} | {e}")

        if not only_subforum and processed_subforums > 0 and forumgroup_fully_finished:
            append_visited_if_missing(
                visited_forumgroups_file,
                visited_forumgroups,
                category_url_norm,
            )
            print(f"[INFO] Fórumcsoport teljesen kész, visited_forumgroups-ba írva: {category_title} | {category_url_norm}")
        elif not only_subforum:
            print(f"[INFO] Fórumcsoport NINCS teljesen kész, ezért NEM kerül visited_forumgroups-ba: {category_title}")

        gc.collect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Index Fórum scraper Playwright + BeautifulSoup alapon, appendelt JSON mentéssel."
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre az index mappa.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.2,
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
        "--auto-reset-fetches",
        type=int,
        default=120,
        help="Ennyi fetch után automatikus context reset.",
    )
    parser.add_argument(
        "--topic-reset-interval",
        type=int,
        default=25,
        help="Ennyi kommentoldalanként teljes context reset hosszú topicoknál.",
    )
    parser.add_argument(
        "--subforum-reset-interval",
        type=int,
        default=20,
        help="Ennyi topiclista oldalanként teljes context reset alforumon belül.",
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
            scrape_main(
                fetcher=fetcher,
                output_dir=args.output,
                delay=args.delay,
                only_category=args.only_category,
                only_subforum=args.only_subforum,
                topic_reset_interval=args.topic_reset_interval,
                subforum_reset_interval=args.subforum_reset_interval,
            )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva felhasználó által.")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] Végzetes hiba: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

    # python index_scraper.py --output ./index --delay 1.5