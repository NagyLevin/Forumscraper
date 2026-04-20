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
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright

BASE_URL = "https://gepigeny.hu"
FORUM_URL = "https://gepigeny.hu/forum/"

FORUM_GROUP_LINK_RE = re.compile(r"viewforum\.php\?forum_id=\d+", re.IGNORECASE)
TOPIC_LINK_RE = re.compile(r"viewthread\.php\?thread_id=\d+", re.IGNORECASE)
COMMENT_BOX_ID_RE = re.compile(r"^c\d+$", re.IGNORECASE)


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


def remove_query_param(url: str, key: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    if key in query:
        del query[key]
    query_parts: List[str] = []
    for k, vals in query.items():
        for v in vals:
            query_parts.append(f"{k}={v}")
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, "&".join(query_parts), parsed.fragment))


def normalize_group_url_for_visited(url: str) -> str:
    return strip_fragment(remove_query_param(url, "rowstart"))


def normalize_topic_url_for_visited(url: str) -> str:
    return strip_fragment(remove_query_param(url, "start"))


def normalize_comment_url(url: str) -> str:
    return strip_fragment(url)


def short_preview(text: str, max_len: int = 120) -> str:
    txt = normalize_ws_inline(text)
    if len(txt) <= max_len:
        return txt
    return txt[: max_len - 3].rstrip() + "..."


def split_name_like_person(name: str) -> Dict[str, str]:
    name = clean_text(name)
    if not name:
        return {"name": ""}
    parts = name.split()
    if len(parts) >= 2:
        return {"family": parts[0], "given": " ".join(parts[1:])}
    return {"name": name}


def stable_comment_signature(comment: Dict) -> str:
    comment_id = str(comment.get("comment_id") or "")
    author = clean_text(comment.get("author") or "")
    date = clean_text(comment.get("date") or "")
    text = clean_text(comment.get("data") or "")[:300]
    return f"{comment_id}|{author}|{date}|{text}"


def get_thread_id(url: str) -> Optional[str]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    vals = query.get("thread_id")
    return vals[0] if vals else None


def get_forum_id(url: str) -> Optional[str]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    vals = query.get("forum_id")
    return vals[0] if vals else None


def parse_int_loose(text: str) -> Optional[int]:
    text = clean_text(text)
    if not text:
        return None
    norm = text.replace(" ", "").replace(".", "")
    m = re.search(r"\d+", norm)
    if not m:
        return None
    return int(m.group(0))


def build_group_page_url(group_url: str, rowstart: int) -> str:
    base = normalize_group_url_for_visited(group_url)
    if rowstart <= 0:
        return base
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}rowstart={rowstart}"


def build_topic_page_url(topic_url: str, start: int) -> str:
    base = normalize_topic_url_for_visited(topic_url)
    if start <= 0:
        return base + "#comments"
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}start={start}#comments"


# --------------------------------------------------
# Adatmodellek
# --------------------------------------------------

@dataclass
class ForumGroupInfo:
    group_title: str
    group_url: str
    forum_id: Optional[str]
    topic_count_hint: Optional[int]
    comment_count_hint: Optional[int]


@dataclass
class TopicInfo:
    group_title: str
    group_url: str
    topic_title: str
    topic_url: str
    thread_id: Optional[str]
    reply_count_hint: Optional[int]
    last_activity_hint: Optional[str]


# --------------------------------------------------
# Állapot / output
# --------------------------------------------------

def ensure_dirs(base_output: Path) -> Dict[str, Path]:
    root = base_output / "gepigeny_forum"
    data_dir = root / "data"
    state_dir = root / "state"

    data_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    visited_topics = state_dir / "visited_topics.txt"
    visited_groups = state_dir / "visited_forum_groups.txt"

    if not visited_topics.exists():
        visited_topics.write_text("", encoding="utf-8")
    if not visited_groups.exists():
        visited_groups.write_text("", encoding="utf-8")

    return {
        "root": root,
        "data": data_dir,
        "state": state_dir,
        "visited_topics": visited_topics,
        "visited_groups": visited_groups,
    }


def load_visited(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def append_visited(path: Path, value: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(value.strip() + "\n")


def topic_file_path_by_parts(data_dir: Path, group_title: str, topic_title: str) -> Path:
    return data_dir / sanitize_filename(group_title) / f"{sanitize_filename(topic_title)}.json"


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


def write_topic_stream_header(topic_file: Path, topic: TopicInfo) -> None:
    header_obj = {
        "title": sanitize_filename(topic.topic_title),
        "authors": [],
        "data": {
            "content": topic.topic_title,
            "likes": None,
            "dislikes": None,
            "score": None,
            "rating": None,
            "date": None,
            "url": normalize_topic_url_for_visited(topic.topic_url),
            "language": "hu",
            "tags": [],
            "rights": "gepigeny.hu fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "forum_group_title": topic.group_title,
                "forum_group_url": normalize_group_url_for_visited(topic.group_url),
                "thread_id": topic.thread_id,
                "reply_count_hint": topic.reply_count_hint,
                "last_activity_hint": topic.last_activity_hint,
            },
            "origin": "gepigeny_forum",
        },
        "origin": "gepigeny_forum",
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
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
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

    def reset_context(self) -> None:
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
        except Exception:
            pass
        self._create_context_and_page()
        gc.collect()

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

    def dismiss_overlays_if_present(self) -> None:
        for txt in [
            "ÖSSZES ELUTASÍTÁSA",
            "Összes elutasítása",
            "Elutasítás",
            "Reject all",
            "Elfogadom",
            "Beleegyezem",
        ]:
            try:
                locator = self.page.locator(f"text={txt}").first
                if locator.count() > 0:
                    locator.click(timeout=1500, force=True)
                    self.page.wait_for_timeout(500)
                    return
            except Exception:
                pass

    def fetch(self, url: str, wait_ms: int = 1500) -> Tuple[str, str]:
        last_exc = None
        if self.auto_reset_fetches > 0 and self.fetch_counter > 0 and self.fetch_counter % self.auto_reset_fetches == 0:
            self.reset_context()

        for attempt in range(1, self.retries + 1):
            try:
                self.ensure_page_alive()
                self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                self.page.wait_for_timeout(wait_ms)
                self.dismiss_overlays_if_present()

                final_url = self.page.url
                html = self.page.content()
                self.fetch_counter += 1
                return final_url, html
            except Exception as e:
                last_exc = e
                print(f"[WARN] Fetch hiba ({attempt}/{self.retries}) -> {url} | {e}")
                if attempt < self.retries:
                    self.reset_context()

        raise last_exc

    def click_and_fetch_html(self, selector: str, wait_ms: int = 1500) -> Tuple[str, str]:
        self.ensure_page_alive()
        self.page.locator(selector).first.click(timeout=self.timeout_ms)
        self.page.wait_for_timeout(wait_ms)
        self.dismiss_overlays_if_present()
        return self.page.url, self.page.content()


# --------------------------------------------------
# Főoldal: fórumcsoportok
# --------------------------------------------------

def parse_forum_groups(html: str, page_url: str) -> List[ForumGroupInfo]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[ForumGroupInfo] = []
    seen: Set[str] = set()

    for a in soup.select("a.forum_c_cont[href], a[href]"):
        href = a.get("href") or ""
        if not FORUM_GROUP_LINK_RE.search(href):
            continue

        full_url = normalize_group_url_for_visited(urljoin(page_url, href))
        if full_url in seen:
            continue

        name_el = a.select_one(".forum_c_name")
        info_el = a.select_one(".forum_c_inf")

        title = clean_text(name_el.get_text(" ", strip=True) if name_el else a.get_text(" ", strip=True))
        if not title:
            continue

        topic_count_hint = None
        comment_count_hint = None
        info_text = clean_text(info_el.get_text(" ", strip=True) if info_el else "")
        if info_text:
            m_topics = re.search(r"Témák:\s*([\d .]+)", info_text, flags=re.I)
            m_comments = re.search(r"Hozzászólások:\s*([\d .]+)", info_text, flags=re.I)
            if m_topics:
                topic_count_hint = parse_int_loose(m_topics.group(1))
            if m_comments:
                comment_count_hint = parse_int_loose(m_comments.group(1))

        seen.add(full_url)
        results.append(
            ForumGroupInfo(
                group_title=title,
                group_url=full_url,
                forum_id=get_forum_id(full_url),
                topic_count_hint=topic_count_hint,
                comment_count_hint=comment_count_hint,
            )
        )

    del soup
    gc.collect()
    return results


# --------------------------------------------------
# Fórumcsoport oldal: topicok + lapozás
# --------------------------------------------------

def parse_topics_from_group_page(html: str, current_url: str, group: ForumGroupInfo) -> List[TopicInfo]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[TopicInfo] = []
    seen: Set[str] = set()

    for a in soup.select("a.forum-tbk-block[href], a[href]"):
        href = a.get("href") or ""
        if not TOPIC_LINK_RE.search(href):
            continue

        full_url = normalize_topic_url_for_visited(urljoin(current_url, href))
        if full_url in seen:
            continue

        title_el = a.select_one(".forum-tbk-name")
        if title_el is None:
            continue

        topic_title = clean_text(title_el.get_text(" ", strip=True))
        if not topic_title:
            continue

        reply_count_hint = None
        last_activity_hint = None

        stat_text = clean_text(a.get_text(" ", strip=True))
        date_match = re.search(r"\d{4}\.\s*\d{2}\.\s*\d{2}\.\s*\d{2}:\d{2}:\d{2}", stat_text)
        if date_match:
            last_activity_hint = date_match.group(0)

        header_parts = [clean_text(x.get_text(" ", strip=True)) for x in a.select(".forum-tbk-a, .forum-tbk-b")]
        for part in header_parts:
            nums = re.findall(r"\d[\d ]*", part)
            if nums:
                parsed = parse_int_loose(nums[-1])
                if parsed is not None:
                    reply_count_hint = parsed

        seen.add(full_url)
        results.append(
            TopicInfo(
                group_title=group.group_title,
                group_url=group.group_url,
                topic_title=topic_title,
                topic_url=full_url,
                thread_id=get_thread_id(full_url),
                reply_count_hint=reply_count_hint,
                last_activity_hint=last_activity_hint,
            )
        )

    del soup
    gc.collect()
    return results


def parse_group_pagination_info(html: str, current_url: str) -> Tuple[int, Optional[int], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    current_page = 1
    total_pages = None
    next_url = None

    pager = soup.select_one(".pagenav")
    if pager:
        active = pager.select_one(".pagenav_c.active")
        if active:
            title = clean_text(active.get("title") or active.get_text(" ", strip=True))
            m = re.search(r"Oldal\s*(\d+)\s*/\s*(\d+)", title, flags=re.I)
            if m:
                current_page = int(m.group(1))
                total_pages = int(m.group(2))
            else:
                n = parse_int_loose(active.get_text(" ", strip=True))
                if n:
                    current_page = n

        nums: List[int] = []
        for el in pager.select("a.pagenav_c, span.pagenav_c"):
            t = clean_text(el.get_text(" ", strip=True))
            if t.isdigit():
                nums.append(int(t))
            ttl = clean_text(el.get("title") or "")
            m2 = re.search(r"Oldal\s*(\d+)\s*/\s*(\d+)", ttl, flags=re.I)
            if m2:
                nums.extend([int(m2.group(1)), int(m2.group(2))])
        if nums:
            total_pages = max(nums) if total_pages is None else max(total_pages, max(nums))

        next_a = pager.select_one("a.pagenav_s[href]")
        if next_a:
            next_href = next_a.get("href")
            if next_href:
                next_url = urljoin(current_url, next_href)

    del soup
    gc.collect()
    return current_page, total_pages, next_url


# --------------------------------------------------
# Topic oldal: kommentek + lapozás
# --------------------------------------------------

def parse_topic_header_info(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    result = {
        "title": None,
        "comment_count": None,
    }

    h1 = soup.select_one(".main-column h1")
    if h1:
        result["title"] = clean_text(h1.get_text(" ", strip=True))

    panel_title = soup.select_one("#comments .panel-title")
    if panel_title:
        text = clean_text(panel_title.get_text(" ", strip=True))
        m = re.search(r"Hozzászólások:\s*([\d ]+)", text, flags=re.I)
        if m:
            result["comment_count"] = str(parse_int_loose(m.group(1)) or "")

    del soup
    gc.collect()
    return result


def parse_comments_from_topic_page(html: str, topic_page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[Dict] = []

    for box in soup.select("div.comment_box[id]"):
        box_id = (box.get("id") or "").strip()
        if not COMMENT_BOX_ID_RE.match(box_id):
            continue

        comment_id = box_id[1:]

        author = "ismeretlen"
        author_el = box.select_one("a.comm-u[href], .comm-u")
        if author_el:
            author = clean_text(author_el.get_text(" ", strip=True)) or "ismeretlen"

        meta_text = clean_text(box.select_one(".comm-inf").get_text(" ", strip=True) if box.select_one(".comm-inf") else "")
        date_text = None
        if meta_text:
            date_match = re.search(r"\d{4}\.\s*\d{2}\.\s*\d{2}\.\s*\d{2}:\d{2}:\d{2}", meta_text)
            if date_match:
                date_text = date_match.group(0)

        profile_level = None
        lvl_el = box.select_one(".comm-ul")
        if lvl_el:
            profile_level = clean_text(lvl_el.get_text(" ", strip=True))

        permalink = box.select_one("a.comm-d[href]")
        comment_url = normalize_comment_url(topic_page_url)
        if permalink and permalink.get("href"):
            comment_url = urljoin(topic_page_url, permalink.get("href"))
        comment_url = f"{strip_fragment(comment_url)}#c{comment_id}"

        body_el = box.select_one(".comm-text")
        body = clean_text(body_el.get_text("\n", strip=True) if body_el else "")

        if not body and not author and not date_text:
            continue

        results.append(
            {
                "comment_id": comment_id,
                "author": author,
                "date": date_text,
                "profile_level": profile_level,
                "rating": None,
                "parent_author": None,
                "index": None,
                "index_total": None,
                "is_offtopic": False,
                "url": comment_url,
                "data": body,
            }
        )

    del soup
    gc.collect()
    return results


def parse_topic_pagination_info(html: str, current_url: str) -> Tuple[int, Optional[int], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    current_page = 1
    total_pages = None
    next_url = None

    pager = soup.select_one(".pagenav")
    if pager:
        active = pager.select_one(".pagenav_c.active")
        if active:
            t = clean_text(active.get_text(" ", strip=True))
            if t.isdigit():
                current_page = int(t)
            title = clean_text(active.get("title") or "")
            m = re.search(r"Oldal\s*(\d+)\s*/\s*(\d+)", title, flags=re.I)
            if m:
                current_page = int(m.group(1))
                total_pages = int(m.group(2))

        nums: List[int] = []
        for el in pager.select("a.pagenav_c, span.pagenav_c"):
            t = clean_text(el.get_text(" ", strip=True))
            if t.isdigit():
                nums.append(int(t))
            title = clean_text(el.get("title") or "")
            m2 = re.search(r"Oldal\s*(\d+)\s*/\s*(\d+)", title, flags=re.I)
            if m2:
                nums.extend([int(m2.group(1)), int(m2.group(2))])
        if nums:
            total_pages = max(nums) if total_pages is None else max(total_pages, max(nums))

        next_a = pager.select_one("a.pagenav_s[href]")
        if next_a and next_a.get("href"):
            next_url = urljoin(current_url, next_a.get("href"))

    del soup
    gc.collect()
    return current_page, total_pages, next_url


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
            "profile_level": c.get("profile_level"),
        },
    }


# --------------------------------------------------
# Topic feldolgozás
# --------------------------------------------------

def scrape_topic(
    fetcher: BrowserFetcher,
    data_dir: Path,
    topic: TopicInfo,
    delay: float,
    topic_reset_interval: int,
    preview: bool,
) -> int:
    fetcher.reset_context()

    initial_url = build_topic_page_url(topic.topic_url, 0)
    print(f"[INFO] Téma megnyitása: {topic.topic_title} | URL: {initial_url}")

    topic_file = topic_file_path_by_parts(data_dir, topic.group_title, topic.topic_title)
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
            resume_url = strip_fragment(last_comment_url)
            resume_after_comment_id = last_comment_id
            need_init_file = False
            print(f"[INFO] Folytatás: {resume_url} | Utolsó comment_id: {resume_after_comment_id}")

    try:
        current_url, html = fetcher.fetch(resume_url or initial_url, wait_ms=int(delay * 1000))
    except Exception as e:
        print(f"[WARN] Fetch sikertelen ({e}), próbálkozás a téma elejéről.")
        current_url, html = fetcher.fetch(initial_url, wait_ms=int(delay * 1000))
        resume_after_comment_id = None
        need_init_file = not topic_file.exists()

    if need_init_file:
        write_topic_stream_header(topic_file, topic)

    total_downloaded = existing_comments
    has_existing_comments = existing_comments > 0
    first_page_after_resume = resume_after_comment_id is not None

    seen_fingerprints: Set[str] = set()
    page_hops = 0

    while True:
        current_page_no, total_pages, next_url = parse_topic_pagination_info(html, current_url)
        page_comments = parse_comments_from_topic_page(html, current_url)

        if current_page_no > 1 and not page_comments:
            break

        if first_page_after_resume and resume_after_comment_id:
            filtered = []
            seen_last = False
            for c in page_comments:
                if not seen_last:
                    if str(c.get("comment_id") or "") == str(resume_after_comment_id):
                        seen_last = True
                    continue
                filtered.append(c)
            page_comments = filtered if seen_last else page_comments
            first_page_after_resume = False

        page_fingerprint = hashlib.sha1(
            "\n".join(stable_comment_signature(c) for c in page_comments).encode("utf-8")
        ).hexdigest()

        if page_fingerprint in seen_fingerprints:
            break
        seen_fingerprints.add(page_fingerprint)

        for comment in page_comments:
            if preview:
                author = comment.get("author") or "ismeretlen"
                date = comment.get("date") or "nincs dátum"
                preview_text = short_preview(comment.get("data", ""))
                print(f"[PREVIEW] {author} | {date} | {preview_text}")

            append_comment_to_stream_file(topic_file, comment_to_output_item(comment), has_existing_comments)
            has_existing_comments = True
            total_downloaded += 1

        print(f"[INFO] Téma: {topic.topic_title} | Oldal: {current_page_no}/{total_pages or '?'} | Eddig letöltve: {total_downloaded}")

        if not next_url:
            break
        if total_pages and current_page_no >= total_pages:
            break

        page_hops += 1
        if topic_reset_interval > 0 and page_hops % topic_reset_interval == 0:
            fetcher.reset_context()

        try:
            current_url, html = fetcher.fetch(next_url, wait_ms=int(delay * 1000))
        except Exception:
            fallback_start = page_hops * 50
            fallback_url = build_topic_page_url(topic.topic_url, fallback_start)
            current_url, html = fetcher.fetch(fallback_url, wait_ms=int(delay * 1000))

    finalize_stream_json(topic_file)
    return total_downloaded


# --------------------------------------------------
# Fórumcsoport feldolgozás
# --------------------------------------------------

def scrape_group(
    fetcher: BrowserFetcher,
    data_dir: Path,
    visited_topics_file: Path,
    visited_groups_file: Path,
    visited_topics: Set[str],
    visited_groups: Set[str],
    group: ForumGroupInfo,
    delay: float,
    only_topic: Optional[str],
    topic_reset_interval: int,
    preview: bool,
) -> None:
    group_key = normalize_group_url_for_visited(group.group_url)
    if group_key in visited_groups:
        print(f"[INFO] Fórumcsoport már kész: {group.group_title}")
        return

    fetcher.reset_context()
    current_url = group.group_url

    print(f"\n[INFO] Fórumcsoport: {group.group_title} | URL: {group.group_url}")

    while True:
        current_url, html = fetcher.fetch(current_url, wait_ms=int(delay * 1000))
        current_page_no, total_pages, next_url = parse_group_pagination_info(html, current_url)

        topics = parse_topics_from_group_page(html, current_url, group)
        print(f"[INFO] Fórumcsoport oldal: {current_page_no}/{total_pages or '?'} | Talált témák: {len(topics)}")

        for topic in topics:
            if only_topic and only_topic.lower() not in topic.topic_title.lower():
                continue

            topic_key = normalize_topic_url_for_visited(topic.topic_url)
            if topic_key in visited_topics:
                continue

            scrape_topic(fetcher, data_dir, topic, delay, topic_reset_interval, preview)
            append_visited(visited_topics_file, topic_key)
            visited_topics.add(topic_key)

        if not next_url:
            break
        if total_pages and current_page_no >= total_pages:
            break

        fallback_rowstart = current_page_no * 75
        current_url = next_url or build_group_page_url(group.group_url, fallback_rowstart)

    append_visited(visited_groups_file, group_key)
    visited_groups.add(group_key)


# --------------------------------------------------
# Fő vezérlés és CLI
# --------------------------------------------------

def scrape_forum(
    fetcher: BrowserFetcher,
    output_dir: str,
    delay: float,
    only_group: Optional[str],
    only_topic: Optional[str],
    topic_reset_interval: int,
    preview: bool,
) -> None:
    paths = ensure_dirs(Path(output_dir).expanduser().resolve())
    visited_topics = {normalize_topic_url_for_visited(x) for x in load_visited(paths["visited_topics"])}
    visited_groups = {normalize_group_url_for_visited(x) for x in load_visited(paths["visited_groups"])}

    print(f"[INFO] Fő fórum megnyitása: {FORUM_URL}")
    final_url, html = fetcher.fetch(FORUM_URL, wait_ms=int(delay * 1000))

    groups = parse_forum_groups(html, final_url)
    print(f"[INFO] Talált fórumcsoportok: {len(groups)}")

    for idx, group in enumerate(groups, start=1):
        if only_group and only_group.lower() not in group.group_title.lower():
            continue

        print(f"[INFO] Fórumcsoport {idx}/{len(groups)}: {group.group_title}")
        scrape_group(
            fetcher,
            paths["data"],
            paths["visited_topics"],
            paths["visited_groups"],
            visited_topics,
            visited_groups,
            group,
            delay,
            only_topic,
            topic_reset_interval,
            preview,
        )

    print("[INFO] Minden feldolgozható fórumcsoport végigment.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="gepigeny.hu fórum scraper Playwright + BeautifulSoup")
    parser.add_argument("--output", default=".", help="Kimeneti mappa.")
    parser.add_argument("--delay", type=float, default=1.5, help="Várakozás (mp).")
    parser.add_argument("--only-group", default=None, help="Csak adott fórumcsoport.")
    parser.add_argument("--only-topic", default=None, help="Csak adott téma.")
    parser.add_argument("--headed", action="store_true", help="Látható böngészővel.")
    parser.add_argument("--timeout-ms", type=int, default=90000, help="Timeout (ms).")
    parser.add_argument("--retries", type=int, default=4, help="Újrapróbálkozások.")
    parser.add_argument("--topic-reset-interval", type=int, default=25, help="Context reset intervallum.")
    parser.add_argument("--auto-reset-fetches", type=int, default=120, help="Automatikus reset (fetch count).")
    parser.add_argument("--preview", action="store_true", help="Komment preview kiírása.")
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
        ) as fetcher:
            scrape_forum(
                fetcher,
                args.output,
                args.delay,
                args.only_group,
                args.only_topic,
                args.topic_reset_interval,
                args.preview,
            )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva.")
        sys.exit(1)


if __name__ == "__main__":
    main()

# python gepigeny_forum_scraper.py --output ./Gepigeny --headed
# python gepigeny_forum_scraper.py --output ./Gepigeny --headed --only-group "Játékokról általában"
# python gepigeny_forum_scraper.py --output ./Gepigeny --preview --delay 3
