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


# --------------------------------------------------
# Adatmodellek
# --------------------------------------------------

@dataclass
class CategoryInfo:
    section_title: str
    category_title: str
    category_url: str


@dataclass
class TopicInfo:
    section_title: str
    category_title: str
    category_url: str
    topic_title: str
    topic_url: str


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


def topic_file_path_by_parts(data_dir: Path, section_title: str, category_title: str, topic_title: str) -> Path:
    return (
        data_dir
        / sanitize_filename(section_title)
        / sanitize_filename(category_title)
        / f"{sanitize_filename(topic_title)}.json"
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
            "rights": "sg.hu fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "section_title": topic.section_title,
                "category_title": topic.category_title,
                "category_url": normalize_category_url_for_visited(topic.category_url),
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
            if self.page: self.page.close()
            if self.context: self.context.close()
            if self.browser: self.browser.close()
            if self.playwright: self.playwright.stop()
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
                    try: route.continue_()
                    except: pass

            self.context.route("**/*", route_handler)

        self.page = self.context.new_page()
        self.page.set_default_timeout(self.timeout_ms)
        self.page.set_default_navigation_timeout(self.timeout_ms)

    def reset_context(self) -> None:
        try:
            if self.page: self.page.close()
            if self.context: self.context.close()
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
        for txt in ["ÖSSZES ELUTASÍTÁSA", "Összes elutasítása", "Elutasítás", "Reject all", "Elfogadom"]:
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


# --------------------------------------------------
# Főoldal: témacsoportok (Javítva)
# --------------------------------------------------

def parse_categories_from_forum_main(html: str, page_url: str) -> List[CategoryInfo]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[CategoryInfo] = []
    seen: Set[str] = set()

    for a in soup.select('a[href^="/forum/temak/"]'):
        href = a.get("href")
        url = normalize_category_url_for_visited(urljoin(page_url, href))
        if url in seen:
            continue

        # Kategória címének tiszta kinyerése (Ha van benne span, az első span a cím)
        spans = a.find_all("span")
        if spans:
            cat_title = clean_text(spans[0].get_text(strip=True))
        else:
            cat_title = clean_text(a.get_text(strip=True))
            # Fallback: levágjuk a sor végi "11:47 3047 db" és dátum szövegeket
            cat_title = re.sub(r'\s+(?:ma|tegnap|tegnapelőtt|\d{4}\.\s*\d{2}\.\s*\d{2}\.?).*$', '', cat_title, flags=re.I)
            cat_title = re.sub(r'\s+\d+[\d .]*\s*db$', '', cat_title, flags=re.I)
            cat_title = cat_title.strip()

        if not cat_title:
            continue

        # Megkeressük, melyik szekcióhoz (pl. "Általános fórumok") tartozik
        sec_title = "Egyéb"
        prev_h = a.find_previous(["h1", "h2", "h3", "h4"])
        while prev_h:
            t = clean_text(prev_h.get_text(strip=True))
            if t in ALLOWED_SECTION_TITLES:
                sec_title = t
                break
            prev_h = prev_h.find_previous(["h1", "h2", "h3", "h4"])

        if sec_title in SKIP_SECTION_TITLES or sec_title not in ALLOWED_SECTION_TITLES:
            continue

        seen.add(url)
        results.append(CategoryInfo(section_title=sec_title, category_title=cat_title, category_url=url))

    del soup
    gc.collect()
    return results


# --------------------------------------------------
# Témacsoport oldal (Javítva)
# --------------------------------------------------

def parse_topics_from_category_page(
    html: str, page_url: str, section_title: str, category_title: str, category_url: str
) -> List[TopicInfo]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[TopicInfo] = []
    seen: Set[str] = set()

    # Megkeressük az "A fórum témái" feliratot, és annak a befoglaló blokkját
    container = soup
    for h in soup.select("h1, h2, h3"):
        if "a fórum témái" in clean_text(h.get_text(strip=True)).lower():
            if h.parent:
                container = h.parent
                break

    for a in container.select('a[href^="/forum/tema/"]'):
        href = a.get("href")
        url = normalize_topic_url_for_visited(urljoin(page_url, href))
        if url in seen:
            continue

        # Topic címének kinyerése a modern dizájnból (az első szöveges span a cím)
        spans = a.find_all("span")
        if spans:
            topic_title = clean_text(spans[0].get_text(strip=True))
        else:
            topic_title = clean_text(a.get_text(strip=True))
            topic_title = re.sub(r'\s+(?:ma|tegnap|tegnapelőtt|\d{4}\.\s*\d{2}\.\s*\d{2}\.?).*$', '', topic_title, flags=re.I)
            topic_title = re.sub(r'\s+\d+[\d .]*\s*db$', '', topic_title, flags=re.I)
            topic_title = topic_title.strip()

        if len(topic_title) >= 2:
            seen.add(url)
            results.append(TopicInfo(section_title, category_title, category_url, topic_title, url))

    del soup
    gc.collect()
    return results


# --------------------------------------------------
# Lapozás (Javítva)
# --------------------------------------------------

def parse_pagination_info(html: str, current_url: str) -> Tuple[int, Optional[int], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    current_page = get_page_no_from_url(current_url)
    total_pages = None
    next_url = None

    page_text = clean_text(soup.get_text(" ", strip=True))
    m = re.search(r"(?:Oldal\s*)?(\d+)\s*/\s*(\d+)", page_text, flags=re.I)
    if m:
        current_page = int(m.group(1))
        total_pages = int(m.group(2))

    for a in soup.select("a[href]"):
        txt = clean_text(a.get_text(strip=True)).lower()
        href = a.get("href")
        if not href:
            continue
        if "következő" in txt or "->" in txt or "→" in txt or ">" in txt:
            full = urljoin(current_url, href)
            if get_page_no_from_url(full) > current_page:
                next_url = full
                break

    if not next_url and total_pages and current_page < total_pages:
        next_url = set_query_param(strip_fragment(current_url), "page", str(current_page + 1))

    del soup
    gc.collect()
    return current_page, total_pages, next_url


# --------------------------------------------------
# Témaoldal kommentek (Javítva)
# --------------------------------------------------

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
    return m.group(1) if m else None

def extract_author_date_header(block: Tag) -> Tuple[str, Optional[str]]:
    text = block.get_text("\n", strip=True)
    lines = [clean_text(x) for x in text.splitlines() if clean_text(x)]
    author = "ismeretlen"
    date_text = None

    for line in lines[:6]:
        m = re.search(r"(\d{4}\.\s*\d{2}\.\s*\d{2}\.?\s*\d{1,2}:\d{2})", line)
        if m:
            date_text = m.group(1)
            break
        # Ha nem dátum és nem azonosító, akkor jó eséllyel a szerző
        if len(line) < 40 and not line.startswith('#') and author == "ismeretlen":
            author = line

    return author, date_text

def extract_message_text(block: Tag) -> str:
    text = block.get_text("\n", strip=True)
    lines = [clean_text(x) for x in text.splitlines() if clean_text(x)]
    body_lines = []
    started = False
    
    for line in lines:
        if re.search(r"\d{4}\.\s*\d{2}\.\s*\d{2}\.?\s*\d{1,2}:\d{2}", line):
            started = True
            continue
        if started:
            body_lines.append(line)
            
    if body_lines:
        return "\n".join(body_lines)
    return "\n".join(lines[2:]) # Fallback

def parse_comments_from_topic_page(html: str, topic_page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    blocks = find_message_blocks(soup)
    results: List[Dict] = []

    for block in blocks:
        comment_id = extract_comment_id(block)
        author, date_text = extract_author_date_header(block)
        body = extract_message_text(block)

        if not body and not author and not date_text:
            continue

        comment_url = strip_fragment(topic_page_url)
        if comment_id:
            comment_url = f"{comment_url}#msg-{comment_id}"

        results.append({
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
        })

    del soup
    gc.collect()
    return results

def comment_to_output_item(c: Dict) -> Dict:
    author_name = c.get("author") or "ismeretlen"
    return {
        "authors": [split_name_like_person(author_name)] if author_name else [],
        "data": c.get("data", ""),
        "likes": None, "dislikes": None, "score": None,
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
# Topic feldolgozás
# --------------------------------------------------

def scrape_topic(
    fetcher: BrowserFetcher, data_dir: Path, topic: TopicInfo, delay: float, topic_reset_interval: int
) -> int:
    fetcher.reset_context()

    initial_url = normalize_topic_url_for_visited(topic.topic_url)
    print(f"[INFO] Téma megnyitása: {topic.topic_title} | URL: {initial_url}")

    topic_file = topic_file_path_by_parts(data_dir, topic.section_title, topic.category_title, topic.topic_title)
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
        current_page_no, total_pages, next_url = parse_pagination_info(html, current_url)
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

        page_fingerprint = hashlib.sha1("\n".join(stable_comment_signature(c) for c in page_comments).encode("utf-8")).hexdigest()
        if page_fingerprint in seen_fingerprints:
            break
        seen_fingerprints.add(page_fingerprint)

        for comment in page_comments:
            append_comment_to_stream_file(topic_file, comment_to_output_item(comment), has_existing_comments)
            has_existing_comments = True
            total_downloaded += 1

        print(f"[INFO] Téma: {topic.topic_title} | Oldal: {current_page_no}/{total_pages or '?'} | Eddig letöltve: {total_downloaded}")

        if not next_url:
            break

        next_page_no = get_page_no_from_url(next_url)
        if total_pages and current_page_no >= total_pages:
            break
        if next_page_no <= current_page_no:
            break

        page_hops += 1
        if topic_reset_interval > 0 and page_hops % topic_reset_interval == 0:
            fetcher.reset_context()

        try:
            current_url, html = fetcher.fetch(next_url, wait_ms=int(delay * 1000))
        except Exception:
            fallback_url = build_topic_page_url(topic.topic_url, next_page_no)
            current_url, html = fetcher.fetch(fallback_url, wait_ms=int(delay * 1000))

    finalize_stream_json(topic_file)
    return total_downloaded


# --------------------------------------------------
# Témacsoport feldolgozás
# --------------------------------------------------

def scrape_category(
    fetcher: BrowserFetcher, data_dir: Path, visited_topics_file: Path, visited_categories_file: Path,
    visited_topics: Set[str], visited_categories: Set[str], category: CategoryInfo, delay: float,
    only_topic: Optional[str], topic_reset_interval: int
) -> None:
    category_key = normalize_category_url_for_visited(category.category_url)
    if category_key in visited_categories:
        print(f"[INFO] Témacsoport már kész: {category.category_title}")
        return

    fetcher.reset_context()
    current_url = category.category_url

    print(f"\n[INFO] Témacsoport: {category.section_title} / {category.category_title} | URL: {category.category_url}")

    while True:
        current_url, html = fetcher.fetch(current_url, wait_ms=int(delay * 1000))
        current_page_no, total_pages, next_url = parse_pagination_info(html, current_url)

        topics = parse_topics_from_category_page(html, current_url, category.section_title, category.category_title, category.category_url)
        print(f"[INFO] Kategória oldal: {current_page_no}/{total_pages or '?'} | Talált témák: {len(topics)}")

        for idx, topic in enumerate(topics, start=1):
            if only_topic and only_topic.lower() not in topic.topic_title.lower():
                continue

            topic_key = normalize_topic_url_for_visited(topic.topic_url)
            if topic_key in visited_topics:
                continue

            scrape_topic(fetcher, data_dir, topic, delay, topic_reset_interval)
            append_visited(visited_topics_file, topic_key)
            visited_topics.add(topic_key)

        if not next_url:
            break

        next_page_no = get_page_no_from_url(next_url)
        if total_pages and current_page_no >= total_pages:
            break
        if next_page_no <= current_page_no:
            break

        fallback_url = build_category_page_url(category.category_url, next_page_no)
        current_url = fallback_url

    append_visited(visited_categories_file, category_key)
    visited_categories.add(category_key)


# --------------------------------------------------
# Fő vezérlés és CLI
# --------------------------------------------------

def scrape_forum(
    fetcher: BrowserFetcher, output_dir: str, delay: float, only_section: Optional[str],
    only_category: Optional[str], only_topic: Optional[str], topic_reset_interval: int
) -> None:
    paths = ensure_dirs(Path(output_dir).expanduser().resolve())
    visited_topics = {normalize_topic_url_for_visited(x) for x in load_visited(paths["visited_topics"])}
    visited_categories = {normalize_category_url_for_visited(x) for x in load_visited(paths["visited_categories"])}

    print(f"[INFO] Fő fórum megnyitása: {FORUM_URL}")
    final_url, html = fetcher.fetch(FORUM_URL, wait_ms=int(delay * 1000))

    categories = parse_categories_from_forum_main(html, final_url)
    print(f"[INFO] Talált feldolgozandó témacsoportok: {len(categories)}")

    for idx, category in enumerate(categories, start=1):
        if only_section and only_section.lower() not in category.section_title.lower(): continue
        if only_category and only_category.lower() not in category.category_title.lower(): continue

        scrape_category(
            fetcher, paths["data"], paths["visited_topics"], paths["visited_categories"],
            visited_topics, visited_categories, category, delay, only_topic, topic_reset_interval
        )

    print("[INFO] Minden feldolgozható témacsoport végigment.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="sg.hu fórum scraper Playwright + BeautifulSoup")
    parser.add_argument("--output", default=".", help="Kimeneti mappa.")
    parser.add_argument("--delay", type=float, default=1.5, help="Várakozás (mp).")
    parser.add_argument("--only-section", default=None, help="Csak adott szekció.")
    parser.add_argument("--only-category", default=None, help="Csak adott kategória.")
    parser.add_argument("--only-topic", default=None, help="Csak adott téma.")
    parser.add_argument("--headed", action="store_true", help="Látható böngészővel.")
    parser.add_argument("--timeout-ms", type=int, default=90000, help="Timeout (ms).")
    parser.add_argument("--retries", type=int, default=4, help="Újrapróbálkozások.")
    parser.add_argument("--topic-reset-interval", type=int, default=25, help="Context reset intervallum.")
    parser.add_argument("--auto-reset-fetches", type=int, default=120, help="Automatikus reset (fetch count).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        with BrowserFetcher(
            headless=not args.headed, slow_mo=50 if args.headed else 0,
            timeout_ms=args.timeout_ms, retries=args.retries, auto_reset_fetches=args.auto_reset_fetches
        ) as fetcher:
            scrape_forum(
                fetcher, args.output, args.delay, args.only_section, args.only_category,
                args.only_topic, args.topic_reset_interval
            )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva.")
        sys.exit(1)

if __name__ == "__main__":
    main()

# python sg_forum_scraper.py --output ./SG --headed
# python sg_forum_scraper.py --output ./SG --headed --only-category "Általános eszmecsere"
# python sg_forum_scraper.py --output ./SG --headed --only-topic "Garfield képregény"