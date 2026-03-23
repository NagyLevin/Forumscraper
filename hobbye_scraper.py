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


BASE_URL = "https://www.hobbielektronika.hu"
MAIN_FORUM_URL = "https://www.hobbielektronika.hu/forum/"

COMMENT_ID_RE = re.compile(r'"comment_id"\s*:\s*"([^"]+)"')
COMMENT_URL_RE = re.compile(r'"url"\s*:\s*"([^"]+)"')
TOPIC_LINK_HINT_RE = re.compile(r"/forum/", re.IGNORECASE)


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


def remove_query_param(url: str, key: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query.pop(key, None)
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


def normalize_url(url: str) -> str:
    return strip_fragment(url).strip()


def get_topic_base_url(url: str) -> str:
    url = normalize_url(url)
    url = remove_query_param(url, "pg")
    return url


def get_main_page_index(url: str) -> int:
    """
    0-alapú pg indexet ad vissza.
    Első oldal: 0
    """
    pg = extract_query_param(url, "pg")
    if pg and pg.isdigit():
        return int(pg)
    return 0


def get_main_human_page(url: str) -> int:
    return get_main_page_index(url) + 1


def get_topic_page_index_from_url(url: str) -> Optional[int]:
    pg = extract_query_param(url, "pg")
    if pg and pg.isdigit():
        return int(pg)
    return None


def get_topic_human_page_from_url(url: str) -> Optional[int]:
    idx = get_topic_page_index_from_url(url)
    if idx is None:
        return None
    return idx + 1


def parse_topic_displayed_page_info(html: str, current_url: str) -> Tuple[int, Optional[int]]:
    """
    Visszaadja:
      - jelenlegi oldal száma 1-alapon
      - összes oldal száma, ha felismerhető
    """
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text("\n", strip=True))

    m = re.search(r"\b(\d+)\s*/\s*(\d+)\b", page_text)
    if m:
        cur = parse_int_from_text(m.group(1)) or 1
        total = parse_int_from_text(m.group(2))
        return cur, total

    url_page = get_topic_human_page_from_url(current_url)
    if url_page is not None:
        return url_page, None

    # Ha nincs pg param, a topic megnyitásakor jellemzően az utolsó oldal nyílik meg,
    # de biztosabbat nem tudunk. Ilyenkor legalább 1-et adunk vissza.
    return 1, None


# -----------------------------
# Fájl / output kezelés
# -----------------------------

def ensure_dirs(base_output: Path) -> Tuple[Path, Path, Path]:
    forum_dir = base_output / "hobbielektronika"
    topics_dir = forum_dir / "topics"
    forum_dir.mkdir(parents=True, exist_ok=True)
    topics_dir.mkdir(parents=True, exist_ok=True)

    visited_file = forum_dir / "visited.txt"
    if not visited_file.exists():
        visited_file.write_text("", encoding="utf-8")

    return forum_dir, topics_dir, visited_file


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
    """
    Visszaadja:
      - utolsó comment_id
      - utolsó komment URL
      - meglévő kommentek száma
    """
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

    last_comment_id = comment_ids[-1] if comment_ids else None
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
            "rights": "hobbielektronika.hu fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "detected_total_comments": topic_meta.get("detected_total_comments"),
                "fetched_page": topic_meta.get("fetched_page"),
                "fetched_total_pages": topic_meta.get("fetched_total_pages"),
            },
            "origin": "hobbielektronika_forum",
        },
        "origin": "hobbielektronika_forum",
    }

    header_json = json.dumps(header_obj, ensure_ascii=False, indent=2)
    if not header_json.endswith("}"):
        raise RuntimeError("Hibás header JSON generálás.")

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
            viewport={"width": 1600, "height": 2200},
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

def is_probable_topic_link(a: Tag) -> bool:
    href = (a.get("href") or "").strip()
    text = clean_text(a.get_text(" ", strip=True))

    if not href or not text:
        return False
    if href.startswith("javascript:"):
        return False
    if href.startswith("#"):
        return False
    if not TOPIC_LINK_HINT_RE.search(href):
        return False

    lower = text.lower()
    blacklist = {
        "ok",
        "belépés",
        "regisztráció",
        "elfelejtett jelszó",
        "új téma nyitása",
        "keresés",
    }
    if lower in blacklist:
        return False

    return True


def parse_topic_rows_from_main_page(html: str, page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    topics: List[Dict] = []
    seen = set()

    all_rows = soup.select("tr")
    print(f"[DEBUG] Főoldali tr sorok száma: {len(all_rows)}")

    for row in all_rows:
        row_text = clean_text(row.get_text(" ", strip=True))
        if "Válaszok:" not in row_text:
            continue

        title_a = None
        for a in row.select("a[href]"):
            if is_probable_topic_link(a):
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

        comment_count = None
        view_count = None
        opener = None
        last_user = None
        last_message = None

        m_replies = re.search(r"Válaszok:\s*([0-9 .]+)", row_text, flags=re.I)
        if m_replies:
            comment_count = parse_int_from_text(m_replies.group(1))

        m_views = re.search(r"Olvasva:\s*([0-9 .]+)", row_text, flags=re.I)
        if m_views:
            view_count = parse_int_from_text(m_views.group(1))

        tds = row.find_all("td")
        if len(tds) >= 3:
            opener = clean_text(tds[1].get_text(" ", strip=True))
            last_user = clean_text(tds[2].get_text(" ", strip=True))
            last_message = last_user

        topics.append(
            {
                "title": topic_title,
                "url": topic_url_norm,
                "comment_count": comment_count,
                "view_count": view_count,
                "opener": opener,
                "last_user": last_user,
                "last_message": last_message,
            }
        )

    return topics


def get_main_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    current_idx = get_main_page_index(current_url)
    wanted_next_idx = current_idx + 1

    candidates: List[Tuple[int, str]] = []

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        full = urljoin(current_url, href)
        if not full.startswith(MAIN_FORUM_URL):
            continue

        pg = extract_query_param(full, "pg")
        if pg and pg.isdigit():
            candidates.append((int(pg), full))

    # Először a pontosan következő indexű oldalt keressük
    for idx, full in candidates:
        if idx == wanted_next_idx:
            return full

    # Ha nincs, vegyük a legkisebb nagyobbat
    bigger = sorted((idx, full) for idx, full in candidates if idx > current_idx)
    if bigger:
        return bigger[0][1]

    # Végső fallback
    return set_query_param(MAIN_FORUM_URL, "pg", str(wanted_next_idx))


# -----------------------------
# Topicoldal parsing
# -----------------------------

def extract_topic_title(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "div#mainContent > table h1",
        "div#mainContent h1",
        "title",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            if text:
                text = re.sub(r"^\s*Fórum\s*»\s*", "", text, flags=re.I)
                return text

    # Gyakori, hogy a felső zöld fejléc tartalmazza a címet
    for node in soup.select("td, div, span"):
        txt = clean_text(node.get_text(" ", strip=True))
        if txt and len(txt) >= 3 and txt == fallback:
            return txt

    return fallback


def extract_topic_meta(html: str, topic_url: str) -> Dict:
    current_human_page, total_pages = parse_topic_displayed_page_info(html, topic_url)
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text("\n", strip=True))

    detected_total_comments = None
    all_boxes = find_comment_containers(soup)
    if all_boxes:
        detected_total_comments = len(all_boxes)

    return {
        "url": get_topic_base_url(topic_url),
        "detected_total_comments": detected_total_comments,
        "fetched_page": current_human_page,
        "fetched_total_pages": total_pages,
        "page_text_excerpt": page_text[:400],
    }


def find_comment_containers(soup: BeautifulSoup) -> List[Tag]:
    """
    A hozzászólások tipikusan ilyenek:
      <div class="box" id="pd2630220"> ... </div>
    """
    containers = []

    for div in soup.select("div.box[id]"):
        div_id = (div.get("id") or "").strip()
        if re.match(r"^pd\d+$", div_id):
            containers.append(div)

    return containers


def parse_comment_index(text: str) -> Tuple[Optional[int], Optional[int]]:
    text = clean_text(text)
    m = re.search(r"\((\d+)\s*/\s*(\d+)\)", text)
    if not m:
        return None, None
    return parse_int_from_text(m.group(1)), parse_int_from_text(m.group(2))


def extract_comment_header_parts(box: Tag) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Próbálja kiszedni:
      - author
      - parent_author
      - date
    """
    header_text = ""

    header_candidates = [
        box.select_one(".boxhp"),
        box.select_one(".boxh"),
        box.select_one(".boxhead"),
    ]
    for node in header_candidates:
        if node:
            header_text = clean_text(node.get_text(" ", strip=True))
            if header_text:
                break

    if not header_text:
        # fallback: vegyük a box elején lévő szöveget body nélkül
        header_text = clean_text(box.get_text(" ", strip=True))

    author = None
    parent_author = None
    date_text = None

    # Dátum a végén pl. "Okt 29, 2024"
    m_date = re.search(r"([A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]{3,}\s+\d{1,2},\s+\d{4})\s*$", header_text)
    if m_date:
        date_text = clean_text(m_date.group(1))
        header_wo_date = clean_text(header_text[:m_date.start()])
    else:
        header_wo_date = header_text

    header_wo_date = re.sub(r"\(\s*»\s*\)\s*$", "", header_wo_date).strip()
    header_wo_date = re.sub(r"^\(\#\)\s*", "", header_wo_date).strip()

    m_reply = re.match(r"^(.*?)\s+válasza\s+(.*?)\s+hozzászólására\s*$", header_wo_date, flags=re.I)
    if m_reply:
        author = clean_text(m_reply.group(1))
        parent_author = clean_text(m_reply.group(2))
    else:
        # Fallback: első link rendszerint a szerző
        links = [clean_text(a.get_text(" ", strip=True)) for a in box.select("a[href]")]
        links = [x for x in links if x]
        if links:
            author = links[0]

        if not author:
            # végső fallback: első szókapcsolat a headerből
            m_author = re.match(r"^(.*?)(?:\s+\(|$)", header_wo_date)
            if m_author:
                author = clean_text(m_author.group(1))

    return author, parent_author, date_text


def extract_comment_from_container(container: Tag, topic_page_url: str) -> Optional[Dict]:
    div_id = (container.get("id") or "").strip()
    m_id = re.match(r"^pd(\d+)$", div_id)
    comment_id = m_id.group(1) if m_id else None

    author, parent_author, date_text = extract_comment_header_parts(container)

    if not author:
        author = "ismeretlen"

    message_node = None
    for selector in [".boxpc", ".boxp", ".content", ".text"]:
        node = container.select_one(selector)
        if node:
            message_node = node
            break

    body = clean_text(message_node.get_text("\n", strip=True)) if message_node else ""

    whole_text = clean_text(container.get_text("\n", strip=True))
    comment_no, total_no = parse_comment_index(whole_text)

    classes = " ".join(container.get("class", []))
    is_offtopic = False
    if "offtopic" in classes.lower() or re.search(r"\bofftopic\b", whole_text, flags=re.I):
        is_offtopic = True

    comment_url = strip_fragment(topic_page_url)
    if comment_id:
        comment_url = f"{comment_url}#comment-{comment_id}"

    return {
        "comment_id": comment_id,
        "author": author,
        "date": date_text,
        "rating": None,
        "parent_author": parent_author,
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
    text = clean_text(comment.get("data") or "")[:300]
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


def topic_page_looks_closed_or_unavailable(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    text = clean_text(soup.get_text("\n", strip=True)).lower()

    signals = [
        "a téma lezárásra került",
        "nem tudsz hozzászólni",
        "a téma hozzászólásai áthelyezésre kerültek ide",
    ]

    if any(sig in text for sig in signals) and not topic_has_any_comment_container(html):
        return True

    return False


def get_topic_prev_page_url(html: str, current_url: str) -> Optional[str]:
    """
    Visszalép az előző oldalra.
    A topicot tipikusan az utolsó oldalról nyitja meg a fórum,
    ezért visszafelé kell mennünk az elsőig.
    """
    soup = BeautifulSoup(html, "html.parser")
    current_human_page, _ = parse_topic_displayed_page_info(html, current_url)

    if current_human_page <= 1:
        return None

    wanted_prev_human = current_human_page - 1
    wanted_prev_idx = wanted_prev_human - 1

    candidates: List[Tuple[int, str]] = []

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        full = urljoin(current_url, href)
        if get_topic_base_url(full) != get_topic_base_url(current_url):
            continue

        pg = extract_query_param(full, "pg")
        if pg and pg.isdigit():
            candidates.append((int(pg), full))

    for idx, full in candidates:
        if idx == wanted_prev_idx:
            return full

    smaller = sorted((idx, full) for idx, full in candidates if idx < current_human_page - 1)
    if smaller:
        return smaller[-1][1]

    # fallback
    return set_query_param(get_topic_base_url(current_url), "pg", str(wanted_prev_idx))


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
        "tags": ["offtopic"] if c.get("is_offtopic") else [],
        "extra": {
            "comment_id": c.get("comment_id"),
            "parent_author": c.get("parent_author"),
            "index": c.get("index"),
            "index_total": c.get("index_total"),
            "is_offtopic": c.get("is_offtopic"),
        },
    }


# -----------------------------
# Topic scrape
# -----------------------------

def parse_resume_page_from_comment_url(url: str) -> Optional[int]:
    if not url:
        return None
    human = get_topic_human_page_from_url(url)
    if human is not None:
        return human

    # ha a mentett komment URL-ben nem volt pg, akkor ez valószínűleg az utolsó oldalról jött
    return None


def scrape_topic(
    fetcher: BrowserFetcher,
    topic_title: str,
    topic_url: str,
    topic_file: Path,
    delay: float,
) -> int:
    existing_comments = 0
    resume_human_page: Optional[int] = None
    resume_after_comment_id = None
    need_init_file = True

    if topic_file.exists():
        if is_stream_json_finalized(topic_file):
            print("[INFO] A topic fájl már lezárt JSON, ezt késznek vesszük.")
            return count_existing_comments_in_stream_file(topic_file)

        last_comment_id, last_comment_url, existing_comments = get_last_written_comment_info(topic_file)
        if last_comment_url:
            resume_human_page = parse_resume_page_from_comment_url(last_comment_url)
            resume_after_comment_id = last_comment_id
            need_init_file = False
            print(
                f"[INFO] Meglévő félkész topicfájl, folytatás: "
                f"page={resume_human_page if resume_human_page else 'ismeretlen/utolsó'}, "
                f"utolsó comment_id={resume_after_comment_id}, meglévő kommentek={existing_comments}"
            )

    first_fetch_url = topic_url
    if resume_human_page is not None:
        first_fetch_url = set_query_param(get_topic_base_url(topic_url), "pg", str(resume_human_page - 1))

    print(f"[INFO] Topic megnyitása: {topic_title}")
    current_url, html = fetcher.fetch(first_fetch_url, wait_ms=int(delay * 1000))

    if topic_page_looks_closed_or_unavailable(html):
        print("[INFO] A topic lezárt / nem elérhető kommentekkel. Nem mentek kommentet.")
        return 0

    if not topic_has_any_comment_container(html):
        print("[INFO] Nem található hozzászólás ezen a topicoldalon. Nem mentek kommentet.")
        return 0

    resolved_title = extract_topic_title(html, topic_title)
    topic_meta = extract_topic_meta(html, current_url)

    if need_init_file:
        write_topic_stream_header(topic_file, resolved_title, topic_meta, topic_url)
        print(f"[INFO] Új streamelt topicfájl létrehozva: {topic_file}")

    current_human_page, total_pages = parse_topic_displayed_page_info(html, current_url)
    total_downloaded = existing_comments
    has_existing_comments = existing_comments > 0

    seen_page_fingerprints: Set[str] = set()
    first_page_after_resume = True

    while True:
        print(
            f"[INFO] Kommentoldal #{current_human_page}"
            + (f"/{total_pages}" if total_pages else "")
            + f": {current_url}"
        )

        page_comments = parse_comments_from_topic_page(html, current_url)

        if not page_comments:
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
                    f"[INFO] Resume szűrés: ezen az oldalon {original_len} kommentből "
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

        prev_url = get_topic_prev_page_url(html, current_url)
        if not prev_url:
            print("[INFO] Elértem az első kommentoldalt ennél a topicnál.")
            break

        print(f"[INFO] Előző kommentoldal jelölt: {prev_url}")

        fetched_prev_url, prev_html = fetcher.fetch(prev_url, wait_ms=int(delay * 1000))
        if not topic_has_any_comment_container(prev_html):
            print("[INFO] Az előző oldal már nem tartalmaz kommenteket, megállok.")
            break

        prev_comments = parse_comments_from_topic_page(prev_html, fetched_prev_url)
        prev_fingerprint = build_page_fingerprint(prev_comments)
        print(f"[DEBUG] Előző oldal fingerprint: {prev_fingerprint}")

        if prev_fingerprint == current_fingerprint:
            print("[INFO] Az előző oldal kommentjei megegyeznek a mostanival, ezért itt vége a topicnak.")
            break

        if prev_fingerprint in seen_page_fingerprints:
            print("[INFO] Az előző oldal tartalma már korábban szerepelt, ezért itt vége a topicnak.")
            break

        current_url = fetched_prev_url
        html = prev_html
        current_human_page, total_pages = parse_topic_displayed_page_info(html, current_url)

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
    forum_dir, topics_dir, visited_file = ensure_dirs(base_output)

    visited_topics = {
        normalize_topic_url_for_visited(x)
        for x in load_visited(visited_file)
    }

    # start_page itt emberi 1-alapú oldal
    start_idx = max(0, start_page - 1)
    current_url = MAIN_FORUM_URL if start_idx == 0 else set_query_param(MAIN_FORUM_URL, "pg", str(start_idx))
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

                if total_downloaded > 0:
                    print(f"[INFO] Topic mentve: {topic_json_path}")
                else:
                    print(f"[INFO] Topic komment nélkül / lezárva, csak visitedbe került: {topic_url_norm}")

                print(f"[INFO] Topic visitedbe írva: {topic_url_norm}")

            except Exception as e:
                print(f"[WARN] Hiba topic feldolgozás közben: {topic_url} | {e}")

        processed_main_pages += 1

        next_url = get_main_next_page_url(html, final_url)
        if not next_url:
            print("[INFO] Nincs több főoldali topiclista oldal.")
            break

        next_idx = get_main_page_index(next_url)
        next_page_no = next_idx + 1
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
        description="hobbielektronika.hu fórum scraper Playwright + BeautifulSoup alapon, streamelt komment-append módban"
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre a hobbielektronika/ mappa.",
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
        help="A fórum főoldali lapozásának kezdő oldala (1-alapú).",
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
    # Példa:
    # python hobbye_scraper.py --output ./hobbielektronika --headed