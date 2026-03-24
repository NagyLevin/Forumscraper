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

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

BASE_URL = "https://www.agroinform.hu"
MAIN_FORUM_URL = "https://www.agroinform.hu/forum"

TOPIC_URL_RE = re.compile(
    r"^https?://(?:www\.)?agroinform\.hu/forum/[^?#]+/t\d+(?:/p/\d+)?(?:[?#].*)?$",
    re.I,
)
TOPIC_HREF_RE = re.compile(
    r"^/forum/[^?#]+/t\d+(?:/p/\d+)?(?:[?#].*)?$",
    re.I,
)

DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\b")
PAGE_PAIR_RE = re.compile(r"^\s*(\d+)\s*/\s*(\d+)\s*$")
COMMENT_COUNT_HEADER_RE = re.compile(r"(\d+)\s+hozzászólás", re.I)
COMMENT_ID_RE = re.compile(r'"comment_id"\s*:\s*(?:"([^"]+)"|(\d+)|null)')
COMMENT_URL_RE = re.compile(r'"url"\s*:\s*"([^"]+)"')


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_multiline_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    lines = [clean_text(x) for x in text.split("\n")]
    lines = [x for x in lines if x]
    return "\n".join(lines).strip()


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
    return url.split("#", 1)[0]


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


def normalize_topic_url_for_visited(url: str) -> str:
    return strip_fragment(url)


def comment_anchor_url(topic_page_url: str, comment_id: Optional[str]) -> str:
    base = strip_fragment(topic_page_url)
    if comment_id:
        return f"{base}#comment-{comment_id}"
    return base


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
            "url": strip_fragment(topic_url),
            "language": "hu",
            "tags": [],
            "rights": "agroinform.hu fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "topic_creator": topic_meta.get("topic_creator"),
                "detected_total_comments": topic_meta.get("detected_total_comments"),
                "detected_total_comment_pages": topic_meta.get("detected_total_comment_pages"),
                "comment_page_indicator_text": topic_meta.get("comment_page_indicator_text"),
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
            viewport={"width": 1700, "height": 2600},
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
        return final_url, html

    def html(self) -> Tuple[str, str]:
        try:
            self.page.wait_for_load_state("networkidle", timeout=5000)
        except PlaywrightTimeoutError:
            pass
        self.page.wait_for_timeout(800)
        return self.page.url, self.page.content()

    # --- KÖVETKEZŐ OLDAL TELJES, ABSZOLÚT URL-JÉNEK LEKÉRÉSE ---
    def get_next_page_url(self) -> Optional[str]:
        """Kikeresi a 'Következő oldal' gombot és visszaadja a böngésző által generált natív ABSZOLÚT URL-t."""
        script = """
        () => {
            const nextLink = document.querySelector('a[title*="Következő oldal"], a[rel="next"]');
            if (nextLink) {
                return nextLink.href; // A .href a böngésző által feloldott TELJES url-t adja vissza!
            }
            
            // Ha esetleg csak a képet találná meg
            const img = document.querySelector('img[alt="Következő"]');
            if (img && img.closest('a')) {
                return img.closest('a').href;
            }
            return null;
        }
        """
        try:
            url = self.page.evaluate(script)
            if url:
                return url
        except Exception:
            pass
        return None

    def open_topic_by_url(self, topic_url: str, wait_ms: int = 1500) -> Tuple[str, str]:
        return self.fetch(topic_url, wait_ms=wait_ms)

    def extract_topic_rows_current_page(self) -> List[Dict]:
        script = """
() => {
  const rows = Array.from(document.querySelectorAll('table tr'));
  const out = [];

  for (const row of rows) {
    const links = Array.from(row.querySelectorAll('a[href]'));
    let topicLink = null;

    for (const a of links) {
      const href = (a.getAttribute('href') || '').trim();
      if (/^\\/forum\\/[^?#]+\\/t\\d+(?:\\/p\\/\\d+)?(?:[?#].*)?$/i.test(href)) {
        topicLink = a;
        break;
      }
    }

    if (!topicLink) continue;

    const tds = Array.from(row.querySelectorAll('td')).map(td =>
      (td.innerText || td.textContent || '').replace(/\\u00a0/g, ' ').trim()
    );

    out.push({
      title: (topicLink.innerText || topicLink.textContent || '').replace(/\\u00a0/g, ' ').trim(),
      href: (topicLink.getAttribute('href') || '').trim(),
      row_text: (row.innerText || row.textContent || '').replace(/\\u00a0/g, ' ').trim(),
      cells: tds
    });
  }

  return out;
}
"""
        try:
            return self.page.evaluate(script)
        except Exception:
            return []

    def extract_page_pairs_current_page(self) -> List[str]:
        script = """
() => {
  const texts = [];
  const nodes = Array.from(document.querySelectorAll('select option, select, a, span, div'));
  for (const n of nodes) {
    const t = ((n.innerText || n.textContent || '') + '').replace(/\\u00a0/g, ' ').trim();
    if (/^\\d+\\s*\\/\\s*\\d+$/.test(t)) {
      texts.push(t);
    }
  }
  return Array.from(new Set(texts));
}
"""
        try:
            return self.page.evaluate(script)
        except Exception:
            return []

    def extract_topic_meta_current_page(self) -> Dict:
        script = """
() => {
  const titleNode = document.querySelector('h1') || document.querySelector('h2');
  const title = titleNode ? (titleNode.innerText || titleNode.textContent || '').replace(/\\u00a0/g, ' ').trim() : '';

  const bodyText = (document.body.innerText || document.body.textContent || '').replace(/\\u00a0/g, ' ');
  let creator = null;
  let createdAt = null;
  const m = bodyText.match(/Létrehozta:\\s*(.+?)\\s*,\\s*(\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2})/i);
  if (m) {
    creator = (m[1] || '').trim();
    createdAt = (m[2] || '').trim();
  }

  let totalComments = null;
  const m2 = bodyText.match(/(\\d+)\\s+hozzászólás/i);
  if (m2) {
    totalComments = parseInt(m2[1], 10);
  }

  const pagePairs = [];
  const nodes = Array.from(document.querySelectorAll('select option, select, a, span, div'));
  for (const n of nodes) {
    const t = ((n.innerText || n.textContent || '') + '').replace(/\\u00a0/g, ' ').trim();
    if (/^\\d+\\s*\\/\\s*\\d+$/.test(t)) {
      pagePairs.push(t);
    }
  }

  return {
    title,
    creator,
    createdAt,
    totalComments,
    pagePairs: Array.from(new Set(pagePairs))
  };
}
"""
        try:
            return self.page.evaluate(script)
        except Exception:
            return {
                "title": "",
                "creator": None,
                "createdAt": None,
                "totalComments": None,
                "pagePairs": [],
            }

    def extract_comments_current_page(self) -> List[Dict]:
        script = r"""
() => {
  function norm(s) {
    return (s || '').replace(/\u00a0/g, ' ').replace(/\r/g, '').trim();
  }

  function cleanLines(text) {
    let lines = String(text || '').split('\n').map(x => norm(x)).filter(Boolean);
    if (!lines.length) return '';
    lines = lines.filter(line => !/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/.test(line));
    lines = lines.filter(line => !/^#\d+$/.test(line));
    lines = lines.filter(line => line !== 'Válasz erre');
    lines = lines.filter(line => !/^Válasz\s+.+?\s+#\d+\.\s*hozzászólására$/i.test(line));
    return lines.join('\n').trim();
  }

  const cards = Array.from(document.querySelectorAll('div.card.card-comment .card-body'));
  const out = [];

  for (const card of cards) {
    const idNode = card.querySelector('div[id]');
    if (!idNode) continue;
    
    const cid = norm(idNode.getAttribute('id'));
    const authorNode = card.querySelector('.comment-author') || card.querySelector('a[href*="/profil/"]');
    let author = norm(authorNode ? (authorNode.innerText || authorNode.textContent || '') : '');
    author = author.replace(/^#\d+\s*/, '').trim();

    const dateNode = card.querySelector('div.d-block.d-sm-inline') || card.querySelector('.forum-items-header');
    let dateText = norm(dateNode ? (dateNode.innerText || dateNode.textContent || '') : '');
    const dm = dateText.match(/\b\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\b/);
    dateText = dm ? dm[0] : '';

    let parentAuthor = null;
    let parentCommentId = null;

    const links = Array.from(card.querySelectorAll('a'));
    for (const a of links) {
      const t = norm(a.innerText || a.textContent || '');
      const m = t.match(/^Válasz\s+(.+?)\s+#(\d+)\.\s*hozzászólására$/i);
      if (m) {
        parentAuthor = norm(m[1]);
        parentCommentId = norm(m[2]);
        break;
      }
    }

    const clone = card.cloneNode(true);
    const removeSelectors = ['.float-right.clearfix.header-bar', '.header-bar', '.comment-author', 'div.d-block.d-sm-inline', 'button', 'script', 'style', 'form'];
    
    for (const sel of removeSelectors) {
      for (const n of Array.from(clone.querySelectorAll(sel))) {
        n.remove();
      }
    }

    for (const a of Array.from(clone.querySelectorAll('a'))) {
      const txt = norm(a.innerText || a.textContent || '');
      if (txt === 'Válasz erre' || /^Válasz\s+.+?\s+#\d+\.\s*hozzászólására$/i.test(txt)) {
        a.remove();
      }
    }

    let raw = '';
    const innerIdNode = clone.querySelector('div[id]');
    if (innerIdNode) raw = innerIdNode.innerText || innerIdNode.textContent || '';
    else raw = clone.innerText || clone.textContent || '';

    let body = cleanLines(raw);

    if (author) {
      const bodyLines = body.split('\n').map(x => x.trim()).filter(Boolean);
      if (bodyLines.length && bodyLines[0] === author) {
        body = bodyLines.slice(1).join('\n').trim();
      }
    }

    if (!cid && !body) continue;

    out.push({
      comment_id: cid || null,
      author: author || 'ismeretlen',
      date: dateText || null,
      parent_author: parentAuthor,
      parent_comment_id: parentCommentId,
      data: body || ''
    });
  }

  return out;
}
"""
        try:
            return self.page.evaluate(script)
        except Exception:
            return []


def choose_best_page_indicator(page_pairs: List[str], prefer_large_total: bool = False) -> Dict[str, Optional[int]]:
    best_current = None
    best_total = None
    best_raw = None

    parsed: List[Tuple[int, int, str]] = []
    for t in page_pairs:
        m = PAGE_PAIR_RE.match(clean_text(t))
        if not m:
            continue
        a = int(m.group(1))
        b = int(m.group(2))
        parsed.append((a, b, clean_text(t)))

    if not parsed:
        return {"page_current": None, "page_total": None, "raw_text": None}

    if prefer_large_total:
        parsed.sort(key=lambda x: (x[1], x[0]), reverse=True)
        best_current, best_total, best_raw = parsed[0]
    else:
        parsed.sort(key=lambda x: (x[1], x[0]))
        best_current, best_total, best_raw = parsed[0]

    return {
        "page_current": best_current,
        "page_total": best_total,
        "raw_text": best_raw,
    }


def parse_topic_rows_from_dom_rows(rows: List[Dict]) -> List[Dict]:
    topics: List[Dict] = []
    seen = set()

    for row in rows:
        title = clean_text(row.get("title") or "")
        href = clean_text(row.get("href") or "")
        row_text = clean_multiline_text(row.get("row_text") or "")
        cells = [clean_multiline_text(x) for x in (row.get("cells") or [])]

        if not title:
            continue
        if title.lower() == "agroinform.hu fórumszabályzat":
            continue

        if not href:
            continue
        topic_url = href if href.startswith("http") else BASE_URL + href
        topic_url = normalize_topic_url_for_visited(topic_url)

        if topic_url in seen:
            continue
        seen.add(topic_url)

        comment_count = None
        starter = None
        starter_date = None
        last_user = None
        last_activity = None

        if len(cells) >= 4:
            starter = cells[1] if len(cells) > 1 else None
            last_user = cells[2] if len(cells) > 2 else None
            last_activity = cells[2] if len(cells) > 2 else None
            comment_count = parse_int_from_text(cells[-1])

        dates = DATE_RE.findall(row_text)
        if dates:
            starter_date = dates[0]

        topics.append(
            {
                "title": title,
                "url": topic_url,
                "comment_count": comment_count,
                "starter": starter,
                "starter_date": starter_date,
                "last_user": last_user,
                "last_activity": last_activity,
            }
        )

    return topics


def extract_topic_meta_from_fetcher(fetcher: BrowserFetcher, topic_url: str) -> Dict:
    meta = fetcher.extract_topic_meta_current_page()
    page_info = choose_best_page_indicator(meta.get("pagePairs") or [], prefer_large_total=True)

    return {
        "url": strip_fragment(topic_url),
        "topic_creator": clean_text(meta.get("creator") or "") or None,
        "created_at": clean_text(meta.get("createdAt") or "") or None,
        "detected_total_comments": meta.get("totalComments"),
        "detected_total_comment_pages": page_info.get("page_total"),
        "detected_current_comment_page": page_info.get("page_current"),
        "comment_page_indicator_text": page_info.get("raw_text"),
    }


def extract_topic_title_from_fetcher(fetcher: BrowserFetcher, fallback: str) -> str:
    meta = fetcher.extract_topic_meta_current_page()
    title = clean_text(meta.get("title") or "")
    return title or fallback


def parse_comments_from_fetcher(fetcher: BrowserFetcher, topic_page_url: str) -> Tuple[List[Dict], Dict[str, Optional[int]]]:
    raw_comments = fetcher.extract_comments_current_page()
    topic_meta = extract_topic_meta_from_fetcher(fetcher, topic_page_url)

    comments: List[Dict] = []
    for idx, c in enumerate(raw_comments, start=1):
        comment = {
            "comment_id": clean_text(c.get("comment_id") or "") or None,
            "author": clean_text(c.get("author") or "") or "ismeretlen",
            "date": clean_text(c.get("date") or "") or None,
            "rating": None,
            "parent_author": clean_text(c.get("parent_author") or "") or None,
            "parent_comment_id": clean_text(c.get("parent_comment_id") or "") or None,
            "url": comment_anchor_url(topic_page_url, clean_text(c.get("comment_id") or "") or None),
            "data": clean_multiline_text(c.get("data") or ""),
        }
        if not comment["comment_id"] and not comment["data"]:
            continue
        comments.append(comment)

    return comments, topic_meta


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


def scrape_topic(
    fetcher: BrowserFetcher,
    topic_title: str,
    topic_url: str,
    topic_file: Path,
    delay: float,
) -> int:
    existing_comments = 0
    resume_after_comment_id = None
    resume_from_url = None
    need_init_file = True

    if topic_file.exists():
        if is_stream_json_finalized(topic_file):
            print("[INFO] A topic fájl már lezárt JSON, ezt késznek vesszük.")
            return count_existing_comments_in_stream_file(topic_file)

        last_comment_id, last_comment_url, existing_comments = get_last_written_comment_info(topic_file)
        if last_comment_id:
            resume_after_comment_id = last_comment_id
            resume_from_url = last_comment_url or topic_url
            need_init_file = False
            print(
                f"[INFO] Meglévő félkész topicfájl, resume indulás | "
                f"utolsó comment_id={resume_after_comment_id} | "
                f"meglévő kommentek={existing_comments}"
            )

    entry_url = resume_from_url or topic_url
    print(f"\n[INFO] Téma megnyitása letöltésre: {topic_title}")
    
    fetcher.open_topic_by_url(entry_url, wait_ms=int(delay * 1000))

    resolved_title = extract_topic_title_from_fetcher(fetcher, topic_title)
    topic_meta = extract_topic_meta_from_fetcher(fetcher, fetcher.page.url)

    if need_init_file:
        write_topic_stream_header(topic_file, resolved_title, topic_meta, topic_url)

    total_downloaded = existing_comments
    has_existing_comments = existing_comments > 0

    seen_page_fingerprints: Set[str] = set()
    previous_page_fingerprint: Optional[str] = None
    resume_done = False

    while True:
        current_url = fetcher.page.url
        page_comments, current_meta = parse_comments_from_fetcher(fetcher, current_url)

        if not page_comments and total_downloaded > 0:
            print("[WARN] Ez az oldal nem tartalmaz kommentkártyákat, valószínűleg rossz helyre navgáltunk. Kilépés a témából.")
            break

        if resume_after_comment_id and not resume_done:
            original_len = len(page_comments)
            seen_last = False
            filtered: List[Dict] = []

            for c in page_comments:
                cid = str(c.get("comment_id") or "")
                if not seen_last:
                    if cid == str(resume_after_comment_id):
                        seen_last = True
                    continue
                filtered.append(c)

            if seen_last:
                page_comments = filtered
                resume_done = True
            else:
                current_fingerprint = build_page_fingerprint(page_comments)
                if current_fingerprint in seen_page_fingerprints:
                    print("[WARN] Végtelen ciklus gyanú resume közben. Kilépés.")
                    break
                seen_page_fingerprints.add(current_fingerprint)

                next_url = fetcher.get_next_page_url()
                if not next_url:
                    break
                
                fetcher.open_topic_by_url(next_url, wait_ms=int(delay * 1000))
                continue

        current_fingerprint = build_page_fingerprint(page_comments)

        if previous_page_fingerprint is not None and current_fingerprint == previous_page_fingerprint:
            print(f"[WARN] Végtelen ciklus elkerülése: A mostani oldal megegyezik az előzővel ({current_url}). Téma lezárása.")
            break

        if current_fingerprint in seen_page_fingerprints:
            print(f"[WARN] Már korábban látott tartalmat töltött be a böngésző ({current_url}). Téma lezárása.")
            break

        seen_page_fingerprints.add(current_fingerprint)

        added_on_this_page = 0
        for c in page_comments:
            item = comment_to_output_item(c)
            append_comment_to_stream_file(topic_file, item, has_existing_comments)
            has_existing_comments = True
            total_downloaded += 1
            added_on_this_page += 1

        current_page_no = current_meta.get('detected_current_comment_page') or "?"
        total_pages_no = current_meta.get('detected_total_comment_pages') or "?"
        total_comm = current_meta.get('detected_total_comments') or "?"
        
        print(f"[INFO] Oldal kigyűjtve | Állás: {current_page_no}/{total_pages_no} oldal | Összes db az oldalon feltüntetve: {total_comm} | Eddig leszedve: {total_downloaded}")

        # Tovább a következő URL-re, HA van Következő gomb!
        next_url = fetcher.get_next_page_url()
        
        if not next_url:
            print(f"[INFO] Téma letöltése befejeződött, a 'Következő oldal' gomb fizikailag elfogyott az oldalon.")
            break

        print(f"[DEBUG] Navigálás a következő kommentoldalra -> {next_url}")
        fetcher.open_topic_by_url(next_url, wait_ms=int(delay * 1000))

        previous_page_fingerprint = current_fingerprint

    finalize_stream_json(topic_file)
    print(f"[INFO] Topic feldolgozása sikeres. Fájl: {topic_file.name} | Összes kinyert komment: {total_downloaded}")
    return total_downloaded


def scrape_main(
    fetcher: BrowserFetcher,
    output_dir: str,
    delay: float,
    only_title: Optional[str],
    start_page: int,
    max_pages: Optional[int],
) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    _, topics_dir, visited_file = ensure_dirs(base_output)

    visited_topics = {normalize_topic_url_for_visited(x) for x in load_visited(visited_file)}

    fetcher.fetch(MAIN_FORUM_URL, wait_ms=int(delay * 1000))

    if start_page > 1:
        print(f"[INFO] Kezdő főoldali listához ugrás: {start_page}. oldal...")
        current_main_page_no = 1
        while current_main_page_no < start_page:
            next_url = fetcher.get_next_page_url()
            if not next_url:
                print("[WARN] Nem tudtam eljutni a kívánt start-page oldalra.")
                break
            
            fetcher.fetch(next_url, wait_ms=int(delay * 1000))
            pairs = fetcher.extract_page_pairs_current_page()
            page_info = choose_best_page_indicator(pairs, prefer_large_total=False)
            detected = page_info.get("page_current")
            if detected:
                current_main_page_no = int(detected)
            else:
                current_main_page_no += 1

    processed_main_pages = 0
    seen_main_page_fingerprints: Set[str] = set()

    while True:
        if max_pages is not None and processed_main_pages >= max_pages:
            print("[INFO] Elértem a feldolgozandó oldalak maximumát.")
            break

        dom_rows = fetcher.extract_topic_rows_current_page()
        topics = parse_topic_rows_from_dom_rows(dom_rows)

        pairs = fetcher.extract_page_pairs_current_page()
        page_info = choose_best_page_indicator(pairs, prefer_large_total=False)

        print(
            f"\n======================================================\n"
            f"[INFO] Főoldali Témalista Feldolgozása | Állás: {page_info.get('page_current')} / {page_info.get('page_total')}\n"
            f"======================================================"
        )

        if not topics:
            print("[INFO] Nem találtam elindítható témát az oldalon, leállok.")
            break

        topic_urls_for_fingerprint = "\n".join(t["url"] for t in topics)
        main_fp = hashlib.sha1(topic_urls_for_fingerprint.encode("utf-8")).hexdigest()

        if main_fp in seen_main_page_fingerprints:
            print("[WARN] Ezt a főoldali listát már láttam (véletlen visszalépés?), kilépek.")
            break
        seen_main_page_fingerprints.add(main_fp)

        for idx, topic in enumerate(topics, start=1):
            topic_title = topic["title"]
            topic_url = topic["url"]
            topic_url_norm = normalize_topic_url_for_visited(topic_url)

            if only_title and only_title.lower() not in topic_title.lower():
                continue

            if topic_url_norm in visited_topics:
                print(f"[SKIP] {topic_title} - Már maradéktalanul be lett fejezve korábban.")
                continue

            topic_json_path = topic_file_path(topics_dir, topic_title)

            try:
                # ITT MEGY BE A TÉMÁBA ÉS VÉGIGLAPOZZA!
                total_downloaded = scrape_topic(
                    fetcher=fetcher,
                    topic_title=topic_title,
                    topic_url=topic_url_norm,
                    topic_file=topic_json_path,
                    delay=delay,
                )

                # AMIKOR VÉGZETT AZ ÖSSZES OLDALLAL, CSAK AKKOR ÍRJA BE A VISITED-BE!
                append_visited(visited_file, topic_url_norm)
                visited_topics.add(topic_url_norm)
                print(f"[SUCCESS] {topic_title} hozzáadva a visited listához!")

            except Exception as e:
                print(f"[FATAL ERROR] Hiba a(z) '{topic_title}' feldolgozásánál: {e}")

            # Mivel kijött a témából, a böngészőt vissza kell vinnünk a főoldal megfelelő pontjára
            fetcher.fetch(MAIN_FORUM_URL, wait_ms=int(delay * 1000))
            target_page = page_info.get("page_current") or 1
            current_page_no = 1

            while current_page_no < int(target_page):
                next_url = fetcher.get_next_page_url()
                if not next_url:
                    break
                fetcher.fetch(next_url, wait_ms=int(delay * 1000))
                
                pairs = fetcher.extract_page_pairs_current_page()
                pi = choose_best_page_indicator(pairs, prefer_large_total=False)
                detected = pi.get("page_current")
                if detected:
                    current_page_no = int(detected)
                else:
                    current_page_no += 1

        processed_main_pages += 1

        # Ugrás a következő FŐOLDALI listára
        next_url = fetcher.get_next_page_url()
        if not next_url:
            print("[INFO] Nincs több lapozható oldal a főoldalon. Befejezés.")
            break
            
        fetcher.fetch(next_url, wait_ms=int(delay * 1000))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="agroinform.hu fórum letöltő - URL alapú hibamentes lapozással."
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.5,
        help="Várakozás másodpercben a hálózatnak.",
    )
    parser.add_argument(
        "--only-title",
        default=None,
        help="Csak ezeket a témákat dolgozza fel.",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="A fórum kezdőoldala ahonnan indít.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Ennyi főoldalt néz át maximum.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Böngésző GUI megjelenítése (Hibakereséshez).",
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
        print("\n[INFO] Leállítva.")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] Végzetes script hiba: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
                         # python agroinform_scraper.py --output ./agro --headed --start-page 1 --max-pages 1
