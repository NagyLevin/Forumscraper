#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://forum.index.hu"
MAIN_FORUM_URL = "https://forum.index.hu/Topic/showTopicList"

SHOW_TOPIC_LIST_RE = re.compile(r"/Topic/showTopicList(?:\?|$)", re.IGNORECASE)
SHOW_ARTICLE_RE = re.compile(r"/Article/showArticle2?(?:\?|$)", re.IGNORECASE)


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


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": BASE_URL + "/",
        }
    )
    return session


def fetch(session: requests.Session, url: str, timeout: int = 60) -> Tuple[str, str]:
    resp = session.get(url, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or resp.encoding or "utf-8"
    return resp.url, resp.text


def ensure_dirs(base_output: Path) -> Path:
    index_dir = base_output / "index"
    index_dir.mkdir(parents=True, exist_ok=True)
    return index_dir


def ensure_visited_file(folder: Path) -> Path:
    visited = folder / "visited_topics.txt"
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


def append_visited(visited_file: Path, topic_url: str) -> None:
    with visited_file.open("a", encoding="utf-8") as f:
        f.write(topic_url.strip() + "\n")


def make_json_serializable(obj):
    return json.loads(json.dumps(obj, ensure_ascii=False))


def extract_query_param(url: str, key: str) -> Optional[str]:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    vals = query.get(key)
    if vals:
        return vals[0]
    return None


def set_query_params(url: str, updates: Dict[str, str]) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    for k, v in updates.items():
        query[k] = [str(v)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )


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


def build_comment_record(
    author: str,
    content: str,
    date_text: Optional[str],
    url: str,
    likes: Optional[int] = None,
    dislikes: Optional[int] = None,
    score: Optional[int] = None,
    extra: Optional[dict] = None,
) -> Dict:
    return {
        "authors": [split_name_like_person(author)] if author else [],
        "data": content,
        "likes": likes,
        "dislikes": dislikes,
        "score": score,
        "date": date_text,
        "url": url,
        "language": "hu",
        "tags": [],
        "extra": extra or {},
    }


def parse_main_categories(html: str, page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[Dict] = []

    containers = soup.select("div.container")
    print(f"[DEBUG] Főoldali konténerek száma: {len(containers)}")

    for idx, container in enumerate(containers, start=1):
        title_p = container.select_one("p.title")
        links_p = container.select_one("p.links")
        body_p = container.select_one("p.body")

        if not title_p or not links_p:
            continue

        title_a = title_p.select_one("a")
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

        if not sublinks:
            continue

        body_text = clean_text(body_p.get_text(" ", strip=True)) if body_p else ""

        print(
            f"[DEBUG] Fórumcsoport #{idx}: {category_title} | kis linkek: {len(sublinks)}"
        )

        results.append(
            {
                "category_title": category_title,
                "category_url": category_url,
                "category_description": body_text,
                "subforums": sublinks,
            }
        )

    return results


def parse_topic_rows_from_subforum_page(html: str, page_url: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    topics: List[Dict] = []
    seen = set()

    rows = soup.select("table.topiclist tr")
    if not rows:
        rows = soup.select("table tr")

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

        creator = None
        last_user = None
        raw_cells = row.find_all("td")
        if len(raw_cells) >= 4:
            creator = clean_text(raw_cells[1].get_text(" ", strip=True))
            last_user = clean_text(raw_cells[2].get_text(" ", strip=True))

        count_text = clean_text(raw_cells[3].get_text(" ", strip=True)) if len(raw_cells) >= 4 else ""
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


def parse_subforum_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
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


def get_subforum_next_page_url(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    nav_candidates = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        full = urljoin(current_url, href)
        if not SHOW_TOPIC_LIST_RE.search(full):
            continue

        txt = clean_text(a.get_text(" ", strip=True))
        img = a.select_one("img[alt]")
        alt = clean_text(img.get("alt", "")) if img else ""

        if alt in {"10>", ">", ">>"} or txt in {">", ">>"}:
            nav_candidates.append(full)

    if nav_candidates:
        return nav_candidates[0]

    current_nt_start = extract_query_param(current_url, "nt_start")
    current_t = extract_query_param(current_url, "t")
    if current_t is not None:
        current_start_int = int(current_nt_start) if current_nt_start and current_nt_start.isdigit() else 0

        possible = []
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

        if possible:
            possible.sort(key=lambda x: x[0])
            return possible[0][1]

    return None


def extract_topic_title(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "meta[property='og:title']",
        "div#mainspacer h1",
        "h1",
        "title",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue

        if selector.startswith("meta"):
            text = clean_text(node.get("content", ""))
        else:
            text = clean_text(node.get_text(" ", strip=True))

        text = re.sub(r"\s*-\s*Index Fórum.*$", "", text, flags=re.I)
        if text:
            return text

    return fallback


def extract_topic_meta(html: str, topic_url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text("\n", strip=True))

    opener = None
    opened_date = None
    post_count = None
    comment_count = None

    m = re.search(
        r"Nyitotta:\s*(.+?),\s*([0-9]{4}\.[0-9]{2}\.[0-9]{2}\s+[0-9]{2}:[0-9]{2})\s*\|\s*Hozzászólások:\s*([0-9]+)\s*\|\s*Hozzászólók:\s*([0-9]+)",
        page_text,
        flags=re.I,
    )
    if m:
        opener = clean_text(m.group(1))
        opened_date = clean_text(m.group(2))
        post_count = parse_int_from_text(m.group(3))
        comment_count = parse_int_from_text(m.group(4))

    return {
        "opener": opener,
        "opened_date": opened_date,
        "post_count": post_count,
        "commenter_count": comment_count,
        "url": topic_url,
    }


def find_comment_tables(soup: BeautifulSoup) -> List[Tag]:
    tables = soup.select("table.art")
    if tables:
        return tables

    tables = []
    for table in soup.select("table"):
        txt = clean_text(table.get_text(" ", strip=True))
        if txt and len(txt) > 20:
            if "előzmény" in txt.lower() or "új hozzászólás" in txt.lower():
                tables.append(table)
    return tables


def extract_comment_from_table(table: Tag, topic_page_url: str) -> Optional[Dict]:
    full_text = clean_text(table.get_text("\n", strip=True))
    if not full_text:
        return None

    author = None
    date_text = None
    likes = None
    dislikes = None
    score = None
    comment_id = None
    comment_url = topic_page_url
    body = ""

    header_text = ""
    header_row = table.select_one("tr.art_h")
    if header_row:
        header_text = clean_text(header_row.get_text(" ", strip=True))
    else:
        first_tr = table.select_one("tr")
        if first_tr:
            header_text = clean_text(first_tr.get_text(" ", strip=True))

    author_candidates = []
    if header_row:
        for a in header_row.select("a[href], b, span"):
            txt = clean_text(a.get_text(" ", strip=True))
            if txt:
                author_candidates.append(txt)

    for cand in author_candidates:
        if len(cand) > 1 and cand.lower() not in {
            "cc",
            "v",
            "új hozzászólás",
            "előzmény",
        }:
            if not re.fullmatch(r"-?\d+", cand):
                author = cand
                break

    if not author:
        m_author = re.match(r"([^\d][^#|]{1,80}?)\s+(?:cc\s+)?(?:\d+\s+)?(?:órája|perce|napja|hete|hónapja|éve)", header_text, flags=re.I)
        if m_author:
            author = clean_text(m_author.group(1))

    if not author:
        links = table.select("a[href]")
        for a in links:
            txt = clean_text(a.get_text(" ", strip=True))
            href = a.get("href", "")
            if txt and txt.lower() not in {"előzmény", "új hozzászólás"} and "/Article/" not in href:
                author = txt
                break

    date_patterns = [
        r"\b\d+\s+perce\b",
        r"\b\d+\s+órája\b",
        r"\b\d+\s+napja\b",
        r"\b\d+\s+hete\b",
        r"\b\d+\s+hónapja\b",
        r"\b\d+\s+éve\b",
        r"\b[0-9]{4}\.[0-9]{2}\.[0-9]{2}\.? ?[0-9]{0,2}:?[0-9]{0,2}\b",
    ]
    for pat in date_patterns:
        m = re.search(pat, header_text, flags=re.I)
        if m:
            date_text = clean_text(m.group(0))
            break

    header_links_text = " ".join(clean_text(a.get_text(" ", strip=True)) for a in table.select("a[href]"))
    nums = re.findall(r"(?<![#\d])-?\d+(?!\d)", header_links_text + " " + header_text)
    small_nums = []
    for n in nums:
        try:
            iv = int(n)
            if -5000 <= iv <= 5000:
                small_nums.append(iv)
        except ValueError:
            pass

    if small_nums:
        negatives = [x for x in small_nums if x < 0]
        positives = [x for x in small_nums if x > 0]
        zeros = [x for x in small_nums if x == 0]

        dislikes = abs(negatives[0]) if negatives else 0
        likes = positives[-1] if positives else 0
        score = likes - dislikes
        if not positives and not negatives and zeros:
            likes = 0
            dislikes = 0
            score = 0

    id_candidates = re.findall(r"\b\d{4,}\b", header_text)
    if id_candidates:
        comment_id = id_candidates[-1]

    body_candidates: List[str] = []

    for node in table.select("div.art_t, div.art_body, td[colspan='3'] div, p"):
        txt = clean_text(node.get_text("\n", strip=True))
        if not txt:
            continue
        if txt == header_text:
            continue
        if txt.lower() == "előzmény":
            continue
        body_candidates.append(txt)

    if body_candidates:
        uniq = []
        seen = set()
        for item in body_candidates:
            if item not in seen:
                uniq.append(item)
                seen.add(item)
        body = "\n\n".join(uniq).strip()

    if not body:
        lines = [line.strip() for line in full_text.splitlines() if line.strip()]
        if len(lines) >= 2:
            body = "\n".join(lines[1:]).strip()

    if comment_id:
        comment_url = topic_page_url.split("#")[0] + f"#msg{comment_id}"

    if not body:
        return None

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
            f"| szerző={parsed['author']} | like={parsed.get('likes')} "
            f"| dislike={parsed.get('dislikes')} | preview={preview}"
        )
        comments.append(parsed)

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
        return candidates[0]

    current_start = extract_query_param(current_url, "na_start")
    current_t = extract_query_param(current_url, "t")
    current_article_id = extract_query_param(current_url, "a")
    current_start_int = int(current_start) if current_start and current_start.isdigit() else 0

    possible = []
    for a in soup.select("a[href]"):
        full = urljoin(current_url, a.get("href", ""))
        if not SHOW_ARTICLE_RE.search(full):
            continue
        t_val = extract_query_param(full, "t")
        a_val = extract_query_param(full, "a")
        na_start = extract_query_param(full, "na_start")
        if t_val == current_t and a_val == current_article_id and na_start and na_start.isdigit():
            na_start_int = int(na_start)
            if na_start_int > current_start_int:
                possible.append((na_start_int, full))

    if possible:
        possible.sort(key=lambda x: x[0])
        return possible[0][1]

    return None


def topic_file_path(subforum_dir: Path, topic_title: str) -> Path:
    return subforum_dir / f"{sanitize_filename(topic_title)}.txt"


def save_topic_json(topic_file: Path, payload: Dict) -> None:
    topic_file.write_text(
        json.dumps(make_json_serializable(payload), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def scrape_topic(
    session: requests.Session,
    topic_title: str,
    topic_url: str,
    delay: float,
) -> Dict:
    print(f"[INFO] Topic megnyitása: {topic_title}")
    current_url, html = fetch(session, topic_url)
    time.sleep(delay)

    resolved_title = extract_topic_title(html, topic_title)
    topic_meta = extract_topic_meta(html, current_url)

    all_comments: List[Dict] = []
    seen_comment_keys: Set[str] = set()
    page_no = 1

    while True:
        print(f"[INFO] Kommentoldal #{page_no}: {current_url}")
        page_comments = parse_comments_from_topic_page(html, current_url)

        for item in page_comments:
            key = f"{item.get('comment_id') or ''}::{item.get('author') or ''}::{item.get('data')[:80]}"
            if key in seen_comment_keys:
                continue
            seen_comment_keys.add(key)
            all_comments.append(item)

        next_url = get_topic_next_page_url(html, current_url)
        if not next_url or next_url == current_url:
            print("[INFO] Nincs több kommentoldal ennél a topicnál.")
            break

        print(f"[INFO] Következő kommentoldal: {next_url}")
        current_url, html = fetch(session, next_url)
        time.sleep(delay)
        page_no += 1

    opener = topic_meta.get("opener") or ""
    first_author_list = [split_name_like_person(opener)] if opener else []

    payload = {
        "title": resolved_title,
        "authors": first_author_list,
        "data": {
            "content": resolved_title,
            "likes": None,
            "dislikes": None,
            "score": None,
            "date": topic_meta.get("opened_date"),
            "url": topic_meta.get("url"),
            "language": "hu",
            "tags": [],
            "rights": "Index Fórum tartalom",
            "date_modified": now_iso(),
            "extra": {
                "opener_username": opener,
                "post_count": topic_meta.get("post_count"),
                "commenter_count": topic_meta.get("commenter_count"),
            },
            "origin": "index_forum",
        },
        "comments": [
            {
                "authors": [split_name_like_person(c["author"])] if c.get("author") else [],
                "data": c["data"],
                "likes": c.get("likes"),
                "dislikes": c.get("dislikes"),
                "score": c.get("score"),
                "date": c.get("date"),
                "url": c.get("url"),
                "language": "hu",
                "tags": [],
                "extra": {
                    "comment_id": c.get("comment_id"),
                },
            }
            for c in all_comments
        ],
        "origin": "index_forum",
    }

    return payload


def scrape_subforum(
    session: requests.Session,
    category_title: str,
    subforum_title: str,
    subforum_url: str,
    base_index_dir: Path,
    delay: float,
) -> None:
    category_dir = base_index_dir / sanitize_filename(category_title)
    subforum_dir = category_dir / sanitize_filename(subforum_title)
    subforum_dir.mkdir(parents=True, exist_ok=True)

    visited_file = ensure_visited_file(subforum_dir)
    visited_topics = load_visited(visited_file)

    print(f"\n[INFO] Alforum indul: {category_title} -> {subforum_title}")
    print(f"[INFO] Alforum URL: {subforum_url}")

    current_url = subforum_url
    page_no = 1

    while True:
        print(f"\n[INFO] Topiclista oldal #{page_no}: {current_url}")
        final_url, html = fetch(session, current_url)
        time.sleep(delay)

        resolved_subforum_title = parse_subforum_title(html)
        print(f"[DEBUG] Felismert alforum cím: {resolved_subforum_title}")

        topics = parse_topic_rows_from_subforum_page(html, final_url)
        print(f"[INFO] Talált topicok ezen az oldalon: {len(topics)}")

        for idx, topic in enumerate(topics, start=1):
            topic_title = topic["title"]
            topic_url = topic["url"]

            print(f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title}")
            if topic_url in visited_topics:
                print("[INFO] Már visitedben van, kihagyva.")
                continue

            topic_path = topic_file_path(subforum_dir, topic_title)

            try:
                payload = scrape_topic(
                    session=session,
                    topic_title=topic_title,
                    topic_url=topic_url,
                    delay=delay,
                )

                resolved_title = payload.get("title") or topic_title
                final_topic_path = topic_file_path(subforum_dir, resolved_title)

                if final_topic_path != topic_path and topic_path.exists():
                    topic_path.replace(final_topic_path)
                    topic_path = final_topic_path
                else:
                    topic_path = final_topic_path

                save_topic_json(topic_path, payload)
                append_visited(visited_file, topic_url)
                visited_topics.add(topic_url)

                print(f"[INFO] Topic mentve: {topic_path}")
                print(f"[INFO] Topic visitedbe írva: {topic_url}")

            except Exception as e:
                print(f"[WARN] Hiba topic feldolgozás közben: {topic_url} | {e}")

        next_url = get_subforum_next_page_url(html, final_url)
        if not next_url or next_url == final_url:
            print(f"[INFO] Nincs több topiclista oldal ennél az alforumnál: {subforum_title}")
            break

        print(f"[INFO] Következő topiclista oldalra lépek ({page_no + 1}. oldal): {next_url}")
        current_url = next_url
        page_no += 1


def scrape_main(
    session: requests.Session,
    output_dir: str,
    delay: float,
    only_category: Optional[str],
    only_subforum: Optional[str],
) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    index_dir = ensure_dirs(base_output)

    print(f"[INFO] Főoldal megnyitása: {MAIN_FORUM_URL}")
    final_url, html = fetch(session, MAIN_FORUM_URL)
    time.sleep(delay)

    categories = parse_main_categories(html, final_url)
    print(f"[INFO] Feldolgozandó fórumcsoportok száma: {len(categories)}")

    for cat_idx, cat in enumerate(categories, start=1):
        category_title = cat["category_title"]

        if only_category and only_category.lower() not in category_title.lower():
            continue

        print(f"\n[INFO] Fórumcsoport ({cat_idx}/{len(categories)}): {category_title}")

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
                scrape_subforum(
                    session=session,
                    category_title=category_title,
                    subforum_title=subforum_title,
                    subforum_url=subforum_url,
                    base_index_dir=index_dir,
                    delay=delay,
                )
            except Exception as e:
                print(f"[WARN] Hiba alforum feldolgozás közben: {subforum_url} | {e}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Index Fórum scraper: főoldal -> kis fórumlinkek -> topicok -> kommentek -> JSON .txt mentés."
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre az index mappa. Alapértelmezett: aktuális mappa.",
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
        help="Csak azokat a kis linkes alforumokat dolgozza fel, amelyek címében ez szerepel.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = make_session()

    try:
        scrape_main(
            session=session,
            output_dir=args.output,
            delay=args.delay,
            only_category=args.only_category,
            only_subforum=args.only_subforum,
        )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva felhasználó által.")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] Végzetes hiba: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()