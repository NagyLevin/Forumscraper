import argparse
import re
import sys
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_LIST_URL = "https://prohardver.hu/temak/notebook/listaz.php"

HSZ_URL_RE = re.compile(
    r"^(?P<prefix>https?://[^#]+?/hsz_)(?P<start>\d+)-(?P<end>\d+)(?P<suffix>\.html)(?:#msg(?P<msg>\d+))?$",
    re.IGNORECASE,
)

RANGE_ONLY_RE = re.compile(r"^(\d+)-(\d+)$")


def build_list_url(offset: int) -> str:
    return BASE_LIST_URL if offset <= 0 else f"{BASE_LIST_URL}?offset={offset}"


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def sanitize_filename(name: str, max_len: int = 140) -> str:
    name = clean_text(name)
    if not name:
        return "ismeretlen_topic"

    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))

    for src, dst in [
        ("/", "-"),
        ("\\", "-"),
        (":", " -"),
        ("*", ""),
        ("?", ""),
        ('"', ""),
        ("<", "("),
        (">", ")"),
        ("|", "-"),
    ]:
        name = name.replace(src, dst)

    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"[. ]+$", "", name)

    if len(name) > max_len:
        name = name[:max_len].rstrip(" .")

    return name or "ismeretlen_topic"


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
            "Referer": "https://prohardver.hu/",
        }
    )
    return session


def fetch(session: requests.Session, url: str, timeout: int = 60) -> Tuple[str, str]:
    r = session.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or r.encoding or "utf-8"
    return r.url, r.text


def parse_topic_links(html: str, page_url: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        "div.thread-list h4 a[href*='/tema/']",
        "div.col.thread-title-thread h4 a[href*='/tema/']",
        "main h4 a[href*='/tema/']",
        "h4 a[href*='/tema/']",
    ]

    anchors = []
    for selector in selectors:
        anchors = soup.select(selector)
        if anchors:
            break

    topics: List[Tuple[str, str]] = []
    seen: Set[str] = set()

    for a in anchors:
        href = a.get("href")
        if not href:
            continue

        full_url = urljoin(page_url, href)
        if "/tema/" not in full_url or "/temak/" in full_url:
            continue

        title = clean_text(a.get_text(" ", strip=True))
        if not title:
            continue

        if full_url in seen:
            continue

        seen.add(full_url)
        topics.append((title, full_url))

    return topics[:100]


def page_has_messages_html(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    return len(soup.select("li.media[data-id]")) > 0


def is_404_html(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    title = clean_text(soup.title.get_text(" ", strip=True) if soup.title else "").lower()
    body_text = clean_text(soup.get_text("\n", strip=True)).lower()
    return "404" in title or "404 not found" in body_text or "a kért oldal nem létezik" in body_text


def extract_topic_title_from_html(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for selector in ["meta[property='og:title']", "title", "h1"]:
        node = soup.select_one(selector)
        if not node:
            continue

        if selector.startswith("meta"):
            text = clean_text(node.get("content", ""))
        else:
            text = clean_text(node.get_text(" ", strip=True))

        text = re.sub(r"\s*-\s*PROHARDVER!.*$", "", text, flags=re.I)
        if text:
            return text

    return fallback


def extract_author(post) -> str:
    header = post.select_one(".msg-header")
    if header:
        header_text = clean_text(header.get_text(" ", strip=True))
        m = re.match(r"#\d+\s+(.+?)\s*>\s*.+?#\d+", header_text)
        if m:
            author = clean_text(m.group(1))
            if author:
                return author

    for selector in [".msg-user", ".media-left"]:
        node = post.select_one(selector)
        if node:
            txt = clean_text(node.get_text("\n", strip=True))
            lines = [line.strip() for line in txt.splitlines() if line.strip()]
            if lines:
                return lines[0]

    return "ismeretlen"


def extract_comment_text(post) -> str:
    for selector in [".msg-content p.mgt0", ".msg-content", "p.mgt0"]:
        nodes = post.select(selector)
        if not nodes:
            continue

        parts = []
        for node in nodes:
            text = clean_text(node.get_text("\n", strip=True))
            if text:
                parts.append(text)

        if parts:
            joined = "\n".join(parts)
            joined = re.sub(r"\n{3,}", "\n\n", joined).strip()
            if joined:
                return joined

    return ""


def parse_comments_from_html(html: str) -> List[Tuple[str, str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    posts = soup.select("li.media[data-id]")
    results: List[Tuple[str, str, str]] = []

    print(f"[DEBUG] Talált li.media[data-id] elemek száma: {len(posts)}")

    for index, post in enumerate(posts, start=1):
        post_id = clean_text(post.get("data-id", ""))
        author = extract_author(post)
        comment = extract_comment_text(post)

        preview = comment[:120].replace("\n", " | ") if comment else "<üres>"
        print(f"[DEBUG] Poszt #{index} | data-id={post_id or '-'} | szerző={author} | preview={preview}")

        if not comment:
            continue

        results.append((post_id, author, comment))

    print(f"[DEBUG] Kinyert kommentek ezen az oldalon: {len(results)}")
    return results


def get_next_page_url_from_html(html: str, current_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "a[rel='next']",
        "a[title*='Következő blokk']",
        "li.nav-arrow a[rel='next']",
    ]

    for selector in selectors:
        a = soup.select_one(selector)
        if a and a.get("href"):
            return urljoin(current_url, a["href"])

    current_range = parse_hsz_range_from_url(current_url)
    if current_range:
        cur_start, cur_end = current_range
        for a in soup.select("a[href*='/hsz_']"):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(current_url, href)
            rng = parse_hsz_range_from_url(full)
            if not rng:
                continue
            start, end = rng
            if start < cur_start and end < cur_end:
                return full

    return None


def parse_hsz_range_from_url(url: str) -> Optional[Tuple[int, int]]:
    m = HSZ_URL_RE.match(url)
    if not m:
        return None
    return int(m.group("start")), int(m.group("end"))


def build_hsz_url_with_range(current_url: str, start: int, end: int) -> Optional[str]:
    m = HSZ_URL_RE.match(current_url)
    if not m:
        return None

    prefix = m.group("prefix")
    suffix = m.group("suffix")
    return f"{prefix}{start}-{end}{suffix}#msg{end + 1}"


def normalize_topic_base_url(topic_url: str) -> str:
    base = topic_url.split("#")[0].rstrip("/")
    base = re.sub(r"/friss\.html$", "", base, flags=re.I)
    base = re.sub(r"/hsz_\d+-\d+\.html$", "", base, flags=re.I)
    return base


def build_fresh_url_from_topic_url(topic_url: str) -> str:
    return f"{normalize_topic_base_url(topic_url)}/friss.html"


def build_hsz_url_from_topic_url(topic_url: str, start: int, end: int) -> str:
    base = normalize_topic_base_url(topic_url)
    return f"{base}/hsz_{start}-{end}.html#msg{end + 1}"


def build_prev_range_from_saved(saved_start: int, saved_end: int) -> Optional[Tuple[int, int]]:
    new_start = saved_start - 100
    new_end = saved_end - 100
    if new_start < 1 or new_end < 1:
        return None
    return new_start, new_end


def build_fallback_next_hsz_url(current_url: str) -> Optional[str]:
    parsed = parse_hsz_range_from_url(current_url)
    if not parsed:
        return None

    start, end = parsed
    new_start = start - 100
    new_end = end - 100
    if new_start < 1 or new_end < 1:
        return None

    return build_hsz_url_with_range(current_url, new_start, new_end)


def ensure_output_dirs(base_output: Path) -> Tuple[Path, Path, Path]:
    prohardver_dir = base_output / "prohardver"
    notebooks_dir = prohardver_dir / "notebooks"
    visited_file = prohardver_dir / "visited_notebook.txt"

    prohardver_dir.mkdir(parents=True, exist_ok=True)
    notebooks_dir.mkdir(parents=True, exist_ok=True)

    if not visited_file.exists():
        visited_file.write_text("", encoding="utf-8")

    return prohardver_dir, notebooks_dir, visited_file


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


def topic_file_path(notebooks_dir: Path, title: str) -> Path:
    return notebooks_dir / f"{sanitize_filename(title)}.txt"


def ensure_topic_metadata(topic_file: Path, title: str, topic_url: str) -> None:
    now_str = datetime.now().strftime("%Y.%m.%d")

    if not topic_file.exists():
        topic_file.write_text(
            f"--visited--{now_str}\nTopic:\n{title}\nURL:\n{topic_url}\n\n",
            encoding="utf-8",
        )
        return

    text = topic_file.read_text(encoding="utf-8")
    if not text.strip():
        topic_file.write_text(
            f"--visited--{now_str}\nTopic:\n{title}\nURL:\n{topic_url}\n\n",
            encoding="utf-8",
        )
        return

    lines = text.splitlines()
    if lines and lines[0].startswith("--visited--"):
        lines[0] = f"--visited--{now_str}"
        topic_file.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    else:
        topic_file.write_text(
            f"--visited--{now_str}\nTopic:\n{title}\nURL:\n{topic_url}\n\n{text}",
            encoding="utf-8",
        )


def get_last_nonempty_line(topic_file: Path) -> Optional[str]:
    if not topic_file.exists():
        return None

    lines = topic_file.read_text(encoding="utf-8").splitlines()
    for line in reversed(lines):
        stripped = line.strip()
        if stripped:
            return stripped

    return None


def read_resume_range_from_last_line(topic_file: Path) -> Optional[Tuple[int, int]]:
    last_line = get_last_nonempty_line(topic_file)
    if not last_line:
        return None

    m = RANGE_ONLY_RE.match(last_line)
    if not m:
        return None

    return int(m.group(1)), int(m.group(2))


def remove_trailing_range_line(topic_file: Path) -> None:
    if not topic_file.exists():
        return

    lines = topic_file.read_text(encoding="utf-8").splitlines()
    while lines and not lines[-1].strip():
        lines.pop()

    if lines and RANGE_ONLY_RE.match(lines[-1].strip()):
        lines.pop()

    text = "\n".join(lines).rstrip()
    if text:
        text += "\n"
    topic_file.write_text(text, encoding="utf-8")


def append_page_and_range(
    topic_file: Path,
    page_range: Tuple[int, int],
    comments: List[Tuple[str, str, str]],
) -> None:
    remove_trailing_range_line(topic_file)

    existing = topic_file.read_text(encoding="utf-8") if topic_file.exists() else ""
    existing = existing.rstrip()

    block_lines = []
    for _, author, comment in comments:
        block_lines.append("Comment:")
        block_lines.append(f"{author}: {comment}")
        block_lines.append("")

    start, end = page_range

    parts = []
    if existing:
        parts.append(existing)
    if block_lines:
        parts.append("\n".join(block_lines).rstrip())
    parts.append(f"{start}-{end}")

    new_text = "\n\n".join(part for part in parts if part).rstrip() + "\n"
    topic_file.write_text(new_text, encoding="utf-8")


def finalize_topic_file(topic_file: Path, title: str, topic_url: str) -> None:
    if not topic_file.exists():
        return

    remove_trailing_range_line(topic_file)

    now_str = datetime.now().strftime("%Y.%m.%d")
    text = topic_file.read_text(encoding="utf-8")
    lines = text.splitlines()

    if lines:
        if lines[0].startswith("--visited--"):
            lines[0] = f"--visited--{now_str}"
        else:
            lines = [f"--visited--{now_str}", "Topic:", title, "URL:", topic_url, ""] + lines
    else:
        lines = [f"--visited--{now_str}", "Topic:", title, "URL:", topic_url, ""]

    topic_file.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def resolve_resume_url(topic_url: str, topic_file: Path) -> str:
    resume_range = read_resume_range_from_last_line(topic_file)
    if not resume_range:
        return build_fresh_url_from_topic_url(topic_url)

    saved_start, saved_end = resume_range
    print(f"[DEBUG] Resume range megtalálva a fájl végén: {saved_start}-{saved_end}")

    prev_range = build_prev_range_from_saved(saved_start, saved_end)
    if not prev_range:
        print("[DEBUG] A mentett range-ből már nem lehet 100-zal visszalépni, indul a friss.html oldalról.")
        return build_fresh_url_from_topic_url(topic_url)

    new_start, new_end = prev_range
    fixed_url = build_hsz_url_from_topic_url(topic_url, new_start, new_end)
    print(f"[DEBUG] Resume URL (100-zal visszaléptetve): {fixed_url}")
    return fixed_url


def open_topic_start_page(
    session: requests.Session,
    topic_url: str,
    topic_file: Path,
    delay: float,
) -> Tuple[str, str]:
    start_url = resolve_resume_url(topic_url, topic_file)
    fresh_url = build_fresh_url_from_topic_url(topic_url)

    print(f"[DEBUG] Topic megnyitása: {start_url}")
    final_url, html = fetch(session, start_url)

    if is_404_html(html) or not page_has_messages_html(html):
        if start_url != fresh_url:
            print(f"[DEBUG] A resume URL nem adott használható kommentoldalt, fallback friss.html-re: {fresh_url}")
            final_url, html = fetch(session, fresh_url)

    time.sleep(delay)
    return final_url, html


def try_go_to_next_page(
    session: requests.Session,
    current_url: str,
    current_html: str,
    delay: float,
) -> Tuple[Optional[bool], Optional[str], Optional[str]]:
    next_url = get_next_page_url_from_html(current_html, current_url)
    if next_url:
        print(f"[DEBUG] Következő oldal link megvan: {next_url}")
        try:
            final_url, html = fetch(session, next_url)
            time.sleep(delay)
            if final_url != current_url:
                return True, final_url, html
            return False, current_url, current_html
        except Exception as e:
            print(f"[DEBUG] Következő oldal letöltési hiba: {e}")
            return None, None, None

    fallback_url = build_fallback_next_hsz_url(current_url)
    if not fallback_url:
        print("[DEBUG] Nincs következő link, és URL fallback sem készíthető.")
        return False, current_url, current_html

    print(f"[DEBUG] URL fallback próbálva: {fallback_url}")
    try:
        final_url, html = fetch(session, fallback_url)
        time.sleep(delay)
        if final_url != current_url and page_has_messages_html(html):
            return True, final_url, html
        return False, current_url, current_html
    except Exception as e:
        print(f"[DEBUG] URL fallback hiba: {e}")
        return None, None, None


def scrape_topic_sequentially(
    session: requests.Session,
    topic_title: str,
    topic_url: str,
    topic_file: Path,
    delay: float,
) -> Tuple[str, bool]:
    current_url, html = open_topic_start_page(session, topic_url, topic_file, delay)
    print(f"[DEBUG] Ténylegesen megnyitott kezdőoldal: {current_url}")

    resolved_title = extract_topic_title_from_html(html, topic_title)
    ensure_topic_metadata(topic_file, resolved_title, topic_url)

    visited_urls: Set[str] = set()
    page_index = 1

    while True:
        if current_url in visited_urls:
            print(f"[DEBUG] Már feldolgozott oldal, leállás: {current_url}")
            return resolved_title, False

        visited_urls.add(current_url)
        current_range = parse_hsz_range_from_url(current_url)

        print(f"[DEBUG] Kommentoldal #{page_index}: {current_url}")
        page_comments = parse_comments_from_html(html)

        if current_range:
            append_page_and_range(topic_file, current_range, page_comments)
            print(f"[DEBUG] Oldal mentve, új utolsó sor: {current_range[0]}-{current_range[1]}")
        else:
            print("[DEBUG] Ez a friss.html oldal, itt nincs számozott range, ezért ide nem kerül range sor.")

        moved, next_url, next_html = try_go_to_next_page(session, current_url, html, delay)

        if moved is True:
            page_index += 1
            current_url = next_url
            html = next_html
            continue

        if moved is False:
            print("[DEBUG] Nincs több oldal, utolsó range törlése és topic véglegesítése.")
            finalize_topic_file(topic_file, resolved_title, topic_url)
            return resolved_title, True

        if moved is None:
            print("[DEBUG] Letöltési vagy navigációs hiba történt, a topic NEM kerül a visitedbe.")
            return resolved_title, False


def scrape_offsets(start_offset: int, end_offset: int, output_dir: str, delay: float) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    _, notebooks_dir, visited_file = ensure_output_dirs(base_output)

    session = make_session()
    visited_topics = load_visited(visited_file)

    for offset in range(start_offset, end_offset + 1, 100):
        list_url = build_list_url(offset)
        print(f"\n[INFO] Listaoldal megnyitása: {list_url}")

        try:
            final_url, html = fetch(session, list_url)
            time.sleep(delay)
        except Exception as e:
            print(f"[WARN] Hiba a listaoldalnál: {list_url} | {e}")
            continue

        topics = parse_topic_links(html, final_url)
        print(f"[INFO] Talált topicok száma: {len(topics)}")
        if not topics:
            continue

        for idx, (topic_title, topic_url) in enumerate(topics, start=1):
            if topic_url in visited_topics:
                print(f"[INFO] ({idx}/{len(topics)}) Már feldolgozva, kihagyva: {topic_title}")
                continue

            topic_file = topic_file_path(notebooks_dir, topic_title)
            print(f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title}")

            try:
                resolved_title, finished = scrape_topic_sequentially(
                    session, topic_title, topic_url, topic_file, delay
                )

                if sanitize_filename(resolved_title) != sanitize_filename(topic_title):
                    new_path = topic_file_path(notebooks_dir, resolved_title)
                    if new_path != topic_file and topic_file.exists():
                        topic_file.replace(new_path)
                        topic_file = new_path

                print(f"[INFO] Topic fájl: {topic_file}")

                if finished:
                    append_visited(visited_file, topic_url)
                    visited_topics.add(topic_url)
                    print(f"[INFO] Topic teljesen feldolgozva, visitedbe írva: {resolved_title}")
                else:
                    print(f"[INFO] Topic nincs kész vagy hibával megállt, NEM kerül visitedbe: {resolved_title}")

            except Exception as e:
                print(f"[WARN] Váratlan hiba a topicnál: {topic_url} | {e}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PROHARDVER notebook topic scraper requests + BeautifulSoup alapon.")
    parser.add_argument("start_offset", type=int, help="Kezdő offset. Pl. 0 vagy 100")
    parser.add_argument("end_offset", type=int, help="Vég offset. Pl. 200 vagy 300")
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre a prohardver mappa. Alapértelmezett: aktuális mappa.",
    )
    parser.add_argument("--delay", type=float, default=1.2, help="Várakozás oldalak között másodpercben.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.start_offset < 0 or args.end_offset < 0:
        print("A start_offset és end_offset nem lehet negatív.")
        sys.exit(1)

    if args.start_offset > args.end_offset:
        print("A start_offset nem lehet nagyobb, mint az end_offset.")
        sys.exit(1)

    if args.start_offset % 100 != 0 or args.end_offset % 100 != 0:
        print("Az offsetek legyenek 100-zal oszthatók: 0, 100, 200, ...")
        sys.exit(1)

    scrape_offsets(
        start_offset=args.start_offset,
        end_offset=args.end_offset,
        output_dir=args.output,
        delay=args.delay,
    )


if __name__ == "__main__":
    main()