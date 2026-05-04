from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

BASE_LIST_URL = "https://prohardver.hu/temak/alaplap_chipset_ram/listaz.php"

HSZ_URL_RE = re.compile(
    r"^(?P<prefix>https?://[^#]+?/hsz_)(?P<start>\d+)-(?P<end>\d+)(?P<suffix>\.html)(?:#msg(?P<msg>\d+))?$",
    re.IGNORECASE,
)

URL_FIELD_RE = re.compile(r'"url"\s*:\s*"([^"]+)"')
NEXT_URL_FIELD_RE = re.compile(r'"next_resume_url"\s*:\s*(?:"([^"]+)"|null)')
COMMENT_ID_FIELD_RE = re.compile(r'"comment_id"\s*:\s*(?:"([^"]+)"|null)')

# A PROHARDVER forum DOM-ja időnként változik.
# Régi post elem: li.media[data-id]
# Új post elem a jelenlegi oldalon: li.message-off/message-on[data-id][data-rplid]
MESSAGE_ITEM_SELECTOR = ", ".join(
    [
        "li.media[data-id]",
        "li.message-off[data-id]",
        "li.message-on[data-id]",
        "li[class^='message-'][data-id]",
        "li[class*=' message-'][data-id]",
        "ul.message-list-desc > li[data-id]",
    ]
)

MESSAGE_TEXT_SELECTORS = [
    ".message-content p.mgt0",
    ".message-content",
    ".msg-content p.mgt0",
    ".msg-content",
    "p.mgt0",
]

MESSAGE_AUTHOR_SELECTORS = [
    ".message-body-user .user-name",
    ".message-body-user a[href*='/tag/']",
    ".message-body-user a",
    ".message-body-user",
    ".msg-user",
    ".media-left",
]

MESSAGE_HEADER_SELECTORS = [
    ".message-head",
    ".message-header",
    ".msg-header",
    "time",
    ".msg-date",
    ".date",
]


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


def sanitize_filename(name: str, max_len: int = 180) -> str:
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


def split_name_like_person(name: str) -> Dict[str, str]:
    name = clean_text(name)
    if not name:
        return {"name": ""}

    parts = name.split()
    if len(parts) >= 2:
        return {"family": parts[0], "given": " ".join(parts[1:])}
    return {"name": name}


def now_local_iso() -> str:
    return datetime.now().astimezone().isoformat()


def setup_driver(headless: bool = False) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1600,1200")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--lang=hu-HU")
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def wait_ready(driver: webdriver.Chrome, timeout: int = 20) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )


def safe_click(driver: webdriver.Chrome, element) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(0.2)
        try:
            element.click()
        except Exception:
            driver.execute_script("arguments[0].click();", element)
        return True
    except Exception:
        return False


def click_first_visible(driver: webdriver.Chrome, xpaths: List[str], timeout: float = 5.0) -> bool:
    end_time = time.time() + timeout
    while time.time() < end_time:
        for xpath in xpaths:
            try:
                elements = driver.find_elements(By.XPATH, xpath)
            except Exception:
                elements = []

            for element in elements:
                try:
                    if not element.is_displayed():
                        continue
                except StaleElementReferenceException:
                    continue

                if safe_click(driver, element):
                    time.sleep(0.8)
                    return True
        time.sleep(0.2)
    return False


def reject_cookies(driver: webdriver.Chrome, timeout: float = 8.0) -> bool:
    xpaths = [
        "//*[self::button or self::a or self::span][normalize-space()='NEM FOGADOM EL']",
        "//*[contains(translate(normalize-space(), 'abcdefghijklmnopqrstuvwxyzáéíóöőúüű', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÖŐÚÜŰ'), 'NEM FOGADOM EL')]",
    ]
    clicked = click_first_visible(driver, xpaths, timeout=timeout)
    if clicked:
        print("[DEBUG] Sütik elutasítva.")
    return clicked


def close_skip_popup(driver: webdriver.Chrome, timeout: float = 4.0) -> bool:
    xpaths = [
        "//*[self::button or self::a or self::span][normalize-space()='Lemaradok']",
        "//*[contains(normalize-space(), 'Lemaradok')]",
        "//input[@type='button' and @value='Lemaradok']",
    ]
    clicked = click_first_visible(driver, xpaths, timeout=timeout)
    if clicked:
        print("[DEBUG] Lemaradok popup bezárva.")
    return clicked


def dismiss_known_popups(driver: webdriver.Chrome, first_page: bool = False) -> None:
    if first_page:
        reject_cookies(driver, timeout=8.0)
    close_skip_popup(driver, timeout=3.0)


def wait_for_topic_list(driver: webdriver.Chrome, timeout: int = 20) -> None:
    selectors = [
        "div.thread-list h4 a[href*='/tema/']",
        "div.col.thread-title-thread h4 a[href*='/tema/']",
        "main h4 a[href*='/tema/']",
        "h4 a[href*='/tema/']",
    ]
    for selector in selectors:
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return
        except TimeoutException:
            pass
    raise TimeoutException("Nem található topic lista.")


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


def wait_for_messages(driver: webdriver.Chrome, timeout: int = 20) -> None:
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, MESSAGE_ITEM_SELECTOR))
    )


def page_has_messages(driver: webdriver.Chrome) -> bool:
    try:
        items = driver.find_elements(By.CSS_SELECTOR, MESSAGE_ITEM_SELECTOR)
        return len(items) > 0
    except Exception:
        return False


def page_has_no_results(driver: webdriver.Chrome) -> bool:
    try:
        elems = driver.find_elements(By.CSS_SELECTOR, "li.list-message")
        for elem in elems:
            txt = clean_text(elem.text).lower()
            if "nincs találat" in txt:
                return True
    except Exception:
        pass

    try:
        body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text).lower()
        return "nincs találat" in body_text
    except Exception:
        return False


def is_404_page(driver: webdriver.Chrome) -> bool:
    title = clean_text(driver.title).lower()
    body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text).lower()
    return "404" in title or "404 not found" in body_text or "a kért oldal nem létezik" in body_text


def extract_topic_title(driver: webdriver.Chrome, fallback: str) -> str:
    soup = BeautifulSoup(driver.page_source, "html.parser")
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
    # Régi layoutban a headerben is benne volt a szerző; az új layoutban
    # a bal oldali user blokk: .message-body-user.
    for selector in [".msg-header", ".message-head", ".message-header"]:
        header = post.select_one(selector)
        if header:
            header_text = clean_text(header.get_text(" ", strip=True))
            # Példa: #67312 Crabface > rxmiss #67311 2026-05-03 ...
            m = re.match(r"#\d+\s+(.+?)\s*>\s*.+?#\d+", header_text)
            if m:
                author = clean_text(m.group(1))
                if author:
                    return author

    ignored_lines = {
        "tag", "őstag", "ostag", "senior tag", "aktív tag", "aktiv tag",
        "félisten", "felisten", "veterán", "veteran", "addikt", "nagyúr", "nagyur",
        "újonc", "ujonc", "csendes tag", "titán", "titan",
    }

    for selector in MESSAGE_AUTHOR_SELECTORS:
        node = post.select_one(selector)
        if not node:
            continue
        txt = clean_text(node.get_text("\n", strip=True))
        lines = [line.strip() for line in txt.splitlines() if line.strip()]
        for line in lines:
            if line.lower() in ignored_lines:
                continue
            if line.startswith("#"):
                continue
            return line

    return "ismeretlen"


def extract_comment_date(post) -> Optional[str]:
    patterns = [
        r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b",
        r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}\b",
        r"\b\d{4}\.\d{2}\.\d{2}\.?(?: \d{2}:\d{2}(?::\d{2})?)?\b",
        r"\bma,? \d{1,2}:\d{2}\b",
        r"\btegnap,? \d{1,2}:\d{2}\b",
    ]

    for selector in MESSAGE_HEADER_SELECTORS:
        node = post.select_one(selector)
        if not node:
            continue

        datetime_attr = clean_text(node.get("datetime", ""))
        if datetime_attr:
            return datetime_attr

        header_text = clean_text(node.get_text(" ", strip=True))
        for pattern in patterns:
            m = re.search(pattern, header_text, flags=re.I)
            if m:
                return clean_text(m.group(0))

    # Utolsó fallback: néha a dátum a post teljes szövegében van.
    full_text = clean_text(post.get_text(" ", strip=True))
    for pattern in patterns:
        m = re.search(pattern, full_text, flags=re.I)
        if m:
            return clean_text(m.group(0))

    return None


def extract_comment_likes(post) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    text = clean_text(post.get_text(" ", strip=True))

    likes = None
    dislikes = None

    patterns_like = [
        r"\bLike(?:ok)?\s*[:\-]?\s*(\d+)\b",
        r"\bTetszik\s*[:\-]?\s*(\d+)\b",
    ]
    patterns_dislike = [
        r"\bDislike(?:ok)?\s*[:\-]?\s*(\d+)\b",
        r"\bNem tetszik\s*[:\-]?\s*(\d+)\b",
    ]

    for pattern in patterns_like:
        m = re.search(pattern, text, flags=re.I)
        if m:
            likes = int(m.group(1))
            break

    for pattern in patterns_dislike:
        m = re.search(pattern, text, flags=re.I)
        if m:
            dislikes = int(m.group(1))
            break

    score = None
    if likes is not None or dislikes is not None:
        score = (likes or 0) - (dislikes or 0)

    return likes, dislikes, score


def extract_comment_text(post) -> str:
    for selector in MESSAGE_TEXT_SELECTORS:
        nodes = post.select(selector)
        if not nodes:
            continue

        parts = []
        for node in nodes:
            # A gombok/idézetvezérlők ne kerüljenek bele a komment szövegébe.
            for junk in node.select("script, style, .message-body-btns, .msg-buttons, .buttons"):
                junk.decompose()

            text = clean_text(node.get_text("\n", strip=True))
            if text:
                parts.append(text)

        if parts:
            joined = "\n".join(parts)
            joined = re.sub(r"\n{3,}", "\n\n", joined).strip()
            if joined:
                return joined

    return ""


def comment_url_from_page(current_url: str, post_id: str) -> str:
    base = current_url.split("#")[0]
    if post_id:
        return f"{base}#msg{post_id}"
    return base


def parse_comments_from_html(
    html: str,
    current_url: str,
    next_resume_url: Optional[str],
) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    posts = soup.select(MESSAGE_ITEM_SELECTOR)
    results: List[Dict] = []

    print(f"[DEBUG] Talált komment elemek száma: {len(posts)}")

    for index, post in enumerate(posts, start=1):
        post_id = clean_text(post.get("data-id", ""))
        author = extract_author(post)
        comment = extract_comment_text(post)
        date_text = extract_comment_date(post)
        likes, dislikes, score = extract_comment_likes(post)

        preview = comment[:120].replace("\n", " | ") if comment else "<üres>"
        print(
            f"[DEBUG] Poszt #{index} | data-id={post_id or '-'} | szerző={author} | "
            f"dátum={date_text or '-'} | like={likes} | preview={preview}"
        )

        if not comment:
            continue

        results.append(
            {
                "comment_id": post_id or None,
                "author": author,
                "date": date_text,
                "likes": likes,
                "dislikes": dislikes,
                "score": score,
                "url": comment_url_from_page(current_url, post_id),
                "page_url": current_url.split("#")[0],
                "next_resume_url": next_resume_url,
                "data": comment,
            }
        )

    print(f"[DEBUG] Kinyert kommentek ezen az oldalon: {len(results)}")
    return results


def get_next_page_element(driver: webdriver.Chrome):
    xpaths = [
        "//a[@rel='next']",
        "//a[contains(@title, 'Következő blokk')]",
        "//li[contains(@class,'nav-arrow')]//a[@rel='next']",
        "//a[contains(@href, '/hsz_') and (.//span[contains(@class,'fa-forward')] or .//span[contains(@class,'fa-step-forward')])]",
    ]
    for xpath in xpaths:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
        except Exception:
            elements = []

        for el in elements:
            try:
                if el.is_displayed() and el.is_enabled():
                    return el
            except StaleElementReferenceException:
                continue

    return None


def get_next_page_href(driver: webdriver.Chrome) -> Optional[str]:
    next_el = get_next_page_element(driver)
    if not next_el:
        return None
    try:
        href = next_el.get_attribute("href")
    except Exception:
        href = None
    if href:
        return href.split("#")[0]
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


def load_candidate_comment_page(
    driver: webdriver.Chrome,
    target_url: str,
    delay: float,
) -> str:
    """
    Visszatérési értékek:
    - "messages": normál kommentoldal
    - "empty": hibás/üres oldal, pl. 'Nincs találat.'
    - "404": nem létező oldal
    - "unknown": betöltött valami, de se komment, se egyértelmű üres állapot
    """
    print(f"[DEBUG] Céloldal megnyitása közvetlenül: {target_url}")
    driver.get(target_url)
    wait_ready(driver)
    dismiss_known_popups(driver, first_page=False)
    time.sleep(delay)

    if is_404_page(driver):
        return "404"

    if page_has_messages(driver):
        return "messages"

    if page_has_no_results(driver):
        return "empty"

    try:
        wait_for_messages(driver, timeout=3)
        if page_has_messages(driver):
            return "messages"
    except TimeoutException:
        pass

    if page_has_no_results(driver):
        return "empty"

    return "unknown"


def try_go_to_next_page(driver: webdriver.Chrome, delay: float, max_empty_skips: int = 15) -> Optional[bool]:
    old_url = driver.current_url
    next_href = get_next_page_href(driver)

    if not next_href:
        next_href = build_fallback_next_hsz_url(old_url)
        if next_href:
            next_href = next_href.split("#")[0]

    if not next_href:
        print("[DEBUG] Nincs következő gomb, és URL fallback sem készíthető.")
        return False

    attempted_urls: Set[str] = set()
    empty_skip_count = 0
    candidate_url = next_href

    while candidate_url:
        candidate_url = candidate_url.split("#")[0]

        if candidate_url in attempted_urls:
            print(f"[DEBUG] Ugyanazt a candidate URL-t újra kaptam, leállás: {candidate_url}")
            return False
        attempted_urls.add(candidate_url)

        state = None
        try:
            state = load_candidate_comment_page(driver, candidate_url, delay)
        except TimeoutException:
            print(f"[DEBUG] Timeout a candidate oldal betöltésekor: {candidate_url}")
            state = "unknown"
        except Exception as e:
            print(f"[DEBUG] Hiba a candidate oldal betöltésekor: {candidate_url} | {e}")
            state = "unknown"

        if state == "messages":
            print(f"[DEBUG] Sikeres továbblépés kommentoldalra: {driver.current_url}")
            return True

        if state == "404":
            print(f"[DEBUG] A candidate oldal 404-es, további lap már nincs: {candidate_url}")
            return False

        if state == "empty":
            empty_skip_count += 1
            print(
                f"[DEBUG] Üres/hibás köztes oldal kihagyva ('Nincs találat.'): "
                f"{candidate_url} | skip #{empty_skip_count}"
            )
            if empty_skip_count >= max_empty_skips:
                print("[DEBUG] Túl sok egymás utáni üres oldal, leállás.")
                return False

            next_from_here = get_next_page_href(driver)
            if not next_from_here:
                next_from_here = build_fallback_next_hsz_url(driver.current_url)
                if next_from_here:
                    next_from_here = next_from_here.split("#")[0]

            candidate_url = next_from_here
            continue

        print(f"[DEBUG] Nem egyértelmű állapot a candidate oldalon, megpróbálom URL-fallbackgel: {candidate_url}")
        fallback_url = build_fallback_next_hsz_url(candidate_url)
        if fallback_url:
            candidate_url = fallback_url.split("#")[0]
            continue

        print("[DEBUG] Nem találtam további fallback oldalt.")
        return None

    return False


def ensure_output_dirs(base_output: Path) -> Tuple[Path, Path, Path]:
    prohardver_dir = base_output / "prohardver"
    tv_audio_dir = prohardver_dir / "alaplap_chipset_ram"               #itt van a mappa amibe a topicok mentődnek
    visited_file = prohardver_dir / "visited_alaplap_chipset_ram.txt"

    prohardver_dir.mkdir(parents=True, exist_ok=True)
    tv_audio_dir.mkdir(parents=True, exist_ok=True)

    if not visited_file.exists():
        visited_file.write_text("", encoding="utf-8")

    return prohardver_dir, tv_audio_dir, visited_file


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


def normalize_topic_url_for_visited(topic_url: str) -> str:
    return normalize_topic_base_url(topic_url)


def topic_file_path(output_dir: Path, title: str) -> Path:
    return output_dir / f"{sanitize_filename(title)}.json"


def read_tail_text(path: Path, max_bytes: int = 1024 * 1024) -> str:
    if not path.exists():
        return ""

    size = path.stat().st_size
    with path.open("rb") as f:
        if size > max_bytes:
            f.seek(size - max_bytes)
        data = f.read()

    return data.decode("utf-8", errors="ignore")


def read_head_text(path: Path, max_bytes: int = 1024 * 1024) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as f:
        data = f.read(max_bytes)
    return data.decode("utf-8", errors="ignore")


def count_existing_comments_in_file(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0

    count = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            count += line.count('"comment_id":')
    return count


def file_looks_closed_json(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return False

        if data.get("origin") != "prohardver_tv_audio":
            return False

        extra = data.get("extra")
        if not isinstance(extra, dict):
            return False

        if extra.get("scrape_status") != "finished":
            return False

        comments = data.get("comments")
        if not isinstance(comments, list):
            return False

        return True

    except Exception:
        pass

    tail = read_tail_text(path, max_bytes=256 * 1024).rstrip()
    if not tail.endswith("}"):
        return False

    required_markers = [
        '"origin": "prohardver_tv_audio"',
        '"scrape_status": "finished"',
        '"date_modified":',
    ]
    return all(marker in tail for marker in required_markers)


def file_has_any_saved_comment(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False

    text = read_head_text(path, max_bytes=2 * 1024 * 1024)
    marker = '"comments": ['
    idx = text.find(marker)
    if idx == -1:
        return False

    after = text[idx + len(marker):]
    after = after.lstrip()

    if not after:
        return False

    return after.startswith("{") or '"comment_id"' in after or '"page_url"' in after


def find_last_comment_url_from_file(path: Path) -> Optional[str]:
    if not path.exists() or path.stat().st_size == 0:
        return None

    text = read_tail_text(path, max_bytes=2 * 1024 * 1024)

    marker = '"comments": ['
    idx = text.find(marker)
    if idx != -1:
        text = text[idx + len(marker):]

    matches = URL_FIELD_RE.findall(text)
    if not matches:
        return None

    for url in reversed(matches):
        if "#msg" in url and ("/hsz_" in url or "/friss.html" in url):
            return url

    for url in reversed(matches):
        if "/hsz_" in url or "/friss.html" in url:
            return url

    return None


def find_last_next_resume_url_from_file(path: Path) -> Optional[str]:
    if not path.exists() or path.stat().st_size == 0:
        return None

    text = read_tail_text(path, max_bytes=2 * 1024 * 1024)

    marker = '"comments": ['
    idx = text.find(marker)
    if idx != -1:
        text = text[idx + len(marker):]

    matches = NEXT_URL_FIELD_RE.findall(text)
    if not matches:
        return None

    for value in reversed(matches):
        cleaned = clean_text(value)
        if cleaned and cleaned.lower() != "null":
            return cleaned
    return None


def find_last_comment_id_from_file(path: Path) -> Optional[str]:
    if not path.exists() or path.stat().st_size == 0:
        return None

    text = read_tail_text(path, max_bytes=2 * 1024 * 1024)

    marker = '"comments": ['
    idx = text.find(marker)
    if idx != -1:
        text = text[idx + len(marker):]

    matches = COMMENT_ID_FIELD_RE.findall(text)
    if not matches:
        return None

    for value in reversed(matches):
        cleaned = clean_text(value)
        if cleaned and cleaned.lower() != "null":
            return cleaned
    return None


def init_open_json_file_if_needed(
    topic_file: Path,
    resolved_title: str,
    topic_url: str,
) -> None:
    if topic_file.exists() and topic_file.stat().st_size > 0:
        return

    header_obj = {
        "title": resolved_title,
        "authors": [],
        "data": {
            "content": resolved_title,
            "likes": None,
            "dislikes": None,
            "score": None,
            "date": None,
            "url": topic_url,
            "language": "hu",
            "tags": [],
            "rights": "PROHARDVER! TV/Audió fórum tartalom",
            "extra": {},
            "origin": "prohardver_tv_audio",
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
                    "page_url": comment.get("page_url"),
                    "next_resume_url": comment.get("next_resume_url"),
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
    saved_comment_count: int,
    resume_source: Optional[str],
) -> None:
    with topic_file.open("a", encoding="utf-8") as f:
        f.write("\n  ],\n")
        f.write('  "origin": "prohardver_tv_audio",\n')
        f.write('  "extra": {\n')
        f.write('    "scrape_status": "finished",\n')
        f.write(f'    "saved_comment_pages": {saved_comment_pages},\n')
        f.write(f'    "saved_comment_count": {saved_comment_count},\n')
        f.write(f'    "resume_source": {json.dumps(resume_source, ensure_ascii=False)},\n')
        f.write(f'    "date_modified": {json.dumps(now_local_iso(), ensure_ascii=False)}\n')
        f.write("  }\n")
        f.write("}\n")
        f.flush()
        os.fsync(f.fileno())


def derive_next_page_from_comment_url(comment_url: str) -> Optional[str]:
    if not comment_url:
        return None

    base_url = comment_url.split("#")[0]
    parsed = parse_hsz_range_from_url(base_url)
    if not parsed:
        return None

    start, end = parsed
    prev_range = build_prev_range_from_saved(start, end)
    if not prev_range:
        return None

    new_start, new_end = prev_range
    return build_hsz_url_from_topic_url(base_url, new_start, new_end)


def resolve_resume_url(topic_url: str, topic_file: Path) -> Tuple[str, Optional[str], bool]:
    if topic_file.exists() and topic_file.stat().st_size > 0:
        if file_looks_closed_json(topic_file):
            return build_fresh_url_from_topic_url(topic_url), "already_closed", True

        next_resume_url = find_last_next_resume_url_from_file(topic_file)
        if next_resume_url:
            print(f"[INFO] Resume: next_resume_url alapján innen folytatva: {next_resume_url}")
            return next_resume_url, "existing_json_next_resume_url", False

        last_comment_url = find_last_comment_url_from_file(topic_file)
        if last_comment_url:
            derived = derive_next_page_from_comment_url(last_comment_url)
            if derived:
                print(f"[INFO] Resume: utolsó komment URL alapján innen folytatva: {derived}")
                return derived, "existing_json_last_comment_url", False

        print("[INFO] Van meglévő félkész fájl, de nem találtam benne használható resume pontot. Friss oldalról indul.")

    return build_fresh_url_from_topic_url(topic_url), None, False


def open_topic_start_page(driver: webdriver.Chrome, topic_url: str, topic_file: Path, delay: float) -> Tuple[str, Optional[str], bool]:
    start_url, resume_source, already_closed = resolve_resume_url(topic_url, topic_file)
    if already_closed:
        return start_url, resume_source, True

    fresh_url = build_fresh_url_from_topic_url(topic_url)

    print(f"[DEBUG] Topic megnyitása: {start_url}")
    driver.get(start_url)
    wait_ready(driver)
    dismiss_known_popups(driver, first_page=False)
    time.sleep(delay)

    if is_404_page(driver) or (not page_has_messages(driver) and not page_has_no_results(driver)):
        if start_url != fresh_url:
            print(f"[DEBUG] A resume URL nem adott használható kommentoldalt, fallback friss.html-re: {fresh_url}")
            driver.get(fresh_url)
            wait_ready(driver)
            dismiss_known_popups(driver, first_page=False)
            time.sleep(delay)
            resume_source = "fallback_to_fresh"

    if page_has_no_results(driver):
        print("[DEBUG] A kezdő/resume oldal üres ('Nincs találat.'), megpróbálok továbblépni egy valódi kommentoldalra.")
        moved = try_go_to_next_page(driver, delay)
        if moved is not True:
            raise TimeoutException("A resume oldal üres volt, és nem találtam használható következő kommentoldalt.")

    if not page_has_messages(driver):
        wait_for_messages(driver)

    return driver.current_url, resume_source, False


def scrape_topic_sequentially(
    driver: webdriver.Chrome,
    topic_title: str,
    topic_url: str,
    topic_file: Path,
    delay: float,
) -> Tuple[str, bool]:
    opened_url, resume_source, already_closed = open_topic_start_page(driver, topic_url, topic_file, delay)
    if already_closed:
        print("[INFO] A topic fájl már lezárt JSON, kihagyva.")
        return topic_title, True

    print(f"[DEBUG] Ténylegesen megnyitott kezdőoldal: {opened_url}")

    resolved_title = extract_topic_title(driver, topic_title)
    init_open_json_file_if_needed(topic_file, resolved_title, topic_url)

    first_comment_already_written = file_has_any_saved_comment(topic_file)
    existing_comment_count = count_existing_comments_in_file(topic_file)
    last_saved_comment_id = find_last_comment_id_from_file(topic_file)

    total_saved_comments = existing_comment_count
    print(f"[INFO] Már meglévő kommentek a fájlban: {existing_comment_count}")

    visited_urls: Set[str] = set()
    page_index = 1
    first_processed_page = True

    while True:
        current_url = driver.current_url
        current_url_base = current_url.split("#")[0]
        if current_url_base in visited_urls:
            print(f"[DEBUG] Már feldolgozott oldal, leállás: {current_url}")
            return resolved_title, False

        visited_urls.add(current_url_base)
        next_resume_url = get_next_page_href(driver)
        if not next_resume_url:
            next_resume_url = build_fallback_next_hsz_url(current_url)
            if next_resume_url:
                next_resume_url = next_resume_url.split("#")[0]

        print(f"[DEBUG] Kommentoldal #{page_index}: {current_url}")
        page_comments = parse_comments_from_html(driver.page_source, current_url, next_resume_url)

        if first_processed_page and existing_comment_count > 0 and last_saved_comment_id:
            original_len = len(page_comments)
            filtered_comments: List[Dict] = []
            seen_last = False

            for c in page_comments:
                current_comment_id = str(c.get("comment_id") or "")
                if not seen_last:
                    if current_comment_id == str(last_saved_comment_id):
                        seen_last = True
                    continue
                filtered_comments.append(c)

            if seen_last:
                page_comments = filtered_comments
                print(
                    f"[INFO] Resume szűrés az első oldalon: {original_len} kommentből "
                    f"{len(page_comments)} új maradt az utolsó mentett comment_id után."
                )
            else:
                print(
                    "[INFO] Resume módban az utolsó mentett comment_id nem található ezen az oldalon, "
                    "ezért ezt az oldalt újként kezelem."
                )

            first_processed_page = False
            last_saved_comment_id = None
        else:
            first_processed_page = False

        new_comments_on_page = len(page_comments)

        if page_comments:
            first_comment_already_written = append_comments_page_to_open_json(
                topic_file=topic_file,
                comments=page_comments,
                first_comment_already_written=first_comment_already_written,
            )
            total_saved_comments += new_comments_on_page

        print(
            f"[INFO] Oldal appendelve a JSON végére: {topic_file} | "
            f"új kommentek ezen az oldalon: {new_comments_on_page} | "
            f"összes mentett komment eddig: {total_saved_comments}"
        )

        moved = try_go_to_next_page(driver, delay)

        if moved is True:
            page_index += 1
            continue

        if moved is False:
            print("[DEBUG] Nincs több oldal, topic véglegesítése.")
            close_topic_json_file(
                topic_file=topic_file,
                saved_comment_pages=page_index,
                saved_comment_count=total_saved_comments,
                resume_source=resume_source,
            )
            really_closed = file_looks_closed_json(topic_file)
            print(
                f"[INFO] Topic végleg lezárva: {topic_file} | "
                f"lezárt={really_closed} | végső kommentdarab={total_saved_comments}"
            )
            return resolved_title, really_closed

        if moved is None:
            print(
                f"[DEBUG] Timeout vagy navigációs hiba történt, a topic NEM kerül a visitedbe. "
                f"Eddig mentett kommentek: {total_saved_comments}"
            )
            return resolved_title, False


def scrape_offsets(start_offset: int, end_offset: int, output_dir: str, delay: float, headless: bool) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    _, tv_audio_dir, visited_file = ensure_output_dirs(base_output)

    driver = setup_driver(headless=headless)
    visited_topics = load_visited(visited_file)
    visited_topics = {normalize_topic_url_for_visited(x) for x in visited_topics}
    first_list_page = True

    try:
        for offset in range(start_offset, end_offset + 1, 100):
            list_url = build_list_url(offset)
            print(f"\n[INFO] Listaoldal megnyitása: {list_url}")

            try:
                driver.get(list_url)
                wait_ready(driver)
                dismiss_known_popups(driver, first_page=first_list_page)
                first_list_page = False
                wait_for_topic_list(driver)
                time.sleep(delay)
            except TimeoutException:
                print(f"[WARN] Timeout a listaoldalnál: {list_url}")
                continue

            topics = parse_topic_links(driver.page_source, driver.current_url)
            print(f"[INFO] Talált topicok száma: {len(topics)}")
            if not topics:
                continue

            for idx, (topic_title, topic_url) in enumerate(topics, start=1):
                topic_url_norm = normalize_topic_url_for_visited(topic_url)
                if topic_url_norm in visited_topics:
                    print(f"[INFO] ({idx}/{len(topics)}) Már feldolgozva, kihagyva: {topic_title}")
                    continue

                topic_file = topic_file_path(tv_audio_dir, topic_title)
                print(f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title}")

                try:
                    resolved_title, finished = scrape_topic_sequentially(
                        driver, topic_title, topic_url, topic_file, delay
                    )

                    if sanitize_filename(resolved_title) != sanitize_filename(topic_title):
                        new_path = topic_file_path(tv_audio_dir, resolved_title)
                        if new_path != topic_file and topic_file.exists():
                            topic_file.replace(new_path)
                            topic_file = new_path

                    print(f"[INFO] Topic fájl: {topic_file}")

                    if finished and file_looks_closed_json(topic_file):
                        append_visited(visited_file, topic_url_norm)
                        visited_topics.add(topic_url_norm)
                        print(f"[INFO] Topic teljesen feldolgozva, visitedbe írva: {resolved_title}")
                    else:
                        print(f"[INFO] Topic nincs kész vagy hibával megállt, NEM kerül visitedbe: {resolved_title}")

                except TimeoutException:
                    print(f"[WARN] Timeout a topicnál: {topic_url}")
                except WebDriverException as e:
                    print(f"[WARN] Selenium hiba a topicnál: {topic_url} | {e}")
                except Exception as e:
                    print(f"[WARN] Váratlan hiba a topicnál: {topic_url} | {e}")

                try:
                    driver.get(list_url)
                    wait_ready(driver)
                    dismiss_known_popups(driver, first_page=False)
                    wait_for_topic_list(driver)
                    time.sleep(delay)
                except Exception as e:
                    print(f"[WARN] Nem sikerült visszamenni a listaoldalra: {e}")
                    break
    finally:
        driver.quit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PROHARDVER TV/Audió topic scraper Seleniummal, appendelt JSON mentéssel."
    )
    parser.add_argument("start_offset", type=int, help="Kezdő offset. Pl. 0 vagy 100")
    parser.add_argument("end_offset", type=int, help="Vég offset. Pl. 200 vagy 300")
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre a prohardver mappa. Alapértelmezett: aktuális mappa.",
    )
    parser.add_argument("--delay", type=float, default=1.2, help="Várakozás oldalak között másodpercben.")
    parser.add_argument("--headless", action="store_true", help="Headless mód.")
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
        headless=args.headless,
    )


if __name__ == "__main__":
    main()

    # python prohardver_scraper.py 0 6000 --output . --delay 3 --headless