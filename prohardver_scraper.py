import argparse
import re
import sys
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set, Tuple
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

BASE_LIST_URL = "https://prohardver.hu/temak/notebook/listaz.php"

HSZ_URL_RE = re.compile(
    r"^(?P<prefix>https?://[^#]+?/hsz_)(?P<start>\d+)-(?P<end>\d+)(?P<suffix>\.html)(?:#msg(?P<msg>\d+))?$",
    re.IGNORECASE,
)
PAGE_MARKER_RE = re.compile(r"^--page--(\d+)-(\d+)\s*$", re.MULTILINE)


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
        if not title or full_url in seen:
            continue

        seen.add(full_url)
        topics.append((title, full_url))

    return topics[:100]


def wait_for_messages(driver: webdriver.Chrome, timeout: int = 20) -> None:
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "li.media[data-id]"))
    )


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


def try_go_to_next_page(driver: webdriver.Chrome, delay: float) -> bool:
    old_url = driver.current_url

    next_el = get_next_page_element(driver)
    if next_el:
        try:
            next_href = next_el.get_attribute("href")
        except Exception:
            next_href = None
        print(f"[DEBUG] Következő oldal gomb megvan. href={next_href}")

        if safe_click(driver, next_el):
            try:
                WebDriverWait(driver, 20).until(lambda d: d.current_url != old_url)
                wait_ready(driver)
                dismiss_known_popups(driver, first_page=False)
                wait_for_messages(driver)
                time.sleep(delay)
                return True
            except TimeoutException:
                print("[DEBUG] Következő oldal gomb volt, de timeout lett az átmenetnél.")
        else:
            print("[DEBUG] Megvolt a következő oldal gomb, de a kattintás nem sikerült.")

    fallback_url = build_fallback_next_hsz_url(old_url)
    if not fallback_url:
        print("[DEBUG] Nincs következő gomb, és URL fallback sem készíthető.")
        return False

    print(f"[DEBUG] URL fallback próbálva: {fallback_url}")
    try:
        driver.get(fallback_url)
        wait_ready(driver)
        dismiss_known_popups(driver, first_page=False)
        wait_for_messages(driver)
        time.sleep(delay)
        return driver.current_url != old_url
    except Exception as e:
        print(f"[DEBUG] URL fallback hiba: {e}")
        return False


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
    return {line.strip() for line in visited_file.read_text(encoding="utf-8").splitlines() if line.strip()}


def append_visited(visited_file: Path, topic_url: str) -> None:
    with visited_file.open("a", encoding="utf-8") as f:
        f.write(topic_url.strip() + "\n")


def topic_file_path(notebooks_dir: Path, title: str) -> Path:
    return notebooks_dir / f"{sanitize_filename(title)}.txt"


def read_topic_progress_marker(topic_file: Path) -> Optional[Tuple[int, int]]:
    if not topic_file.exists():
        return None
    text = topic_file.read_text(encoding="utf-8")
    matches = PAGE_MARKER_RE.findall(text)
    if not matches:
        return None
    start, end = matches[-1]
    return int(start), int(end)


def strip_progress_markers(text: str) -> str:
    text = PAGE_MARKER_RE.sub("", text)
    text = re.sub(r"\n{3,}", "\n\n", text).rstrip()
    return text


def ensure_topic_metadata(topic_file: Path, title: str, topic_url: str) -> None:
    now_str = datetime.now().strftime("%Y.%m.%d")
    if not topic_file.exists():
        topic_file.write_text(
            f"--visited--{now_str}\nTopic:\n{title}\nURL:\n{topic_url}\n\n",
            encoding="utf-8",
        )
        return

    text = topic_file.read_text(encoding="utf-8")
    stripped = text.lstrip()
    if stripped.startswith("--visited--"):
        lines = text.splitlines()
        if lines:
            lines[0] = f"--visited--{now_str}"
        topic_file.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
        return

    topic_file.write_text(
        f"--visited--{now_str}\nTopic:\n{title}\nURL:\n{topic_url}\n\n{text}",
        encoding="utf-8",
    )


def append_page_to_topic_file(topic_file: Path, page_range: Tuple[int, int], comments: List[Tuple[str, str, str]]) -> None:
    text = topic_file.read_text(encoding="utf-8") if topic_file.exists() else ""
    text = strip_progress_markers(text)

    start, end = page_range
    block_lines = []
    for _, author, comment in comments:
        block_lines.append("Comment:")
        block_lines.append(f"{author}: {comment}")
        block_lines.append("")

    if text:
        text = text.rstrip() + "\n\n"

    if block_lines:
        text += "\n".join(block_lines).rstrip()
        if not text.endswith("\n"):
            text += "\n"

    text += f"\n--page--{start}-{end}\n"
    topic_file.write_text(text, encoding="utf-8")


def finalize_topic_file(topic_file: Path, title: str, topic_url: str) -> None:
    if not topic_file.exists():
        return

    now_str = datetime.now().strftime("%Y.%m.%d")
    text = topic_file.read_text(encoding="utf-8")
    text = strip_progress_markers(text)
    lines = text.splitlines()

    if lines:
        if lines[0].startswith("--visited--"):
            lines[0] = f"--visited--{now_str}"
        else:
            lines = [f"--visited--{now_str}", "Topic:", title, "URL:", topic_url, ""] + lines
    else:
        lines = [f"--visited--{now_str}", "Topic:", title, "URL:", topic_url, ""]

    topic_file.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def resolve_resume_url(default_topic_url: str, topic_file: Path) -> str:
    last_range = read_topic_progress_marker(topic_file)
    if not last_range:
        return default_topic_url

    start, end = last_range
    next_start = start - 100
    next_end = end - 100
    if next_start < 1 or next_end < 1:
        return default_topic_url

    resumed = build_hsz_url_with_range(default_topic_url, next_start, next_end)
    return resumed or default_topic_url


def scrape_topic_sequentially(
    driver: webdriver.Chrome,
    topic_title: str,
    topic_url: str,
    topic_file: Path,
    delay: float,
) -> Tuple[str, bool]:
    start_url = resolve_resume_url(topic_url, topic_file)
    print(f"[DEBUG] Topic megnyitása: {start_url}")

    driver.get(start_url)
    wait_ready(driver)
    dismiss_known_popups(driver, first_page=False)
    wait_for_messages(driver)
    time.sleep(delay)

    resolved_title = extract_topic_title(driver, topic_title)
    ensure_topic_metadata(topic_file, resolved_title, topic_url)

    seen_ranges: Set[Tuple[int, int]] = set()
    last_saved_marker = read_topic_progress_marker(topic_file)
    if last_saved_marker:
        seen_ranges.add(last_saved_marker)

    visited_urls: Set[str] = set()
    page_index = 1

    while True:
        current_url = driver.current_url
        if current_url in visited_urls:
            print(f"[DEBUG] Már feldolgozott oldal, leállás: {current_url}")
            finalize_topic_file(topic_file, resolved_title, topic_url)
            return resolved_title, True

        visited_urls.add(current_url)
        current_range = parse_hsz_range_from_url(current_url)

        print(f"[DEBUG] Kommentoldal #{page_index}: {current_url}")
        page_comments = parse_comments_from_html(driver.page_source)

        if current_range:
            if current_range in seen_ranges:
                print(f"[DEBUG] Ez a range már mentve volt: {current_range[0]}-{current_range[1]}")
            else:
                append_page_to_topic_file(topic_file, current_range, page_comments)
                seen_ranges = {current_range}
                print(f"[DEBUG] Oldal elmentve és marker frissítve: --page--{current_range[0]}-{current_range[1]}")
        else:
            print("[DEBUG] Nem sikerült range-et kiolvasni az URL-ből, ezért ezt az oldalt nem tudom resume-markerrel menteni.")

        moved = try_go_to_next_page(driver, delay)
        if not moved:
            print("[DEBUG] Nincs több oldal, marker törlése és topic véglegesítése.")
            finalize_topic_file(topic_file, resolved_title, topic_url)
            return resolved_title, True

        page_index += 1


def scrape_offsets(start_offset: int, end_offset: int, output_dir: str, delay: float, headless: bool) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    _, notebooks_dir, visited_file = ensure_output_dirs(base_output)

    driver = setup_driver(headless=headless)
    visited_topics = load_visited(visited_file)
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
                if topic_url in visited_topics:
                    print(f"[INFO] ({idx}/{len(topics)}) Már feldolgozva, kihagyva: {topic_title}")
                    continue

                topic_file = topic_file_path(notebooks_dir, topic_title)
                print(f"\n[INFO] ({idx}/{len(topics)}) Topic: {topic_title}")

                try:
                    resolved_title, finished = scrape_topic_sequentially(
                        driver, topic_title, topic_url, topic_file, delay
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
                        print(f"[INFO] Topic teljesen feldolgozva: {resolved_title}")
                    else:
                        print(f"[INFO] Topic félbemaradt, később innen folytatható: {resolved_title}")

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
    parser = argparse.ArgumentParser(description="PROHARDVER notebook topic scraper Seleniummal.")
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