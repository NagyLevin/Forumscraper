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
from urllib.parse import parse_qs, urljoin, urlparse

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

BASE_LIST_URL = "https://hardverapro.hu/aprok/index.html?offset={offset}"
START_URL = BASE_LIST_URL.format(offset=0)
ORIGIN_NAME = "hardverapro_aprok"

URL_FIELD_RE = re.compile(r'"url"\s*:\s*"([^"]+)"')
UADID_FIELD_RE = re.compile(r'"uadid"\s*:\s*(?:"([^"]+)"|(\d+)|null)')
LAST_OFFSET_FIELD_RE = re.compile(r'"list_offset"\s*:\s*(\d+)')


def build_list_url(offset: int) -> str:
    return BASE_LIST_URL.format(offset=offset)


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
        return "hardverapro"

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

    return name or "hardverapro"


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
        "//*[contains(@class,'cookie')]//*[contains(translate(normalize-space(), 'abcdefghijklmnopqrstuvwxyzáéíóöőúüű', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÖŐÚÜŰ'), 'NEM FOGADOM EL')]",
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


def wait_for_listing_page(driver: webdriver.Chrome, timeout: int = 20) -> None:
    selectors = [
        "li.media[data-uadid] h1 a[href*='/aprok/']",
        "li.media[data-uadid] a[href*='/aprok/'][href$='.html']",
        "main li.media[data-uadid]",
    ]
    for selector in selectors:
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return
        except TimeoutException:
            pass
    raise TimeoutException("Nem található hirdetéslista ezen az oldalon.")


def page_has_ads(driver: webdriver.Chrome) -> bool:
    try:
        items = driver.find_elements(By.CSS_SELECTOR, "li.media[data-uadid]")
        return len(items) > 0
    except Exception:
        return False


def page_has_no_results(driver: webdriver.Chrome) -> bool:
    try:
        body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text).lower()
    except Exception:
        return False

    phrases = [
        "nincs találat",
        "nem található hirdetés",
        "nincs több hirdetés",
        "nincsenek hirdetések",
    ]
    return any(p in body_text for p in phrases)


def parse_listing_ads(html: str, page_url: str) -> List[Dict[str, Optional[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("li.media[data-uadid]")

    ads: List[Dict[str, Optional[str]]] = []
    seen_urls: Set[str] = set()

    for item in items:
        uadid = clean_text(item.get("data-uadid", "")) or None

        link = item.select_one("h1 a[href]") or item.select_one("a[href*='/aprok/'][href]")
        if not link:
            continue

        href = link.get("href")
        if not href:
            continue

        full_url = urljoin(page_url, href)
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        title = clean_text(link.get_text(" ", strip=True))

        price_node = item.select_one(".uad-price")
        location_node = item.select_one(".uad-col-info")
        seller_link = item.select_one(".uad-col-title a[href*='/aprok/hirdeto/']")

        ads.append(
            {
                "uadid": uadid,
                "title": title,
                "url": full_url,
                "price": clean_text(price_node.get_text(" ", strip=True)) if price_node else None,
                "listing_location": clean_text(location_node.get_text(" ", strip=True)) if location_node else None,
                "listing_seller": clean_text(seller_link.get_text(" ", strip=True)) if seller_link else None,
            }
        )

    return ads


def wait_for_ad_page(driver: webdriver.Chrome, timeout: int = 20) -> None:
    selectors = [
        "div.uad-content",
        "div.uad-content-block",
        "div.trif-content",
        "a[href*='/aprok/hirdeto/']",
    ]
    for selector in selectors:
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            return
        except TimeoutException:
            pass
    raise TimeoutException("Nem található hirdetésoldal-tartalom.")


def get_longest_text(nodes) -> str:
    best = ""
    for node in nodes:
        text = clean_text(node.get_text("\n", strip=True))
        if len(text) > len(best):
            best = text
    return best


def extract_uadid_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if "uadid" in qs and qs["uadid"]:
        return clean_text(qs["uadid"][0]) or None

    m = re.search(r"uadid[-_=](\d+)", url, flags=re.I)
    if m:
        return m.group(1)
    return None


def extract_ad_details(html: str, page_url: str, fallback: Dict[str, Optional[str]]) -> Dict:
    soup = BeautifulSoup(html, "html.parser")

    title = ""
    for selector in ["h1", "meta[property='og:title']", "title"]:
        node = soup.select_one(selector)
        if not node:
            continue
        if selector.startswith("meta"):
            title = clean_text(node.get("content", ""))
        else:
            title = clean_text(node.get_text(" ", strip=True))
        if title:
            break

    seller_name = ""
    seller_url = None
    for selector in [
        "div.uad-content a[href*='/aprok/hirdeto/']",
        "a[href*='/aprok/hirdeto/'][style*='font-size']",
        "a[href*='/aprok/hirdeto/']",
    ]:
        node = soup.select_one(selector)
        if node:
            seller_name = clean_text(node.get_text(" ", strip=True))
            href = node.get("href")
            if href:
                seller_url = urljoin(page_url, href)
            if seller_name:
                break

    date_text = ""
    date_patterns = [
        r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b",
        r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}\b",
    ]

    candidate_texts: List[str] = []
    for selector in [
        "div.uad-content",
        "div.uad-content-block",
        "div.uad-time-location",
        "span[data-original-title='Feladás időpontja']",
        "body",
    ]:
        nodes = soup.select(selector)
        for node in nodes:
            txt = clean_text(node.get_text(" ", strip=True))
            if txt:
                candidate_texts.append(txt)

    for txt in candidate_texts:
        for pattern in date_patterns:
            m = re.search(pattern, txt)
            if m:
                date_text = clean_text(m.group(0))
                break
        if date_text:
            break

    content_nodes = []
    for selector in [
        "div.mb-3.trif-content",
        "div.trif-content",
        "div.uad-content-block div.mb-3.trif-content",
        "div.uad-content-block",
        "div.uad-content",
    ]:
        found = soup.select(selector)
        if found:
            content_nodes = found
            break

    content_text = get_longest_text(content_nodes) if content_nodes else ""

    if not content_text:
        paragraphs = soup.select("div.uad-content p, div.uad-content-block p")
        content_text = "\n\n".join(
            clean_text(p.get_text(" ", strip=True)) for p in paragraphs if clean_text(p.get_text(" ", strip=True))
        ).strip()

    if not content_text:
        body = soup.select_one("body")
        if body:
            content_text = clean_text(body.get_text("\n", strip=True))

    price = ""
    for selector in [".uad-price", ".price"]:
        node = soup.select_one(selector)
        if node:
            price = clean_text(node.get_text(" ", strip=True))
            if price:
                break

    data_map: Dict[str, str] = {}
    for row in soup.select("div.uad-details div.row"):
        cols = row.select("div")
        if len(cols) >= 2:
            key = clean_text(cols[0].get_text(" ", strip=True)).rstrip(":")
            value = clean_text(cols[-1].get_text(" ", strip=True))
            if key and value:
                data_map[key] = value

    breadcrumb = [clean_text(x.get_text(" ", strip=True)) for x in soup.select("ol.breadcrumb li, .breadcrumb li")]
    breadcrumb = [x for x in breadcrumb if x]

    resolved_uadid = fallback.get("uadid") or extract_uadid_from_url(page_url)

    return {
        "uadid": resolved_uadid,
        "title": title or fallback.get("title") or "",
        "seller_name": seller_name or fallback.get("listing_seller") or "",
        "seller_url": seller_url,
        "date": date_text or None,
        "price": price or fallback.get("price") or None,
        "url": page_url,
        "content": content_text,
        "listing_location": fallback.get("listing_location"),
        "details": data_map,
        "breadcrumb": breadcrumb,
    }


def ensure_output_files(base_output: Path) -> Tuple[Path, Path, Path]:
    hardverapro_dir = base_output / "hardverapro"
    json_file = hardverapro_dir / "hardverapro.json"
    visited_file = hardverapro_dir / "visited_hardverapro_ads.txt"

    hardverapro_dir.mkdir(parents=True, exist_ok=True)

    if not visited_file.exists():
        visited_file.write_text("", encoding="utf-8")

    return hardverapro_dir, json_file, visited_file


def load_visited(visited_file: Path) -> Set[str]:
    if not visited_file.exists():
        return set()
    return {line.strip() for line in visited_file.read_text(encoding="utf-8").splitlines() if line.strip()}


def append_visited(visited_file: Path, ad_key: str) -> None:
    with visited_file.open("a", encoding="utf-8") as f:
        f.write(ad_key.strip() + "\n")


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


def file_looks_closed_json(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return False
        if data.get("origin") != ORIGIN_NAME:
            return False
        extra = data.get("extra")
        if not isinstance(extra, dict):
            return False
        if extra.get("scrape_status") != "finished":
            return False
        ads = data.get("ads")
        if not isinstance(ads, list):
            return False
        return True
    except Exception:
        return False


def file_has_any_saved_ad(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False

    text = read_head_text(path, max_bytes=2 * 1024 * 1024)
    marker = '"ads": ['
    idx = text.find(marker)
    if idx == -1:
        return False

    after = text[idx + len(marker):].lstrip()
    if not after:
        return False
    return after.startswith("{") or '"uadid"' in after or '"url"' in after


def find_last_url_from_file(path: Path) -> Optional[str]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    text = read_tail_text(path, max_bytes=2 * 1024 * 1024)
    matches = URL_FIELD_RE.findall(text)
    return matches[-1] if matches else None


def find_last_uadid_from_file(path: Path) -> Optional[str]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    text = read_tail_text(path, max_bytes=2 * 1024 * 1024)
    matches = UADID_FIELD_RE.findall(text)
    if not matches:
        return None
    for quoted, numeric in reversed(matches):
        value = quoted or numeric
        value = clean_text(value)
        if value and value.lower() != "null":
            return value
    return None


def find_last_offset_from_file(path: Path) -> Optional[int]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    text = read_tail_text(path, max_bytes=2 * 1024 * 1024)
    matches = LAST_OFFSET_FIELD_RE.findall(text)
    if not matches:
        return None
    try:
        return int(matches[-1])
    except Exception:
        return None


def count_existing_ads_in_file(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    count = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            count += line.count('"uadid":')
    return count


def init_open_json_file_if_needed(json_file: Path) -> None:
    if json_file.exists() and json_file.stat().st_size > 0:
        return

    with json_file.open("w", encoding="utf-8") as f:
        f.write("{\n")
        f.write('  "title": "HardverApró összes hirdetés",\n')
        f.write('  "source_url": "https://hardverapro.hu/aprok/index.html?offset=0",\n')
        f.write(f'  "date_started": {json.dumps(now_local_iso(), ensure_ascii=False)},\n')
        f.write('  "ads": [\n')
        f.flush()
        os.fsync(f.fileno())


def append_ad_to_open_json(json_file: Path, ad: Dict, first_ad_already_written: bool) -> bool:
    with json_file.open("a", encoding="utf-8") as f:
        if first_ad_already_written:
            f.write(",\n")
        f.write("    ")
        f.write(json.dumps(ad, ensure_ascii=False, indent=4))
        f.flush()
        os.fsync(f.fileno())
    return True


def close_json_file(json_file: Path, saved_count: int, last_offset: int) -> None:
    with json_file.open("a", encoding="utf-8") as f:
        f.write("\n  ],\n")
        f.write(f'  "origin": {json.dumps(ORIGIN_NAME, ensure_ascii=False)},\n')
        f.write('  "extra": {\n')
        f.write('    "scrape_status": "finished",\n')
        f.write(f'    "saved_ad_count": {saved_count},\n')
        f.write(f'    "last_offset": {last_offset},\n')
        f.write(f'    "date_modified": {json.dumps(now_local_iso(), ensure_ascii=False)}\n')
        f.write("  }\n")
        f.write("}\n")
        f.flush()
        os.fsync(f.fileno())


def ad_key(ad: Dict) -> str:
    uadid = clean_text(str(ad.get("uadid") or ""))
    if uadid:
        return f"uadid:{uadid}"
    url = clean_text(ad.get("url") or "")
    if url:
        return f"url:{url}"
    title = clean_text(ad.get("title") or "")
    return f"title:{title}"


def normalize_ad_for_output(ad: Dict, offset: int, list_url: str) -> Dict:
    return {
        "uadid": ad.get("uadid"),
        "title": ad.get("title"),
        "seller_name": ad.get("seller_name"),
        "seller_url": ad.get("seller_url"),
        "date": ad.get("date"),
        "price": ad.get("price"),
        "url": ad.get("url"),
        "language": "hu",
        "tags": [],
        "content": ad.get("content"),
        "extra": {
            "list_offset": offset,
            "list_url": list_url,
            "listing_location": ad.get("listing_location"),
            "details": ad.get("details") or {},
            "breadcrumb": ad.get("breadcrumb") or [],
            "scraped_at": now_local_iso(),
        },
    }


def scrape_single_ad(
    driver: webdriver.Chrome,
    ad_meta: Dict[str, Optional[str]],
    delay: float,
) -> Dict:
    ad_url = ad_meta["url"]
    if not ad_url:
        raise ValueError("Hiányzó hirdetés URL.")

    print(f"[DEBUG] Hirdetés megnyitása: {ad_url}")
    driver.get(ad_url)
    wait_ready(driver)
    dismiss_known_popups(driver, first_page=False)
    wait_for_ad_page(driver)
    time.sleep(delay)

    details = extract_ad_details(driver.page_source, driver.current_url, ad_meta)

    preview = clean_text((details.get("content") or "")[:120]).replace("\n", " | ")
    print(
        f"[DEBUG] Kinyerve | uadid={details.get('uadid') or '-'} | "
        f"seller={details.get('seller_name') or '-'} | date={details.get('date') or '-'} | "
        f"preview={preview or '<üres>'}"
    )
    return details


def scrape_all_offsets(output_dir: str, delay: float, headless: bool, start_offset: int, max_empty_offsets: int) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    _, json_file, visited_file = ensure_output_files(base_output)

    if file_looks_closed_json(json_file):
        print(f"[INFO] A JSON már lezárt állapotban van: {json_file}")
        print("[INFO] Ha újra akarod futtatni, töröld a hardverapro.json és visited_hardverapro_ads.txt fájlokat.")
        return

    init_open_json_file_if_needed(json_file)

    driver = setup_driver(headless=headless)
    visited = load_visited(visited_file)
    first_ad_already_written = file_has_any_saved_ad(json_file)
    total_saved = count_existing_ads_in_file(json_file)
    empty_offsets_seen = 0
    first_page = True

    last_uadid = find_last_uadid_from_file(json_file)
    last_url = find_last_url_from_file(json_file)
    resume_offset = find_last_offset_from_file(json_file)
    if resume_offset is not None and resume_offset > start_offset:
        start_offset = resume_offset

    print(f"[INFO] Már mentett hirdetések a fájlban: {total_saved}")
    if last_uadid:
        print(f"[INFO] Utolsó mentett uadid: {last_uadid}")
    if last_url:
        print(f"[INFO] Utolsó mentett URL: {last_url}")
    print(f"[INFO] Induló offset: {start_offset}")

    try:
        offset = start_offset
        while True:
            list_url = build_list_url(offset)
            print(f"\n[INFO] Listaoldal megnyitása: {list_url}")

            try:
                driver.get(list_url)
                wait_ready(driver)
                dismiss_known_popups(driver, first_page=first_page)
                first_page = False
                time.sleep(delay)

                if page_has_ads(driver):
                    wait_for_listing_page(driver)
                elif page_has_no_results(driver):
                    empty_offsets_seen += 1
                    print(f"[INFO] Nincs találat ezen az offseten: {offset} | üres oldalak egymás után: {empty_offsets_seen}")
                    if empty_offsets_seen >= max_empty_offsets:
                        print("[INFO] Több egymás utáni üres oldal után leállok, valószínűleg elfogytak a hirdetések.")
                        break
                    offset += 100
                    continue
                else:
                    try:
                        wait_for_listing_page(driver, timeout=5)
                    except TimeoutException:
                        empty_offsets_seen += 1
                        print(f"[WARN] Nem találtam egyértelmű hirdetéslistát ezen az oldalon: {offset}")
                        if empty_offsets_seen >= max_empty_offsets:
                            break
                        offset += 100
                        continue

            except TimeoutException:
                print(f"[WARN] Timeout a listaoldal betöltésénél: {list_url}")
                offset += 100
                continue

            ads = parse_listing_ads(driver.page_source, driver.current_url)
            print(f"[INFO] Talált hirdetések száma ezen az oldalon: {len(ads)}")

            if not ads:
                empty_offsets_seen += 1
                print(f"[INFO] Üres listaoldal: {offset} | üres oldalak egymás után: {empty_offsets_seen}")
                if empty_offsets_seen >= max_empty_offsets:
                    print("[INFO] Több egymás utáni üres oldal miatt leállok.")
                    break
                offset += 100
                continue

            empty_offsets_seen = 0

            skip_until_last = bool(total_saved > 0 and (last_uadid or last_url) and offset == start_offset)
            seen_last_marker = False
            saved_on_this_page = 0

            for idx, ad_meta in enumerate(ads, start=1):
                current_key = ad_key(ad_meta)
                current_uadid = clean_text(str(ad_meta.get("uadid") or ""))
                current_url = clean_text(ad_meta.get("url") or "")

                if skip_until_last and not seen_last_marker:
                    if (last_uadid and current_uadid == last_uadid) or (last_url and current_url == last_url):
                        seen_last_marker = True
                        print(f"[INFO] Resume marker megtalálva ezen az oldalon: {current_uadid or current_url}")
                    continue

                if current_key in visited or (current_uadid and f"uadid:{current_uadid}" in visited) or (current_url and f"url:{current_url}" in visited):
                    print(f"[INFO] ({idx}/{len(ads)}) Már mentve, kihagyva: {ad_meta.get('title')}")
                    continue

                print(f"\n[INFO] ({idx}/{len(ads)}) Hirdetés feldolgozása: {ad_meta.get('title')}")

                try:
                    scraped = scrape_single_ad(driver, ad_meta, delay)
                    output_ad = normalize_ad_for_output(scraped, offset=offset, list_url=list_url)
                    first_ad_already_written = append_ad_to_open_json(json_file, output_ad, first_ad_already_written)
                    key = ad_key(output_ad)
                    append_visited(visited_file, key)
                    visited.add(key)
                    if output_ad.get("uadid"):
                        visited.add(f"uadid:{output_ad['uadid']}")
                    if output_ad.get("url"):
                        visited.add(f"url:{output_ad['url']}")
                    total_saved += 1
                    saved_on_this_page += 1
                    print(f"[INFO] Hirdetés appendelve | összes mentett eddig: {total_saved}")
                except TimeoutException:
                    print(f"[WARN] Timeout a hirdetésnél: {ad_meta.get('url')}")
                except WebDriverException as e:
                    print(f"[WARN] Selenium hiba a hirdetésnél: {ad_meta.get('url')} | {e}")
                except Exception as e:
                    print(f"[WARN] Váratlan hiba a hirdetésnél: {ad_meta.get('url')} | {e}")

                try:
                    driver.get(list_url)
                    wait_ready(driver)
                    dismiss_known_popups(driver, first_page=False)
                    wait_for_listing_page(driver)
                    time.sleep(delay)
                except Exception as e:
                    print(f"[WARN] Nem sikerült visszamenni a listaoldalra: {e}")
                    break

            print(f"[INFO] Oldal kész | offset={offset} | újonnan mentett hirdetések ezen az oldalon: {saved_on_this_page}")
            offset += 100

        close_json_file(json_file, saved_count=total_saved, last_offset=max(start_offset, offset - 100))
        print(f"[INFO] Kész. JSON lezárva: {json_file} | összes mentett hirdetés: {total_saved}")
    finally:
        driver.quit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="HardverApró hirdetés scraper Seleniummal, egy nagy appendelt hardverapro.json fájlba mentéssel."
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre a hardverapro mappa. Alapértelmezett: aktuális mappa.",
    )
    parser.add_argument("--start-offset", type=int, default=0, help="Kezdő offset. Alapértelmezett: 0")
    parser.add_argument("--delay", type=float, default=1.5, help="Várakozás oldalak között másodpercben.")
    parser.add_argument("--headless", action="store_true", help="Headless mód.")
    parser.add_argument(
        "--max-empty-offsets",
        type=int,
        default=3,
        help="Ennyi egymás utáni üres offset után áll le. Alapértelmezett: 3",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.start_offset < 0:
        print("A --start-offset nem lehet negatív.")
        sys.exit(1)

    if args.start_offset % 100 != 0:
        print("A --start-offset legyen 100-zal osztható: 0, 100, 200, ...")
        sys.exit(1)

    if args.max_empty_offsets < 1:
        print("A --max-empty-offsets legalább 1 legyen.")
        sys.exit(1)

    scrape_all_offsets(
        output_dir=args.output,
        delay=args.delay,
        headless=args.headless,
        start_offset=args.start_offset,
        max_empty_offsets=args.max_empty_offsets,
    )


if __name__ == "__main__":
    main()

    # python hardverapro_scraper.py --output . --delay 3 --headless
