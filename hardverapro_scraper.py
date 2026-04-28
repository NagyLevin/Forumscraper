#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
RIGHTS_TEXT = "hardverapro.hu hirdetés tartalom"

URL_FIELD_RE = re.compile(r'"url"\s*:\s*"([^"]+)"')
COMMENT_ID_RE = re.compile(r'"comment_id"\s*:\s*"([^"]+)"')
LAST_OFFSET_FIELD_RE = re.compile(r'"list_offset"\s*:\s*(\d+)')


def build_list_url(offset: int) -> str:
    return BASE_LIST_URL.format(offset=offset)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
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


def setup_driver(headless: bool = False, page_load_timeout: int = 20) -> webdriver.Chrome:
    options = Options()

    # FONTOS GYORSÍTÁS:
    # normal helyett eager: nem várja meg az összes kép/reklám/tracker betöltését,
    # csak azt, hogy a DOM már olvasható legyen.
    options.page_load_strategy = "eager"

    if headless:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1600,1200")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--lang=hu-HU")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-sync")
    options.add_argument("--metrics-recording-only")
    options.add_argument("--mute-audio")

    # Képek tiltása: scraperhez nem kell, viszont sok időt visz el.
    options.add_experimental_option(
        "prefs",
        {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
        },
    )

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(page_load_timeout)
    return driver


def wait_ready(driver: webdriver.Chrome, timeout: int = 8) -> None:
    # eager page load mellett elég az interactive vagy complete állapot.
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
    )
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )


def safe_click(driver: webdriver.Chrome, element) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(0.05)
        try:
            element.click()
        except Exception:
            driver.execute_script("arguments[0].click();", element)
        return True
    except Exception:
        return False


def click_first_visible(driver: webdriver.Chrome, xpaths: List[str], timeout: float = 0.7) -> bool:
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
                    time.sleep(0.15)
                    return True
        time.sleep(0.05)
    return False


def reject_cookies(driver: webdriver.Chrome, timeout: float = 2.0) -> bool:
    xpaths = [
        "//*[self::button or self::a or self::span][normalize-space()='NEM FOGADOM EL']",
        "//*[contains(translate(normalize-space(), 'abcdefghijklmnopqrstuvwxyzáéíóöőúüű', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÖŐÚÜŰ'), 'NEM FOGADOM EL')]",
        "//*[contains(@class,'cookie')]//*[contains(translate(normalize-space(), 'abcdefghijklmnopqrstuvwxyzáéíóöőúüű', 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÖŐÚÜŰ'), 'NEM FOGADOM EL')]",
    ]
    clicked = click_first_visible(driver, xpaths, timeout=timeout)
    if clicked:
        print("[DEBUG] Sütik elutasítva.")
    return clicked


def close_skip_popup(driver: webdriver.Chrome, timeout: float = 0.4) -> bool:
    xpaths = [
        "//*[self::button or self::a or self::span][normalize-space()='Lemaradok']",
        "//*[contains(normalize-space(), 'Lemaradok')]",
        "//input[@type='button' and @value='Lemaradok']",
    ]
    clicked = click_first_visible(driver, xpaths, timeout=timeout)
    if clicked:
        print("[DEBUG] Lemaradok popup bezárva.")
    return clicked


def dismiss_known_popups(driver: webdriver.Chrome, first_page: bool = False, popup_timeout: float = 0.4) -> None:
    # Régen ez minden oldalnál akár 3 mp-et várt feleslegesen.
    # Most csak röviden néz rá, mert ha nincs popup, nem kell másodperceket elvesztegetni.
    if first_page:
        reject_cookies(driver, timeout=max(1.0, popup_timeout))
    close_skip_popup(driver, timeout=popup_timeout)


def wait_for_listing_page(driver: webdriver.Chrome, timeout: int = 8) -> None:
    selector = ", ".join(
        [
            "li.media[data-uadid] h1 a[href*='/aprok/']",
            "li.media[data-uadid] a[href*='/aprok/'][href$='.html']",
            "main li.media[data-uadid]",
        ]
    )
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
    )

def page_has_ads(driver: webdriver.Chrome) -> bool:
    try:
        return len(driver.find_elements(By.CSS_SELECTOR, "li.media[data-uadid]")) > 0
    except Exception:
        return False


def page_has_no_results(driver: webdriver.Chrome) -> bool:
    try:
        body_text = clean_text(driver.find_element(By.TAG_NAME, "body").text).lower()
    except Exception:
        return False
    phrases = ["nincs találat", "nem található hirdetés", "nincs több hirdetés", "nincsenek hirdetések"]
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

        seller_link = (
            item.select_one("a[href*='/aprok/hirdeto/'][href*='uadid=']")
            or item.select_one("a[href*='/aprok/hirdeto/']")
        )

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


def wait_for_ad_page(driver: webdriver.Chrome, timeout: int = 8) -> None:
    selector = ", ".join(
        [
            "div.uad-content div.mb-3.trif-content",
            "div.uad-content",
            "div.uad-content-block",
            "div.trif-content",
            "a[href*='/aprok/hirdeto/']",
        ]
    )
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
    )

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


def extract_main_ad_text(soup: BeautifulSoup) -> str:
    selectors = [
        "div.uad-content div.mb-3.trif-content",
        "div.uad-content .trif-content",
        "div.mb-3.trif-content",
        "div.trif-content",
    ]

    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = clean_text(node.get_text("\n", strip=True))
            if text:
                return text

    fallback_nodes = soup.select("div.uad-content p, div.uad-content-block p")
    parts = []
    for node in fallback_nodes:
        text = clean_text(node.get_text("\n", strip=True))
        if text:
            parts.append(text)

    if parts:
        return clean_text("\n\n".join(parts))

    node = soup.select_one("div.uad-content")
    if node:
        return clean_text(node.get_text("\n", strip=True))

    return ""


def extract_ad_details(html: str, page_url: str, fallback: Dict[str, Optional[str]]) -> Dict:
    soup = BeautifulSoup(html, "html.parser")

    # ------------------------
    # Title
    # ------------------------
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

    # ------------------------
    # Seller 
    # ------------------------
    seller_name = ""
    seller_url = None

    # Végtelenül leegyszerűsített keresés, ami pontosan azt csinálja, amit kértél: 
    # Megkeresi a "Hirdető" feliratot, és a közvetlenül mellette lévő linket veszi ki.
    hirdeto_nodes = soup.find_all(string=re.compile(r"Hirdető"))
    for node in hirdeto_nodes:
        parent = node.parent
        if not parent:
            continue
            
        a_tag = parent.find("a", href=True)
        if a_tag:
            seller_name = clean_text(a_tag.get_text(" ", strip=True))
            seller_url = urljoin(page_url, a_tag.get("href"))
            break

    if not seller_name:
        seller_name = fallback.get("listing_seller") or ""

    # ------------------------
    # Date
    # ------------------------
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
        for node in soup.select(selector):
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

    # ------------------------
    # Price
    # ------------------------
    price = ""
    for selector in [".uad-price", ".price"]:
        node = soup.select_one(selector)
        if node:
            price = clean_text(node.get_text(" ", strip=True))
            if price:
                break

    # ------------------------
    # Details table
    # ------------------------
    details_map: Dict[str, str] = {}

    for row in soup.select("div.uad-details div.row"):
        cols = row.select("div")

        if len(cols) >= 2:
            key = clean_text(cols[0].get_text(" ", strip=True)).rstrip(":")
            value = clean_text(cols[-1].get_text(" ", strip=True))

            if key and value:
                details_map[key] = value

    # ------------------------
    # Breadcrumb
    # ------------------------
    breadcrumb = [
        clean_text(x.get_text(" ", strip=True))
        for x in soup.select("ol.breadcrumb li, .breadcrumb li")
    ]
    breadcrumb = [x for x in breadcrumb if x]

    # ------------------------
    # Content 
    # ------------------------
    content_text = extract_main_ad_text(soup)

    # ------------------------
    # UADID
    # ------------------------
    resolved_uadid = fallback.get("uadid") or extract_uadid_from_url(page_url)

    return {
        "uadid": resolved_uadid,
        "title": title or fallback.get("title") or "",
        "seller_name": seller_name,
        "seller_url": seller_url,
        "date": date_text or None,
        "price": price or fallback.get("price") or None,
        "url": page_url,
        "content": content_text,
        "listing_location": fallback.get("listing_location"),
        "details": details_map,
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


def append_visited(visited_file: Path, ad_key_value: str) -> None:
    with visited_file.open("a", encoding="utf-8") as f:
        f.write(ad_key_value.strip() + "\n")


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
        if not isinstance(data.get("comments"), list):
            return False
        extra = data.get("extra", {})
        if not isinstance(extra, dict):
            return False
        return extra.get("scrape_status") == "finished"
    except Exception:
        return False


def file_has_any_saved_comment(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    text = read_head_text(path, max_bytes=2 * 1024 * 1024)
    marker = '"comments": ['
    idx = text.find(marker)
    if idx == -1:
        return False
    after = text[idx + len(marker):].lstrip()
    return bool(after and (after.startswith("{") or '"comment_id"' in after or '"url"' in after))


def find_last_url_from_file(path: Path) -> Optional[str]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    text = read_tail_text(path, max_bytes=2 * 1024 * 1024)
    matches = URL_FIELD_RE.findall(text)
    return matches[-1] if matches else None


def find_last_comment_id_from_file(path: Path) -> Optional[str]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    text = read_tail_text(path, max_bytes=2 * 1024 * 1024)
    matches = COMMENT_ID_RE.findall(text)
    return matches[-1] if matches else None


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


def count_existing_comments_in_file(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0
    count = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            count += line.count('"comment_id":')
    return count


def init_open_json_file_if_needed(json_file: Path) -> None:
    if json_file.exists() and json_file.stat().st_size > 0:
        return

    root_data = {
        "content": "hardverapro",
        "likes": None,
        "dislikes": None,
        "score": None,
        "rating": None,
        "date": None,
        "url": START_URL,
        "language": "hu",
        "tags": [],
        "rights": RIGHTS_TEXT,
        "date_modified": now_local_iso(),
        "extra": {
            "scrape_status": "running",
            "detected_total_comments": None,
            "fetched_page": None,
        },
        "origin": ORIGIN_NAME,
    }

    with json_file.open("w", encoding="utf-8") as f:
        f.write("{\n")
        f.write('  "title": "hardverapro",\n')
        f.write('  "authors": [],\n')
        f.write('  "data": ')
        f.write(json.dumps(root_data, ensure_ascii=False, indent=2).replace("\n", "\n  "))
        f.write(",\n")
        f.write(f'  "origin": {json.dumps(ORIGIN_NAME, ensure_ascii=False)},\n')
        f.write('  "comments": [\n')
        f.flush()
        os.fsync(f.fileno())


def append_comment_to_open_json(json_file: Path, comment: Dict, first_comment_already_written: bool, do_fsync: bool = False) -> bool:
    with json_file.open("a", encoding="utf-8") as f:
        if first_comment_already_written:
            f.write(",\n")
        comment_json = json.dumps(comment, ensure_ascii=False, indent=4)
        f.write("    " + comment_json.replace("\n", "\n    "))
        f.flush()
        if do_fsync:
            os.fsync(f.fileno())
    return True


def close_json_file(json_file: Path, saved_count: int, last_offset: int) -> None:
    closing_data = {
        "scrape_status": "finished",
        "saved_comment_count": saved_count,
        "last_offset": last_offset,
        "date_modified": now_local_iso(),
    }
    with json_file.open("a", encoding="utf-8") as f:
        f.write("\n  ],\n")
        f.write('  "extra": ')
        f.write(json.dumps(closing_data, ensure_ascii=False, indent=2).replace("\n", "\n  "))
        f.write("\n}\n")
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


def normalize_author(name: str) -> Dict[str, str]:
    name = clean_text(name)
    if not name:
        return {"name": ""}
    return {"name": name}


def normalize_ad_as_comment(ad: Dict, offset: int, list_url: str, index_total: Optional[int]) -> Dict:
    uadid = clean_text(str(ad.get("uadid") or "")) or None
    title = clean_text(ad.get("title") or "")
    seller_name = clean_text(ad.get("seller_name") or "")
    comment_id = uadid or sanitize_filename(title)

    authors = []
    if seller_name:
        authors.append(normalize_author(seller_name))

    return {
        "authors": authors,
        "data": clean_text(ad.get("content") or ""),
        "likes": None,
        "dislikes": None,
        "score": None,
        "rating": None,
        "date": ad.get("date"),
        "url": ad.get("url"),
        "language": "hu",
        "tags": [],
        "extra": {
            "comment_id": comment_id,
            "dom_comment_id": uadid,
            "dom_id": f"uad{uadid}" if uadid else None,
            "parent_author": None,
            "index": int(uadid) if uadid and uadid.isdigit() else None,
            "index_total": index_total,
            "is_offtopic": False,
            "title": title,
            "seller_name": seller_name or None,
            "seller_url": ad.get("seller_url"),
            "price": ad.get("price"),
            "listing_location": ad.get("listing_location"),
            "details": ad.get("details") or {},
            "breadcrumb": ad.get("breadcrumb") or [],
            "list_offset": offset,
            "list_url": list_url,
            "rights": RIGHTS_TEXT,
            "origin": ORIGIN_NAME,
            "scraped_at": now_local_iso(),
        },
    }


def scrape_single_ad(driver: webdriver.Chrome, ad_meta: Dict[str, Optional[str]], delay: float, page_timeout: int, popup_timeout: float) -> Dict:
    ad_url = ad_meta["url"]
    if not ad_url:
        raise ValueError("Hiányzó hirdetés URL.")

    print(f"[DEBUG] Hirdetés megnyitása: {ad_url}")
    t0 = time.perf_counter()

    try:
        driver.get(ad_url)
    except TimeoutException:
        print(f"[WARN] Page-load timeout, de megpróbálom feldolgozni: {ad_url}")

    t_get = time.perf_counter()
    wait_ready(driver, timeout=page_timeout)
    t_ready = time.perf_counter()
    dismiss_known_popups(driver, first_page=False, popup_timeout=popup_timeout)
    t_popup = time.perf_counter()
    wait_for_ad_page(driver, timeout=page_timeout)
    t_wait = time.perf_counter()

    if delay > 0:
        time.sleep(delay)

    details = extract_ad_details(driver.page_source, driver.current_url, ad_meta)
    t_extract = time.perf_counter()
    preview = clean_text((details.get("content") or "")[:160]).replace("\n", " | ")

    print(
        f"[DEBUG] Kinyerve | uadid={details.get('uadid') or '-'} | "
        f"seller={details.get('seller_name') or '-'} | "
        f"date={details.get('date') or '-'} | "
        f"preview={preview or '<üres>'}"
    )
    print(
        "[TIME] ad_get={:.2f}s | ready={:.2f}s | popup={:.2f}s | wait_ad={:.2f}s | extract+delay={:.2f}s | total={:.2f}s".format(
            t_get - t0,
            t_ready - t_get,
            t_popup - t_ready,
            t_wait - t_popup,
            t_extract - t_wait,
            t_extract - t0,
        )
    )

    return details


def scrape_all_offsets(output_dir: str, delay: float, headless: bool, start_offset: int, max_empty_offsets: int, page_timeout: int, popup_timeout: float, do_fsync: bool) -> None:
    base_output = Path(output_dir).expanduser().resolve()
    _, json_file, visited_file = ensure_output_files(base_output)

    if file_looks_closed_json(json_file):
        print(f"[INFO] A JSON már lezárt állapotban van: {json_file}")
        print("[INFO] Ha újra akarod futtatni, töröld a hardverapro.json és visited_hardverapro_ads.txt fájlokat.")
        return

    init_open_json_file_if_needed(json_file)

    driver = setup_driver(headless=headless, page_load_timeout=page_timeout)
    visited = load_visited(visited_file)
    first_comment_already_written = file_has_any_saved_comment(json_file)
    total_saved = count_existing_comments_in_file(json_file)
    empty_offsets_seen = 0
    first_page = True

    last_comment_id = find_last_comment_id_from_file(json_file)
    last_url = find_last_url_from_file(json_file)
    resume_offset = find_last_offset_from_file(json_file)

    if resume_offset is not None and resume_offset > start_offset:
        start_offset = resume_offset

    print(f"[INFO] Már mentett hirdetések a fájlban: {total_saved}")
    if last_comment_id:
        print(f"[INFO] Utolsó mentett comment_id: {last_comment_id}")
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
                wait_ready(driver, timeout=page_timeout)
                dismiss_known_popups(driver, first_page=first_page, popup_timeout=popup_timeout)
                first_page = False
                if delay > 0:
                    time.sleep(delay)

                if page_has_ads(driver):
                    wait_for_listing_page(driver, timeout=page_timeout)
                elif page_has_no_results(driver):
                    empty_offsets_seen += 1
                    print(f"[INFO] Nincs találat ezen az offseten: {offset} | üres oldalak egymás után: {empty_offsets_seen}")
                    if empty_offsets_seen >= max_empty_offsets:
                        print("[INFO] Több egymás utáni üres oldal után leállok.")
                        break
                    offset += 100
                    continue
                else:
                    try:
                        wait_for_listing_page(driver, timeout=min(5, page_timeout))
                    except TimeoutException:
                        empty_offsets_seen += 1
                        print(f"[WARN] Nem találtam hirdetéslistát ezen az oldalon: {offset}")
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
            skip_until_last = bool(total_saved > 0 and (last_comment_id or last_url) and offset == start_offset)
            seen_last_marker = False
            saved_on_this_page = 0

            for idx, ad_meta in enumerate(ads, start=1):
                current_key = ad_key(ad_meta)
                current_uadid = clean_text(str(ad_meta.get("uadid") or ""))
                current_url = clean_text(ad_meta.get("url") or "")

                if skip_until_last and not seen_last_marker:
                    if (last_comment_id and current_uadid and current_uadid == last_comment_id) or (last_url and current_url == last_url):
                        seen_last_marker = True
                        print(f"[INFO] Resume marker megtalálva: {current_uadid or current_url}")
                    continue

                if (
                    current_key in visited
                    or (current_uadid and f"uadid:{current_uadid}" in visited)
                    or (current_url and f"url:{current_url}" in visited)
                ):
                    print(f"[INFO] ({idx}/{len(ads)}) Már mentve, kihagyva: {ad_meta.get('title')}")
                    continue

                print(f"\n[INFO] ({idx}/{len(ads)}) Hirdetés feldolgozása: {ad_meta.get('title')}")

                try:
                    scraped = scrape_single_ad(driver, ad_meta, delay, page_timeout=page_timeout, popup_timeout=popup_timeout)
                    output_comment = normalize_ad_as_comment(scraped, offset=offset, list_url=list_url, index_total=None)
                    first_comment_already_written = append_comment_to_open_json(
                        json_file,
                        output_comment,
                        first_comment_already_written,
                        do_fsync=do_fsync,
                    )

                    key = ad_key(scraped)
                    append_visited(visited_file, key)
                    visited.add(key)

                    if scraped.get("uadid"):
                        visited.add(f"uadid:{scraped['uadid']}")
                    if scraped.get("url"):
                        visited.add(f"url:{scraped['url']}")

                    total_saved += 1
                    saved_on_this_page += 1
                    print(f"[INFO] Hirdetés appendelve | összes mentett eddig: {total_saved}")

                except TimeoutException:
                    print(f"[WARN] Timeout a hirdetésnél: {ad_meta.get('url')}")
                except WebDriverException as e:
                    print(f"[WARN] Selenium hiba a hirdetésnél: {ad_meta.get('url')} | {e}")
                except Exception as e:
                    print(f"[WARN] Váratlan hiba a hirdetésnél: {ad_meta.get('url')} | {e}")

            print(f"[INFO] Oldal kész | offset={offset} | újonnan mentett hirdetések ezen az oldalon: {saved_on_this_page}")
            offset += 100

        close_json_file(json_file, saved_count=total_saved, last_offset=max(start_offset, offset - 100))
        print(f"[INFO] Kész. JSON lezárva: {json_file} | összes mentett hirdetés: {total_saved}")

    finally:
        driver.quit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="HardverApró scraper Hoxa-szerű JSON formátummal, comments tömbbe mentve."
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Kimeneti alapmappa. Ide jön létre a hardverapro mappa. Alapértelmezett: aktuális mappa.",
    )
    parser.add_argument("--start-offset", type=int, default=0, help="Kezdő offset. Alapértelmezett: 0.")
    parser.add_argument("--delay", type=float, default=1.5, help="Várakozás oldalak között másodpercben.")
    parser.add_argument("--headless", action="store_true", help="Headless mód.")
    parser.add_argument("--page-timeout", type=int, default=8, help="Várakozási timeout oldalelemekre és oldalbetöltésre. Alap: 8 mp.")
    parser.add_argument("--popup-timeout", type=float, default=0.4, help="Popup keresési idő. Alap: 0.4 mp.")
    parser.add_argument("--fsync", action="store_true", help="Minden mentés után fizikai lemezre flush. Biztonságosabb, de lassabb.")
    parser.add_argument(
        "--max-empty-offsets",
        type=int,
        default=3,
        help="Ennyi egymás utáni üres offset után áll le. Alapértelmezett: 3.",
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
        page_timeout=args.page_timeout,
        popup_timeout=args.popup_timeout,
        do_fsync=args.fsync,
    )


if __name__ == "__main__":
    main()