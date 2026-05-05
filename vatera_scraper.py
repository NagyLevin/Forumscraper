#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import gc
import json
import math
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright


# ==================================================
# Itt lehet alapból paraméterezni a Vatera kategória URL-t
# CLI-ből felülírható: python vatera_scraper.py --url "https://..."
# ==================================================
DEFAULT_LIST_URL = "https://www.vatera.hu/muszaki-cikk-es-mobil/index-c12082.html"
BASE_URL = "https://www.vatera.hu"

AD_ID_RE = re.compile(r'"ad_id"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"')
AD_URL_RE = re.compile(r'"url"\s*:\s*"([^"\\]*(?:\\.[^"\\]*)*)"')
DATE_RE = re.compile(r"\d{4}\.\d{2}\.\d{2}\.?\s+\d{2}:\d{2}:\d{2}")


# --------------------------------------------------
# Segédfüggvények
# --------------------------------------------------

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_ws_inline(text: str) -> str:
    return clean_text(text).replace("\n", " ").strip()


def short_preview(text: str, max_len: int = 140) -> str:
    txt = normalize_ws_inline(text)
    if len(txt) <= max_len:
        return txt
    return txt[: max_len - 3].rstrip() + "..."


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


def strip_fragment(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))


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
            parsed.fragment,
        )
    )


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


def build_list_page_url(base_url: str, page_no: int) -> str:
    """Vatera lapozás: ?p=2, ?p=3, stb. Az első oldalnál eltávolítjuk a p paramétert."""
    base_url = strip_fragment(base_url)
    if page_no <= 1:
        return remove_query_param(base_url, "p")
    return set_query_param(base_url, "p", str(page_no))


def parse_int_from_text(text: str) -> Optional[int]:
    text = clean_text(text)
    if not text:
        return None
    normalized = text.replace(".", "").replace(" ", "")
    m = re.search(r"\d+", normalized)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def normalize_ad_url(url: str) -> str:
    return strip_fragment(url)


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def split_name_like_person(name: str) -> Dict[str, str]:
    """A példakód szerinti authors mezőhöz egyszerű bontás."""
    name = clean_text(name)
    if not name:
        return {"name": ""}

    parts = name.split()
    if len(parts) >= 2:
        return {"family": parts[0], "given": " ".join(parts[1:])}
    return {"name": name}


def extract_ad_id_from_url(url: str) -> str:
    """Vatera termék URL-ek végén gyakran ott a numerikus termékkód: ..._3502347080.html"""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if not path:
        return ""

    m = re.search(r"_(\d+)\.html$", path)
    if m:
        return m.group(1)

    nums = re.findall(r"\d+", path)
    return nums[-1] if nums else clean_text(path.split("/")[-1])


def looks_like_vatera_product_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False

    host = parsed.netloc.lower()
    path = parsed.path.lower()

    if host and "vatera.hu" not in host:
        return False

    if not path.endswith(".html"):
        return False

    blocked_parts = [
        "/user/",
        "/help/",
        "/login",
        "/belepes",
        "/registration",
        "/index-c",
    ]
    if any(part in path for part in blocked_parts):
        return False

    # A kategóriaoldalak általában index-c123.html formájúak, ezek nem hirdetések.
    if re.search(r"/index-c\d+\.html$", path):
        return False

    return True


# --------------------------------------------------
# Adatmodellek
# --------------------------------------------------

@dataclass
class AdCard:
    title: str
    url: str
    ad_id: str = ""


@dataclass
class AdDetails:
    title: str
    date: str
    seller: str
    description: str
    url: str
    ad_id: str = ""


# --------------------------------------------------
# Állapot / output
# --------------------------------------------------

def ensure_dirs(base_output: Path) -> Dict[str, Path]:
    root = base_output / "vatera"
    topics_dir = root / "topics"
    state_dir = root / "state"

    root.mkdir(parents=True, exist_ok=True)
    topics_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    visited_ads = state_dir / "visited_ads.txt"
    if not visited_ads.exists():
        visited_ads.write_text("", encoding="utf-8")

    return {
        "root": root,
        "topics": topics_dir,
        "state": state_dir,
        "visited_ads": visited_ads,
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


def topic_file_path(topics_dir: Path, topic_name: str) -> Path:
    return topics_dir / f"{sanitize_filename(topic_name)}.json"


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


def count_existing_ads_in_stream_file(topic_file: Path) -> int:
    if not topic_file.exists():
        return 0

    count = 0
    with topic_file.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            count += line.count('"ad_id":')
    return count


def get_last_written_ad_info(topic_file: Path) -> Tuple[Optional[str], Optional[str], int]:
    if not topic_file.exists():
        return None, None, 0

    existing_count = count_existing_ads_in_stream_file(topic_file)

    try:
        with topic_file.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            read_size = min(size, 1024 * 1024)
            f.seek(max(0, size - read_size))
            tail = f.read().decode("utf-8", errors="ignore")
    except Exception:
        return None, None, existing_count

    ad_ids = AD_ID_RE.findall(tail)
    urls = AD_URL_RE.findall(tail)

    last_ad_id = ad_ids[-1] if ad_ids else None
    last_ad_url = urls[-1] if urls else None

    return last_ad_id, last_ad_url, existing_count


def reopen_finalized_stream_json_for_append(topic_file: Path) -> None:
    if not topic_file.exists():
        return

    content = topic_file.read_text(encoding="utf-8", errors="ignore")
    new_content = re.sub(r"\n\s*\]\s*\}\s*$", "", content.rstrip(), flags=re.S)
    topic_file.write_text(new_content.rstrip() + "\n", encoding="utf-8")


def write_topic_stream_header(
    topic_file: Path,
    resolved_title: str,
    topic_url: str,
    start_page: int,
    end_page: Optional[int],
) -> None:
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
            "url": strip_fragment(topic_url),
            "language": "hu",
            "tags": [],
            "rights": "Vatera hirdetés tartalom",
            "date_modified": now_iso(),
            "extra": {
                "start_page": start_page,
                "end_page": end_page,
            },
            "origin": "vatera",
        },
        "origin": "vatera",
    }

    header_json = json.dumps(header_obj, ensure_ascii=False, indent=2)
    if not header_json.endswith("}"):
        raise RuntimeError("Hibás header JSON generálás.")

    text = header_json[:-1].rstrip() + ',\n  "ads": [\n'
    topic_file.write_text(text, encoding="utf-8")


def append_ad_to_stream_file(topic_file: Path, ad_item: Dict, has_existing_ads: bool) -> None:
    item_json = json.dumps(ad_item, ensure_ascii=False, indent=2)
    item_json = "\n".join("    " + line if line.strip() else line for line in item_json.splitlines())

    with topic_file.open("a", encoding="utf-8") as f:
        if has_existing_ads:
            f.write(",\n")
        f.write(item_json)


def finalize_stream_json(topic_file: Path) -> None:
    if is_stream_json_finalized(topic_file):
        return
    with topic_file.open("a", encoding="utf-8") as f:
        f.write("\n  ]\n}\n")


def ad_to_output_item(ad: AdDetails) -> Dict:
    seller_name = ad.seller or "ismeretlen eladó"
    ad_id = ad.ad_id or extract_ad_id_from_url(ad.url)

    return {
        "title": ad.title,
        "authors": [split_name_like_person(seller_name)] if seller_name else [],
        "data": ad.description,
        "likes": None,
        "dislikes": None,
        "score": None,
        "rating": None,
        "date": ad.date,
        "url": ad.url,
        "language": "hu",
        "tags": [],
        "extra": {
            "seller": ad.seller,
            "ad_id": ad_id,
        },
    }


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
        block_resources: bool = False,
        auto_reset_fetches: int = 100,
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
            viewport={"width": 1600, "height": 2400},
        )

        if self.block_resources:
            def route_handler(route):
                try:
                    req = route.request
                    if req.resource_type in {"media", "font", "image"}:
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
        """
        Cookiebot/Usercentrics sütiablak elfogadása.
        Vaterán gyakran Cookiebot jelenik meg az „Összes süti engedélyezése” gombbal.
        Ha ezt nem zárjuk be, a HTML-ben és a látható oldalon is zavarhatja a parszolást.
        """

        def try_click_locator(locator, label: str) -> bool:
            try:
                if locator.count() == 0:
                    return False
                first = locator.first
                if not first.is_visible(timeout=800):
                    return False
                first.click(timeout=2500, force=True)
                self.page.wait_for_timeout(1000)
                print(f"[INFO] Sütiablak bezárva: {label}")
                return True
            except Exception:
                return False

        selectors = [
            # Cookiebot ismert gomb ID-k
            "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
            "#CybotCookiebotDialogBodyButtonAccept",
            "#CybotCookiebotDialogBodyLevelButtonAccept",
            "#CybotCookiebotDialogBodyLevelButtonCustomize",
            # Usercentrics / OneTrust / Didomi fallbackok
            "[data-testid='uc-accept-all-button']",
            "button#onetrust-accept-btn-handler",
            "#didomi-notice-agree-button",
            "button[aria-label*='elfogad' i]",
            "button[aria-label*='allow' i]",
        ]
        for sel in selectors:
            if try_click_locator(self.page.locator(sel), sel):
                return

        texts = [
            "Összes süti engedélyezése",
            "Összes cookie engedélyezése",
            "Elfogadom az összeset",
            "Elfogadom mindet",
            "Összes elfogadása",
            "Elfogadom",
            "Elfogadás",
            "Accept all",
            "Allow all",
            "Rendben",
            "Egyetértek",
            "Megértettem",
        ]
        for txt in texts:
            # pontosabb gomb keresés
            if try_click_locator(self.page.get_by_role("button", name=re.compile(re.escape(txt), re.I)), txt):
                return
            # fallback sima szöveg alapján
            if try_click_locator(self.page.locator(f"text={txt}"), txt):
                return

        # Ha iframe-ben lenne a consent panel.
        for frame in self.page.frames:
            try:
                for txt in texts:
                    locator = frame.get_by_role("button", name=re.compile(re.escape(txt), re.I))
                    if locator.count() > 0 and locator.first.is_visible(timeout=500):
                        locator.first.click(timeout=2500, force=True)
                        self.page.wait_for_timeout(1000)
                        print(f"[INFO] Sütiablak bezárva iframe-ben: {txt}")
                        return
            except Exception:
                pass

    def fetch(self, url: str, wait_ms: int = 2000) -> Tuple[str, str]:
        last_exc = None

        if self.auto_reset_fetches > 0 and self.fetch_counter > 0 and self.fetch_counter % self.auto_reset_fetches == 0:
            print("[INFO] Browser context újranyitva memória-kíméléshez.")
            self.reset_context()

        for attempt in range(1, self.retries + 1):
            try:
                self.ensure_page_alive()
                self.page.goto(url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                self.page.wait_for_timeout(wait_ms)
                self.dismiss_overlays_if_present()

                try:
                    self.page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass

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
# Listaoldal parse
# --------------------------------------------------

def parse_total_items(html: str) -> Optional[int]:
    soup = BeautifulSoup(html, "html.parser")
    text = clean_text(soup.get_text("\n", strip=True))
    del soup

    # Példa a képen: "1. oldal / 2407 összesen".
    m = re.search(r"/\s*([\d .]+)\s+összesen", text, flags=re.I)
    if m:
        return parse_int_from_text(m.group(1))

    m = re.search(r"([\d .]+)\s+összesen", text, flags=re.I)
    if m:
        return parse_int_from_text(m.group(1))

    return None


def parse_max_page_from_pagination_links(html: str, page_url: str) -> Optional[int]:
    soup = BeautifulSoup(html, "html.parser")
    nums: List[int] = []

    for a in soup.select('a[href]'):
        href = a.get("href") or ""
        abs_url = urljoin(page_url, href)
        q = parse_qs(urlparse(abs_url).query)
        p_vals = q.get("p") or []
        for val in p_vals:
            try:
                nums.append(int(val))
            except ValueError:
                pass

        txt = clean_text(a.get_text(" ", strip=True))
        if re.fullmatch(r"\d+", txt):
            try:
                nums.append(int(txt))
            except ValueError:
                pass

    del soup
    gc.collect()
    return max(nums) if nums else None


def estimate_total_pages(html: str, page_url: str, cards_count: int) -> Tuple[Optional[int], Optional[int]]:
    """
    Visszaadja: (total_pages, total_items)
    Vaterán gyakran csak az összes hirdetésszám látszik: "2407 összesen".
    Ebből az első oldalon talált kártyaszámmal becsülhető az oldalszám.
    Ha a lapozóban van nagyobb p= érték, azt is figyelembe vesszük.
    """
    total_items = parse_total_items(html)
    max_link_page = parse_max_page_from_pagination_links(html, page_url)

    candidates: List[int] = []
    if max_link_page:
        candidates.append(max_link_page)
    if total_items and cards_count > 0:
        candidates.append(max(1, math.ceil(total_items / cards_count)))

    return (max(candidates) if candidates else None), total_items


def parse_ad_cards(html: str, page_url: str) -> List[AdCard]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[AdCard] = []
    seen: Set[str] = set()

    # A képeken látható fő kártya: div.gtm-impression-product.
    card_nodes: List[Tag] = []
    for selector in [
        "div.gtm-impression-product",
        "article.gtm-impression-product",
        "[data-product-id]",
        "[data-gtm-product-id]",
    ]:
        for node in soup.select(selector):
            if isinstance(node, Tag) and node not in card_nodes:
                card_nodes.append(node)

    # Elsődleges: kártyákból olvasunk.
    for card in card_nodes:
        title = clean_text(
            card.get("data-gtm-name")
            or card.get("data-product-title")
            or card.get("data-title")
            or ""
        )
        ad_id = clean_text(
            card.get("data-product-id")
            or card.get("data-gtm-product-id")
            or card.get("data-id")
            or ""
        )

        href = clean_text(
            card.get("data-gtm-url")
            or card.get("data-product-url")
            or card.get("data-url")
            or ""
        )

        a = None
        if not href:
            # A cím linkje sokszor product-title-link, de fallback-ként minden .html linket megnézünk.
            candidate_links = card.select('a.product-title-link[href], a[href$=".html"], a[href*="vatera.hu"][href]')
            for cand in candidate_links:
                cand_href = cand.get("href") or ""
                abs_url = urljoin(page_url, cand_href)
                if looks_like_vatera_product_url(abs_url):
                    a = cand
                    href = cand_href
                    break

        if not href:
            continue

        url = normalize_ad_url(urljoin(page_url, href))
        if not looks_like_vatera_product_url(url):
            continue
        if url in seen:
            continue

        if not title:
            title_el = card.select_one("h3, h2, .product-title, .product-title-link")
            if title_el:
                title = clean_text(title_el.get_text(" ", strip=True))
        if not title and a:
            title = clean_text(a.get("title") or a.get_text(" ", strip=True))
        if not title:
            title = "ismeretlen Vatera hirdetés"

        if not ad_id:
            ad_id = extract_ad_id_from_url(url)

        seen.add(url)
        results.append(AdCard(title=title, url=url, ad_id=ad_id))

    # Másodlagos fallback: ha a fenti nem találna semmit, minden terméknek tűnő linkből dolgozunk.
    if not results:
        for a in soup.select('a[href]'):
            href = a.get("href") or ""
            url = normalize_ad_url(urljoin(page_url, href))
            if not looks_like_vatera_product_url(url):
                continue
            if url in seen:
                continue

            title = clean_text(a.get("title") or a.get_text(" ", strip=True))
            if not title or len(title) < 5:
                continue

            seen.add(url)
            results.append(AdCard(title=title, url=url, ad_id=extract_ad_id_from_url(url)))

    del soup
    gc.collect()
    return results


# --------------------------------------------------
# Hirdetésoldal parse
# --------------------------------------------------

def extract_title(soup: BeautifulSoup) -> str:
    selectors = [
        "h1",
        ".product-title-box h1",
        "[class*='product-title'] h1",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            txt = clean_text(el.get_text(" ", strip=True))
            if txt:
                return txt
    return "ismeretlen hirdetés"


def strip_rating_from_seller(text: str) -> str:
    text = clean_text(text)
    text = re.sub(r"\s*\(\s*\d+\s*\)\s*$", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_bad_seller_candidate(text: str) -> bool:
    """Kiszűri a Cookiebot / technikai / nem eladónév jellegű találatokat."""
    t = strip_rating_from_seller(text)
    if not t:
        return True

    low = t.lower()
    bad_fragments = [
        "client:",
        "cookie",
        "cookiebot",
        "usercentrics",
        "süti",
        "adatkezel",
        "beleegyez",
        "partner",
        "értékel",
        "minősítés",
        "visszajelzés",
        "eladó követése",
        "eladó termékei",
        "szabálytalan hirdetés",
        "megosztás",
    ]
    if any(x in low for x in bad_fragments):
        return True

    if DATE_RE.search(t):
        return True

    # Tipikus technikai azonosító, pl. Client:20a6b3 vagy hasonló hash-szerű név.
    if re.fullmatch(r"[A-Za-z]+:[0-9a-fA-F]{4,}", t):
        return True

    if len(t) < 2 or len(t) > 80:
        return True

    return False


def extract_seller(soup: BeautifulSoup) -> str:
    """
    Vatera eladónév kinyerése.
    A helyes elem a képen is láthatóan általában:
      <a href="/user/rating/rating.php?..." class="btn-link ...">Rektormuhely</a>
      <span class="winner-positive-points">(125)</span>
    A Cookiebot miatt előfordulhat hamis „Client:...” találat, azt direkt szűrjük.
    """

    # 1) Legpontosabb Vatera selectorok.
    selectors = [
        '.userprodbox a[href*="/user/rating/rating.php"]',
        '.userprodbox a[href*="rating.php"]',
        'a.btn-link[href*="/user/rating/rating.php"]',
        'a[href*="/user/rating/rating.php"]',
        'a[href*="rating.php?id="]',
        '[class*="seller"] a[href*="rating.php"]',
        '[class*="user"] a[href*="rating.php"]',
    ]

    for sel in selectors:
        for el in soup.select(sel):
            txt = strip_rating_from_seller(el.get_text(" ", strip=True))
            if not is_bad_seller_candidate(txt):
                return txt

    # 2) Ha a név és a pontszám külön sorban van, keressünk olyan linket, amely mellett winner-positive-points van.
    for points in soup.select('.winner-positive-points, [class*="positive-points"], [class*="points"]'):
        parent = points.parent
        for _ in range(5):
            if not isinstance(parent, Tag):
                break
            for a in parent.select('a[href]'):
                txt = strip_rating_from_seller(a.get_text(" ", strip=True))
                if not is_bad_seller_candidate(txt):
                    return txt
            parent = parent.parent

    # 3) DOM-szöveges fallback az „Eladó” környékéről.
    text = clean_text(soup.get_text("\n", strip=True))
    lines = [clean_text(x) for x in text.split("\n") if clean_text(x)]
    for i, line in enumerate(lines):
        if re.search(r"^Eladó$|Eladó adatai|Eladó információ", line, flags=re.I):
            for nxt in lines[i + 1 : i + 10]:
                candidate = strip_rating_from_seller(nxt)
                if not is_bad_seller_candidate(candidate):
                    return candidate

    return "ismeretlen eladó"


def extract_auction_start_date(soup: BeautifulSoup) -> str:
    labels = [
        "Aukció kezdete",
        "Feltöltés dátuma",
        "Hirdetés kezdete",
    ]

    # 1) Teljes szövegben: címke után dátum.
    text = clean_text(soup.get_text("\n", strip=True))
    for label in labels:
        pattern = re.compile(re.escape(label) + r"\s*:?\s*\n?\s*(" + DATE_RE.pattern + r")", flags=re.I)
        m = pattern.search(text)
        if m:
            return clean_text(m.group(1))

    # 2) Soronként: címke sorától pár sorral lejjebb.
    lines = [clean_text(x) for x in text.split("\n") if clean_text(x)]
    for i, line in enumerate(lines):
        if any(label.lower() in line.lower() for label in labels):
            for nxt in lines[i : i + 8]:
                m = DATE_RE.search(nxt)
                if m:
                    return clean_text(m.group(0))

    # 3) DOM szintű fallback: címke elemének szülőjében keresünk dátumot.
    for el in soup.find_all(string=True):
        val = clean_text(str(el))
        if not val:
            continue
        if any(label.lower() in val.lower() for label in labels):
            parent = el.parent
            for _ in range(4):
                if not parent:
                    break
                block_text = clean_text(parent.get_text("\n", strip=True))
                m = DATE_RE.search(block_text)
                if m:
                    return clean_text(m.group(0))
                parent = parent.parent

    return "ismeretlen dátum"


def is_bad_description_paragraph(text: str) -> bool:
    t = clean_text(text)
    if not t:
        return True

    low = normalize_ws_inline(t).lower()
    bad_exact = {
        "eladó leírása a termékről",
        "leírás",
        "keresés a leírásban is",
        "keresés a leírásban",
    }
    if low in bad_exact:
        return True

    bad_contains = [
        "szabálytalan hirdetés",
        "megosztás facebook",
        "megosztás",
        "eladó termékei",
        "vissza az oldal tetejére",
    ]
    if any(x in low for x in bad_contains):
        return True

    return False


def collect_paragraphs_from_node(node: Tag) -> List[str]:
    paragraphs: List[str] = []
    for p in node.select("p"):
        txt = clean_text(p.get_text("\n", strip=True))
        if is_bad_description_paragraph(txt):
            continue
        paragraphs.append(txt)
    return paragraphs


def unique_keep_order(items: List[str]) -> List[str]:
    cleaned: List[str] = []
    seen: Set[str] = set()
    for item in items:
        item = clean_text(item)
        if not item:
            continue
        key = normalize_ws_inline(item).lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(item)
    return cleaned


def clean_keep_all(items: List[str]) -> List[str]:
    """
    Leírás-paragrafusokhoz használjuk.
    Fontos: itt NEM szedjük ki az ismétlődő paragrafusokat, mert Vaterán az eladók
    gyakran szándékosan megismétlik a fizetési / átvételi / licitálási feltételeket.
    Ha deduplikálnánk, a leírás vége hibásan lemaradhatna.
    """
    cleaned: List[str] = []
    for item in items:
        item = clean_text(item)
        if item:
            cleaned.append(item)
    return cleaned


def is_description_stop_heading(node: Tag, original_heading: Tag) -> bool:
    """
    Igazat ad vissza, ha a leírás után már egy új nagyobb szekció kezdődik.
    A Vaterán az „Eladó leírása a termékről” h3 után minden <p>-t gyűjtünk,
    amíg új h1/h2/h3/h4 címsorhoz vagy ismert következő szekcióhoz nem érünk.
    """
    if node is original_heading:
        return False

    if node.name in {"h1", "h2", "h3", "h4"}:
        txt = normalize_ws_inline(node.get_text(" ", strip=True)).lower()
        # Ha valamiért ugyanaz a címsor ismétlődne, azt nem tekintjük stopnak.
        if "eladó leírása" in txt and "termékr" in txt:
            return False
        return True

    txt = normalize_ws_inline(node.get_text(" ", strip=True)).lower()
    stop_texts = [
        "megosztás",
        "szabálytalan hirdetés",
        "eladó termékei",
        "hasonló termékek",
        "kapcsolódó termékek",
        "ajánlott termékek",
        "vatera biztonság",
        "termékfigyelő",
    ]

    # Csak blokk-jellegű elemeknél állunk meg, hogy egy sima szó miatt ne vágjuk el a leírást.
    if node.name in {"section", "aside", "nav", "footer", "header"}:
        return any(stop in txt for stop in stop_texts)

    # Ha egy külön címsor/div röviden csak a következő szekció nevét tartalmazza, ott is álljunk meg.
    if node.name in {"div", "span", "strong"} and len(txt) <= 80:
        return any(txt == stop or txt.startswith(stop) for stop in stop_texts)

    return False


def extract_all_paragraphs_until_next_section(heading: Tag) -> List[str]:
    """
    A legbiztosabb leírás-kinyerési stratégia:
    az „Eladó leírása a termékről” címsor után dokumentumsorrendben
    minden következő <p> elemet elmentünk, amíg új nagyobb szekciócímet nem találunk.

    Nincs paragrafus darabszám-limit, és nincs deduplikálás sem:
    ha az eladó ugyanazt a szöveget többször írja le, akkor azt többször mentjük.
    """
    paragraphs: List[str] = []

    for node in heading.next_elements:
        if not isinstance(node, Tag):
            continue

        if is_description_stop_heading(node, heading):
            break

        if node.name != "p":
            continue

        txt = clean_text(node.get_text("\n", strip=True))
        if is_bad_description_paragraph(txt):
            continue

        paragraphs.append(txt)

    return clean_keep_all(paragraphs)


def extract_description_after_heading(soup: BeautifulSoup) -> str:
    """
    A kívánt rész pontosan a képen jelölt blokk:
    h3: „Eladó leírása a termékről”

    Elsődleges működés:
    a h3 után minden következő <p> paragrafust mentünk,
    egészen addig, amíg új nagyobb szekciócímet nem találunk.

    Így hosszú leírásnál sincs darabszám-limit, tehát 3, 40 vagy akár több paragrafus is mehet a JSON data mezőbe.
    Ismétlődő paragrafusokat sem dobunk ki, mert ezek a leírás végén is fontosak lehetnek.
    """

    heading: Optional[Tag] = None
    for h in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
        htxt = normalize_ws_inline(h.get_text(" ", strip=True))
        if "eladó leírása" in htxt.lower() and "termékr" in htxt.lower():
            heading = h
            break

    if not heading:
        return ""

    # 1) ÚJ, legbiztosabb stratégia: h3 után minden <p>, új nagyobb szekciócímsorig.
    all_following_paras = extract_all_paragraphs_until_next_section(heading)
    if all_following_paras:
        return "\n\n".join(all_following_paras)

    # 2) Fallback: közvetlen sibling blokkokból gyűjtés.
    sibling_paras: List[str] = []
    for sib in heading.find_next_siblings():
        if not isinstance(sib, Tag):
            continue
        if is_description_stop_heading(sib, heading):
            break
        sibling_paras.extend(collect_paragraphs_from_node(sib))

    sibling_paras = clean_keep_all(sibling_paras)
    if sibling_paras:
        return "\n\n".join(sibling_paras)

    # 3) Fallback: ha a heading nem sibling struktúrában van, a szülő gyerekein megyünk tovább.
    parent = heading.parent
    if isinstance(parent, Tag):
        collect = False
        parent_paras: List[str] = []
        for child in parent.children:
            if child is heading:
                collect = True
                continue
            if not collect or not isinstance(child, Tag):
                continue
            if is_description_stop_heading(child, heading):
                break
            parent_paras.extend(collect_paragraphs_from_node(child))
        parent_paras = clean_keep_all(parent_paras)
        if parent_paras:
            return "\n\n".join(parent_paras)

    return ""


def extract_description(soup: BeautifulSoup) -> str:
    # 1) Pontos blokk: „Eladó leírása a termékről” alatti összes paragraph.
    desc = extract_description_after_heading(soup)
    if desc:
        return desc

    # 2) CSS fallback: description-pane-en belül h3 után közvetlen div.
    for sel in [
        "#description-pane h3 + div",
        "[id*='description'] h3 + div",
        ".userprodbox h3 + div",
    ]:
        node = soup.select_one(sel)
        if isinstance(node, Tag):
            paras = clean_keep_all(collect_paragraphs_from_node(node))
            if paras:
                return "\n\n".join(paras)

    # 3) Csak akkor olvassuk az egész description-pane-t, ha nem került elő a heading.
    for sel in ["#description-pane", "[id*='description']"]:
        node = soup.select_one(sel)
        if isinstance(node, Tag):
            paras = clean_keep_all(collect_paragraphs_from_node(node))
            # Ha csak a keresőmező van benne, akkor ne mentsük.
            real_paras = [p for p in paras if not is_bad_description_paragraph(p)]
            if real_paras:
                return "\n\n".join(real_paras)

    # 4) Szöveges fallback.
    full_text = clean_text(soup.get_text("\n", strip=True))
    m = re.search(
        r"Eladó leírása a termékről\s*(.+?)(Megosztás|Szabálytalan hirdetés|Eladó termékei|$)",
        full_text,
        flags=re.I | re.S,
    )
    if m:
        desc = clean_text(m.group(1))
        desc = re.sub(r"^Keresés\s+a\s+leírásban\s+is\s*", "", desc, flags=re.I)
        if desc:
            return desc

    return "Nincs leírás."


def parse_ad_details(html: str, ad_url: str, fallback_card: Optional[AdCard] = None) -> AdDetails:
    soup = BeautifulSoup(html, "html.parser")

    title = extract_title(soup)
    if title == "ismeretlen hirdetés" and fallback_card and fallback_card.title:
        title = fallback_card.title

    date = extract_auction_start_date(soup)
    seller = extract_seller(soup)
    description = extract_description(soup)
    ad_id = fallback_card.ad_id if fallback_card and fallback_card.ad_id else extract_ad_id_from_url(ad_url)

    del soup
    gc.collect()

    return AdDetails(
        title=title,
        date=date,
        seller=seller,
        description=description,
        url=ad_url,
        ad_id=ad_id,
    )


# --------------------------------------------------
# Fő scrape logika
# --------------------------------------------------

def scrape_listing(
    fetcher: BrowserFetcher,
    list_url: str,
    output_dir: str,
    topic_name: str,
    delay: float,
    preview: bool,
    start_page: int,
    end_page: Optional[int],
    max_pages: int,
) -> None:
    paths = ensure_dirs(Path(output_dir).expanduser().resolve())
    visited_ads = {normalize_ad_url(x) for x in load_visited(paths["visited_ads"])}

    topic_file = topic_file_path(paths["topics"], topic_name)

    existing_ads = 0
    has_existing_ads = False

    if topic_file.exists():
        if is_stream_json_finalized(topic_file):
            print("[INFO] A topic JSON már le volt zárva, újranyitom hozzáfűzéshez.")
            reopen_finalized_stream_json_for_append(topic_file)

        last_ad_id, last_ad_url, existing_ads = get_last_written_ad_info(topic_file)
        has_existing_ads = existing_ads > 0

        if last_ad_url:
            print(
                f"[INFO] Meglévő topic JSON folytatása | meglévő hirdetések: {existing_ads} | "
                f"utolsó URL: {last_ad_url} | utolsó ad_id: {last_ad_id}"
            )
        else:
            print(f"[INFO] Meglévő topic JSON folytatása | meglévő hirdetések: {existing_ads}")

    else:
        write_topic_stream_header(
            topic_file=topic_file,
            resolved_title=topic_name,
            topic_url=list_url,
            start_page=start_page,
            end_page=end_page,
        )
        print(f"[INFO] Új streamelt topicfájl létrehozva: {topic_file}")

    first_page_url = build_list_page_url(list_url, start_page)
    print(f"[INFO] Kezdő listaoldal megnyitása: {first_page_url}")
    final_url, html = fetcher.fetch(first_page_url, wait_ms=int(delay * 1000))
    first_cards = parse_ad_cards(html, final_url)
    total_pages, total_items = estimate_total_pages(html, final_url, len(first_cards))

    if total_pages is None:
        total_pages = end_page

    if end_page is not None:
        real_end_page = end_page
        if total_pages:
            real_end_page = min(real_end_page, total_pages)
    else:
        real_end_page = total_pages

    if real_end_page is None:
        real_end_page = max_pages
        print(
            "[WARN] Nem sikerült biztosan meghatározni az oldalszámot, ezért addig megyek, "
            f"amíg találok hirdetést, de maximum {max_pages} oldalig."
        )

    if total_items:
        print(f"[INFO] Összes hirdetés a Vatera szerint: {total_items}")
    if total_pages:
        print(f"[INFO] Feldolgozási tartomány: {start_page} -> {real_end_page} | Becsült/ismert oldalszám: {total_pages}")
    else:
        print(f"[INFO] Feldolgozási tartomány: {start_page} -> {real_end_page} | Oldalszám: ismeretlen")

    saved_count = 0
    skipped_count = 0

    for page_no in range(start_page, real_end_page + 1):
        page_url = build_list_page_url(list_url, page_no)
        total_label = str(total_pages) if total_pages else "?"
        print(f"\n[INFO] Oldalváltás / listaoldal: {page_no}/{total_label} | URL: {page_url}")

        # Az első oldalt már letöltöttük; ne töltsük újra feleslegesen.
        if page_no == start_page:
            cards = first_cards
        else:
            final_url, html = fetcher.fetch(page_url, wait_ms=int(delay * 1000))
            cards = parse_ad_cards(html, final_url)

        print(f"[INFO] Talált hirdetéskártyák száma ezen az oldalon: {len(cards)}")

        if not cards:
            print("[WARN] Nem találtam hirdetéseket ezen az oldalon, leállok.")
            break

        for idx, card in enumerate(cards, start=1):
            ad_url = normalize_ad_url(card.url)

            if ad_url in visited_ads:
                skipped_count += 1
                print(f"[INFO] Már mentett hirdetés, kihagyom: {card.title}")
                continue

            print(f"[INFO] Hirdetés megnyitása ({idx}/{len(cards)}): {card.title}")
            print(f"[DEBUG] URL: {ad_url}")

            try:
                detail_final_url, detail_html = fetcher.fetch(ad_url, wait_ms=int(delay * 1000))
                details = parse_ad_details(detail_html, normalize_ad_url(detail_final_url), fallback_card=card)
            except Exception as e:
                print(f"[WARN] Hirdetés letöltési/parszolási hiba: {ad_url} | {e}")
                continue

            if preview:
                print(
                    f"[PREVIEW] Eladó: {details.seller} | Dátum: {details.date} | "
                    f"Leírás: {short_preview(details.description)}"
                )

            ad_item = ad_to_output_item(details)
            append_ad_to_stream_file(topic_file, ad_item, has_existing_ads)
            has_existing_ads = True

            append_visited(paths["visited_ads"], ad_url)
            visited_ads.add(ad_url)

            saved_count += 1
            print(f"[INFO] Mentve JSON-be: {details.title}")

            if delay > 0:
                time.sleep(delay)

        if page_no != start_page:
            del cards
        gc.collect()

    finalize_stream_json(topic_file)

    print("\n[INFO] Kész.")
    print(f"[INFO] Újonnan mentett hirdetések: {saved_count}")
    print(f"[INFO] Kihagyott, korábban látott hirdetések: {skipped_count}")
    print(f"[INFO] Meglévő hirdetések a futás előtt: {existing_ads}")
    print(f"[INFO] Kimeneti JSON fájl: {topic_file}")


# --------------------------------------------------
# CLI
# --------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Vatera hirdetés scraper Playwright + BeautifulSoup, streamelt JSON append módban"
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_LIST_URL,
        help="A Vatera listaoldal URL-je. Alapértelmezett: a kód elején lévő DEFAULT_LIST_URL.",
    )
    parser.add_argument(
        "--topic",
        default="vatera_muszaki_cikk_es_mobil",
        help="A mentett JSON fájl neve kiterjesztés nélkül.",
    )
    parser.add_argument(
        "--out",
        "--output",
        dest="output",
        default=".",
        help="Kimeneti mappa. Ebben jön létre a vatera/ mappa.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Várakozás másodpercben a lekérések között. Vateránál érdemes 2-5 másodperc.",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="Kezdő listaoldal.",
    )
    parser.add_argument(
        "--end-page",
        type=int,
        default=None,
        help="Utolsó listaoldal. Ha nincs megadva, megpróbál végigmenni az összesen.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10000,
        help="Biztonsági maximum, ha az oldalszám nem állapítható meg.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Kiírja konzolra: Eladó | dátum | leírás rövid előnézet.",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Látható böngészőablakkal fut.",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=90000,
        help="Timeout ms-ban.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="Újrapróbálkozások száma.",
    )
    parser.add_argument(
        "--auto-reset-fetches",
        type=int,
        default=100,
        help="Ennyi fetch után újranyitja a browser contextet.",
    )
    parser.add_argument(
        "--block-heavy",
        action="store_true",
        help="Képek, fontok és média tiltása gyorsításhoz. Ha gond van az oldallal, futtasd nélküle.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.start_page < 1:
        print("[ERROR] A --start-page legalább 1 kell legyen.")
        sys.exit(1)

    if args.end_page is not None and args.end_page < args.start_page:
        print("[ERROR] A --end-page nem lehet kisebb, mint a --start-page.")
        sys.exit(1)

    if args.max_pages < 1:
        print("[ERROR] A --max-pages legalább 1 kell legyen.")
        sys.exit(1)

    try:
        with BrowserFetcher(
            headless=not args.headed,
            slow_mo=50 if args.headed else 0,
            timeout_ms=args.timeout_ms,
            retries=args.retries,
            auto_reset_fetches=args.auto_reset_fetches,
            block_resources=args.block_heavy,
        ) as fetcher:
            scrape_listing(
                fetcher=fetcher,
                list_url=args.url,
                output_dir=args.output,
                topic_name=args.topic,
                delay=args.delay,
                preview=args.preview,
                start_page=args.start_page,
                end_page=args.end_page,
                max_pages=args.max_pages,
            )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva.")
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] Végzetes hiba: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


#
# Alap futtatás:
#   python vatera_scraper.py --out ./vatera --topic muszaki --preview --delay 3
#
# Csak első 3 oldal teszthez:
#   python vatera_scraper.py --out ./vatera --topic muszaki_teszt --start-page 1 --end-page 3 --preview --headed
#
# Másik Vatera URL-lel:
#   python vatera_scraper.py --url "https://www.vatera.hu/muszaki-cikk-es-mobil/index-c12082.html" --out ./vatera --topic vatera_muszaki --preview
#
# Gyorsabb, fej nélküli futás, nehéz erőforrások tiltásával:
#   python vatera_scraper.py --out ./vatera --topic muszaki --preview --delay 2 --block-heavy
