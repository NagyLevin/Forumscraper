#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import gc
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import sync_playwright


BASE_URL = "https://www.jofogas.hu"
DEFAULT_LIST_URL = "https://www.jofogas.hu/magyarorszag/muszaki-cikkek-elektronika"


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


def short_preview(text: str, max_len: int = 120) -> str:
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
    if page_no <= 1:
        return strip_fragment(base_url)
    return set_query_param(strip_fragment(base_url), "o", str(page_no))


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


# --------------------------------------------------
# Adatmodellek
# --------------------------------------------------

@dataclass
class AdCard:
    title: str
    url: str


@dataclass
class AdDetails:
    title: str
    date: str
    seller: str
    description: str
    url: str


# --------------------------------------------------
# Állapot / output
# --------------------------------------------------

def ensure_dirs(base_output: Path) -> dict:
    root = base_output / "jofogas"
    state_dir = root / "state"

    root.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    visited_ads = state_dir / "visited_ads.txt"
    if not visited_ads.exists():
        visited_ads.write_text("", encoding="utf-8")

    return {
        "root": root,
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


def append_ad_to_txt(path: Path, ad: AdDetails) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write("[\n")
        f.write(f"Hirdetescim: {ad.title}\n")
        f.write(f"Datum: {ad.date}\n")
        f.write(f"Elado: {ad.seller}\n")
        f.write("Leíras:\n")
        f.write(ad.description.rstrip() + "\n")
        f.write("]\n\n")


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
                    if req.resource_type in {"media", "font"}:
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
        texts = [
            "Elfogadom",
            "Elfogadom mindet",
            "Összes elfogadása",
            "Elfogadás",
            "Rendben",
            "Egyetértek",
        ]
        for txt in texts:
            try:
                locator = self.page.locator(f"text={txt}").first
                if locator.count() > 0 and locator.is_visible():
                    locator.click(timeout=1500, force=True)
                    self.page.wait_for_timeout(700)
                    break
            except Exception:
                pass

        selectors = [
            "#didomi-notice-agree-button",
            "button#onetrust-accept-btn-handler",
            "[data-testid='uc-accept-all-button']",
        ]
        for sel in selectors:
            try:
                locator = self.page.locator(sel).first
                if locator.count() > 0 and locator.is_visible():
                    locator.click(timeout=1500, force=True)
                    self.page.wait_for_timeout(700)
                    break
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
                    self.page.wait_for_load_state("networkidle", timeout=4000)
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

def parse_total_pages(html: str) -> Optional[int]:
    soup = BeautifulSoup(html, "html.parser")

    # Képek alapján pagination gombok aria-label-je pl. "2607. oldalra"
    nums: List[int] = []

    for btn in soup.select("button[aria-label]"):
        aria = clean_text(btn.get("aria-label") or "")
        m = re.search(r"(\d+)\.\s*oldalra", aria, flags=re.I)
        if m:
            try:
                nums.append(int(m.group(1)))
            except ValueError:
                pass

    if nums:
        return max(nums)

    # fallback: oldal szövegből
    text = clean_text(soup.get_text(" ", strip=True))
    candidates = re.findall(r"\b\d{2,5}\b", text)
    parsed = []
    for c in candidates:
        try:
            v = int(c)
            if 2 <= v <= 100000:
                parsed.append(v)
        except ValueError:
            pass

    return max(parsed) if parsed else None


def parse_ad_cards(html: str, page_url: str) -> List[AdCard]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[AdCard] = []
    seen: Set[str] = set()

    # A screenshotok alapján a releváns kártyák:
    # div[data-testid="ad-card-general"] és benne <a href=...> + h5 cím
    cards = soup.select('div[data-testid="ad-card-general"]')

    if not cards:
        # fallback
        cards = soup.select('a[href*="/magyarorszag/"][href*="_"]')

    for card in cards:
        a = None
        title = ""

        if isinstance(card, Tag):
            a = card.select_one('a[href]')
            title_el = card.select_one("h5, h4, h3")
            if title_el:
                title = clean_text(title_el.get_text(" ", strip=True))

        if not a:
            continue

        href = a.get("href")
        if not href:
            continue

        url = normalize_ad_url(urljoin(page_url, href))
        if url in seen:
            continue

        if not title:
            title = clean_text(a.get_text(" ", strip=True))

        if not title:
            continue

        seen.add(url)
        results.append(AdCard(title=title, url=url))

    del soup
    gc.collect()
    return results


# --------------------------------------------------
# Hirdetésoldal parse
# --------------------------------------------------

def extract_title(soup: BeautifulSoup) -> str:
    selectors = [
        "h1[data-testid]",
        "h1",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            txt = clean_text(el.get_text(" ", strip=True))
            if txt:
                return txt
    return "ismeretlen hirdetés"


def extract_date(soup: BeautifulSoup) -> str:
    # A screenshot alapján:
    # <p ...>Feladás dátuma:</p>
    # <span ...>ápr 6., 10:49</span>
    for p in soup.select("p, span, div"):
        txt = clean_text(p.get_text(" ", strip=True))
        if txt.lower() == "feladás dátuma:":
            parent = p.parent
            if parent:
                spans = parent.select("span")
                for sp in spans:
                    val = clean_text(sp.get_text(" ", strip=True))
                    if val and "feladás dátuma" not in val.lower():
                        return val

    # fallback: regex a teljes oldal szövegéből
    text = clean_text(soup.get_text("\n", strip=True))
    m = re.search(
        r"Feladás dátuma:\s*([^\n]+)",
        text,
        flags=re.I
    )
    if m:
        return clean_text(m.group(1))

    return "ismeretlen dátum"


def extract_seller(soup: BeautifulSoup) -> str:
    selectors = [
        '[data-testid="contact-box-user-name"]',
        'h5[data-testid="contact-box-user-name"]',
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            txt = clean_text(el.get_text(" ", strip=True))
            if txt:
                return txt

    # fallback a kontakt szekcióból
    text = clean_text(soup.get_text("\n", strip=True))
    m = re.search(r"Kapcsolatfelvétel a Hirdetővel.*?\n([^\n]+)", text, flags=re.I | re.S)
    if m:
        candidate = clean_text(m.group(1))
        if candidate:
            return candidate

    return "ismeretlen eladó"


def extract_description(soup: BeautifulSoup) -> str:
    # A screenshot alapján a leírás egy p elemben van, sok <br> taggel.
    # Megkeressük a "Leírás" címkét, és az utána következő blokkot.
    header_candidates = soup.find_all(["h2", "h3", "div", "span", "p"])
    for hdr in header_candidates:
        txt = clean_text(hdr.get_text(" ", strip=True))
        if txt.lower() == "leírás":
            # próbáljuk a következő nagyobb blokkot kinyerni
            nxt = hdr.find_next(["div", "p"])
            hops = 0
            while nxt and hops < 8:
                hops += 1
                block_text = clean_text(nxt.get_text("\n", strip=True))
                if len(block_text) >= 20 and "Leírás" not in block_text:
                    return block_text
                nxt = nxt.find_next(["div", "p"])

    # screenshot alapján ez gyakran body1 typography p
    for p in soup.select("p"):
        txt = clean_text(p.get_text("\n", strip=True))
        if len(txt) > 80 and (
            "eladó a" in txt.lower()
            or "postázás" in txt.lower()
            or "személyes átvétel" in txt.lower()
            or "készülék" in txt.lower()
        ):
            return txt

    return "Nincs leírás."


def parse_ad_details(html: str, ad_url: str) -> AdDetails:
    soup = BeautifulSoup(html, "html.parser")

    title = extract_title(soup)
    date = extract_date(soup)
    seller = extract_seller(soup)
    description = extract_description(soup)

    del soup
    gc.collect()

    return AdDetails(
        title=title,
        date=date,
        seller=seller,
        description=description,
        url=ad_url,
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
) -> None:
    paths = ensure_dirs(Path(output_dir).expanduser().resolve())
    visited_ads = {normalize_ad_url(x) for x in load_visited(paths["visited_ads"])}

    topic_file = paths["root"] / f"{sanitize_filename(topic_name)}.txt"

    # Első oldal megnyitása, hogy kiderüljön az összes oldalszám
    first_page_url = build_list_page_url(list_url, start_page)
    print(f"[INFO] Listaoldal megnyitása: {first_page_url}")
    final_url, html = fetcher.fetch(first_page_url, wait_ms=int(delay * 1000))

    total_pages = parse_total_pages(html)
    if total_pages is None:
        total_pages = end_page if end_page is not None else start_page

    real_end_page = end_page if end_page is not None else total_pages
    real_end_page = min(real_end_page, total_pages) if total_pages else real_end_page

    print(f"[INFO] Feldolgozási tartomány: {start_page} -> {real_end_page} | Összes oldal: {total_pages}")

    saved_count = 0
    skipped_count = 0

    for page_no in range(start_page, real_end_page + 1):
        page_url = build_list_page_url(list_url, page_no)
        print(f"\n[INFO] Hirdetéslista oldal: {page_no}/{total_pages} | URL: {page_url}")

        final_url, html = fetcher.fetch(page_url, wait_ms=int(delay * 1000))
        cards = parse_ad_cards(html, final_url)

        print(f"[INFO] Talált hirdetéskártyák száma: {len(cards)}")

        if not cards:
            print("[WARN] Nem találtam hirdetéseket ezen az oldalon.")
            continue

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
                details = parse_ad_details(detail_html, detail_final_url)
            except Exception as e:
                print(f"[WARN] Hirdetés letöltési hiba: {ad_url} | {e}")
                continue

            if preview:
                print(
                    f"[PREVIEW] Elado:{details.seller} | {details.date} | {short_preview(details.description)}"
                )

            append_ad_to_txt(topic_file, details)
            append_visited(paths["visited_ads"], ad_url)
            visited_ads.add(ad_url)

            saved_count += 1
            print(f"[INFO] Mentve: {details.title}")

            if delay > 0:
                time.sleep(delay)

    print("\n[INFO] Kész.")
    print(f"[INFO] Mentett hirdetések: {saved_count}")
    print(f"[INFO] Kihagyott (visited) hirdetések: {skipped_count}")
    print(f"[INFO] Kimeneti fájl: {topic_file}")


# --------------------------------------------------
# CLI
# --------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Jófogás hirdetés scraper Playwright + BeautifulSoup")
    parser.add_argument(
        "--url",
        default=DEFAULT_LIST_URL,
        help="A Jófogás listaoldal URL-je.",
    )
    parser.add_argument(
        "--topic",
        default="jofogas_topic",
        help="A mentett txt fájl neve kiterjesztés nélkül.",
    )
    parser.add_argument(
        "--out",
        "--output",
        dest="output",
        default=".",
        help="Kimeneti mappa. Ebben jön létre a jofogas mappa.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Várakozás másodpercben a lekérések között.",
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
        help="Utolsó listaoldal. Ha nincs megadva, végigmegy az összesen.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Kiírja konzolra: Elado|datum|leiras preview",
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.start_page < 1:
        print("[ERROR] A --start-page legalább 1 kell legyen.")
        sys.exit(1)

    if args.end_page is not None and args.end_page < args.start_page:
        print("[ERROR] A --end-page nem lehet kisebb, mint a --start-page.")
        sys.exit(1)

    try:
        with BrowserFetcher(
            headless=not args.headed,
            slow_mo=50 if args.headed else 0,
            timeout_ms=args.timeout_ms,
            retries=args.retries,
            auto_reset_fetches=args.auto_reset_fetches,
            block_resources=False,
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
            )
    except KeyboardInterrupt:
        print("\n[INFO] Megszakítva.")
        sys.exit(1)


if __name__ == "__main__":
    main()


# Példák:
# python jofogas_scraper.py --out ./DATA --topic elektronika --preview --delay 3
# python jofogas_scraper.py --out ./DATA --topic elektronika --start-page 1 --end-page 5 --preview
# python jofogas_scraper.py --out ./DATA --topic xbox --headed --delay 4

# python jofogas_scraper.py --out ./jofogas --topic elektronika --preview --delay 3 --start-page 1 --end-page 5 --headed