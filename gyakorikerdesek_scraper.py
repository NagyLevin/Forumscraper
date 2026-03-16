import argparse
import re
import time
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE = "https://www.gyakorikerdesek.hu"
CATEGORY = "/allatok"
OUTPUT_FILE = "allatok.txt"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

QUESTION_LINK_RE = re.compile(r"^/allatok__[^/]+__\d+-")
ANSWER_PAGE_RE = re.compile(r"__oldal-(\d+)$")
MULTISPACE_RE = re.compile(r"\s+")


def clean_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = MULTISPACE_RE.sub(" ", text)
    return text.strip()


class Scraper:
    def __init__(self, start_page: int, end_page: int, delay: float = 1.0) -> None:
        self.start_page = start_page
        self.end_page = end_page
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.seen_questions = set()

    def fetch(self, url: str) -> requests.Response:
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        response.encoding = response.apparent_encoding or response.encoding
        time.sleep(self.delay)
        return response

    def category_url(self, page_no: int) -> str:
        if page_no == 1:
            return urljoin(BASE, CATEGORY)
        return urljoin(BASE, f"{CATEGORY}__oldal-{page_no}")

    def get_question_links(self, page_url: str) -> List[str]:
        response = self.fetch(page_url)
        soup = BeautifulSoup(response.text, "html.parser")

        links: List[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not QUESTION_LINK_RE.match(href):
                continue
            if ANSWER_PAGE_RE.search(href):
                continue
            full_url = urljoin(BASE, href)
            if full_url in self.seen_questions:
                continue
            self.seen_questions.add(full_url)
            links.append(full_url)
        return links

    def extract_author(self, answer_box) -> str:
        header = answer_box.select_one(".valasz_fejlec")
        if header:
            author_elt = (
                header.select_one(".anonim")
                or header.select_one("a")
                or header.select_one("span")
            )
            if author_elt:
                author = clean_text(author_elt.get_text(" ", strip=True))
                if author:
                    return author

            header_text = clean_text(header.get_text(" ", strip=True))
            match = re.search(r"\d+/\d+\s+(.*?)\s+válasza:?", header_text, re.IGNORECASE)
            if match:
                return clean_text(match.group(1))

        return "ismeretlen"

    def extract_answers_from_soup(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        answers: List[Tuple[str, str]] = []
        for box in soup.select("div.valasz"):
            header = box.select_one(".valasz_fejlec")
            body = box.select_one(".valasz_valasz")
            if not header or not body:
                continue

            author = self.extract_author(box)
            text = clean_text(body.get_text("\n", strip=True))
            if text:
                answers.append((author, text))
        return answers

    def scrape_question(self, question_url: str) -> Tuple[str, List[Tuple[str, str]]]:
        response = self.fetch(question_url)
        soup = BeautifulSoup(response.text, "html.parser")

        title_elt = soup.find("h1")
        title = clean_text(title_elt.get_text(" ", strip=True)) if title_elt else question_url

        all_answers: List[Tuple[str, str]] = []
        seen_pairs = set()

        page_no = 1
        while True:
            current_url = question_url if page_no == 1 else f"{question_url}__oldal-{page_no}"
            try:
                page_response = response if page_no == 1 else self.fetch(current_url)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    break
                raise

            page_soup = soup if page_no == 1 else BeautifulSoup(page_response.text, "html.parser")
            page_answers = self.extract_answers_from_soup(page_soup)
            if not page_answers and page_no > 1:
                break

            new_count = 0
            for author, text in page_answers:
                key = (author, text)
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)
                all_answers.append((author, text))
                new_count += 1

            if page_no > 1 and new_count == 0:
                break

            page_no += 1

        return title, all_answers

    def save_posts(self, output_path: Path) -> None:
        with output_path.open("w", encoding="utf-8") as f:
            for page_no in range(self.start_page, self.end_page + 1):
                page_url = self.category_url(page_no)
                print(f"[INFO] Oldal feldolgozása: {page_url}")

                try:
                    question_links = self.get_question_links(page_url)
                except requests.HTTPError as exc:
                    status = exc.response.status_code if exc.response is not None else "?"
                    print(f"[HIBA] A listaoldal nem tölthető be ({status}): {page_url}")
                    continue

                if not question_links:
                    print(f"[INFO] Nincs több kérdés ezen az oldalon: {page_url}")
                    continue

                for index, question_url in enumerate(question_links, start=1):
                    print(f"[INFO]   {index}/{len(question_links)} kérdés: {question_url}")
                    try:
                        title, answers = self.scrape_question(question_url)
                    except Exception as exc:
                        print(f"[HIBA] Nem sikerült feldolgozni: {question_url} -> {exc}")
                        continue

                    f.write("Post:\n")
                    f.write(f"{title}\n")
                    for author, text in answers:
                        f.write(f"Comment by {author}:\n")
                        f.write(f"{text}\n")
                    f.write("\n" + "-" * 80 + "\n\n")
                    f.flush()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "GyakoriKerdesek Állatok kategória scraper. "
            "Letölti a megadott oldalintervallum kérdéseit és a hozzászólásokat."
        )
    )
    parser.add_argument("startpage", type=int, help="Kezdő oldal száma, pl. 1")
    parser.add_argument("endpage", type=int, help="Záró oldal száma, pl. 3")
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Várakozás másodpercben a kérések között (alapértelmezett: 1.0)",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_FILE,
        help=f"Kimeneti fájl neve (alapértelmezett: {OUTPUT_FILE})",
    )

    args = parser.parse_args()

    if args.startpage < 1 or args.endpage < 1:
        raise SystemExit("A startpage és endpage legalább 1 legyen.")
    if args.startpage > args.endpage:
        raise SystemExit("A startpage nem lehet nagyobb, mint az endpage.")

    scraper = Scraper(args.startpage, args.endpage, delay=args.delay)
    scraper.save_posts(Path(args.output))
    print(f"[KÉSZ] Mentve ide: {args.output}")


if __name__ == "__main__":
    main()
