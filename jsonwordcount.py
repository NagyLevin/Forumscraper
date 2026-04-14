#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import List, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="JSON fájlok szószámlálása (data.content + comments[].data)."
    )
    parser.add_argument(
        "--workdir",
        required=True,
        help="A mappa, amelyben a JSON fájlokat keresse."
    )
    return parser.parse_args()


def count_words(text: str) -> int:
    """Szavak számolása"""
    if not text:
        return 0
    return len(re.findall(r"\S+", text))


def get_total_word_count(json_path: Path) -> int:
    """
    Számolja:
    - data.content
    - comments[].data
    """
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    total_words = 0

    # fő content
    main_content = data.get("data", {}).get("content", "")
    if isinstance(main_content, str):
        total_words += count_words(main_content)

    # kommentek
    comments = data.get("comments", [])
    if isinstance(comments, list):
        for comment in comments:
            comment_text = comment.get("data", "")
            if isinstance(comment_text, str):
                total_words += count_words(comment_text)

    return total_words


def main() -> int:
    args = parse_args()

    workdir = Path(args.workdir).expanduser().resolve()

    if not workdir.exists():
        print(f"[ERROR] A megadott mappa nem létezik: {workdir}")
        return 1

    if not workdir.is_dir():
        print(f"[ERROR] A megadott útvonal nem mappa: {workdir}")
        return 1

    json_files: List[Path] = sorted(workdir.rglob("*.json"))

    if not json_files:
        print(f"[INFO] Nem található .json fájl: {workdir}")
        return 0

    script_dir = Path(__file__).resolve().parent
    output_file = script_dir / "jsonwordscount.txt"

    results: List[Tuple[str, int]] = []
    total_words = 0
    processed = 0
    failed = 0

    print(f"[INFO] Feldolgozás indul...")
    print(f"[INFO] JSON fájlok száma: {len(json_files)}")

    for json_file in json_files:
        try:
            word_count = get_total_word_count(json_file)

            rel_path = json_file.relative_to(workdir)
            results.append((str(rel_path), word_count))

            total_words += word_count
            processed += 1

            print(f"[OK] {json_file.name} -> {word_count} szó")

        except Exception as e:
            failed += 1
            print(f"[ERROR] {json_file.name} hiba: {e}")

    # fájlba írás
    try:
        with output_file.open("w", encoding="utf-8") as f:
            for filename, word_count in results:
                f.write(f"{filename} -> {word_count} szó\n")

            f.write("\n")
            f.write(f"Összesen -> {total_words} szó\n")
            f.write(f"Sikeres fájlok: {processed}\n")
            f.write(f"Hibás fájlok: {failed}\n")

    except Exception as e:
        print(f"[ERROR] Nem sikerült írni a fájlt: {e}")
        return 1

    print()
    print("[INFO] Kész.")
    print(f"[INFO] Összes szó: {total_words}")
    print(f"[INFO] Eredmény: {output_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
    #(minden olyan karakterláncot egy szónak vesz, ami nem whitespace)
    #python jsonwordcount.py --workdir /utvonal/a/mappahoz