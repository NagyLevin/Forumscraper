#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="JSON szöveg dump (data.content + comments[].data)"
    )
    parser.add_argument(
        "--workdir",
        required=True,
        help="Mappa ahol a JSON fájlok vannak"
    )
    parser.add_argument(
        "--rows",
        action="store_true",
        help="Ha meg van adva: minden data mező egy sorba kerül"
    )
    return parser.parse_args()


def extract_words(text: str) -> List[str]:
    if not text:
        return []
    return re.findall(r"\S+", text)


def normalize_row(text: str) -> str:
    """
    Többsoros szöveget egy sorba alakít:
    - sortörések -> szóköz
    - felesleges whitespace-ek eltávolítása
    """
    return re.sub(r"\s+", " ", text).strip()


def get_all_texts(json_path: Path) -> List[str]:
    """
    Visszaadja az összes data szöveget:
    - data.content
    - comments[].data
    """
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    texts = []

    # fő content
    main_content = data.get("data", {}).get("content", "")
    if isinstance(main_content, str):
        texts.append(main_content)

    # kommentek
    comments = data.get("comments", [])
    if isinstance(comments, list):
        for comment in comments:
            comment_text = comment.get("data", "")
            if isinstance(comment_text, str):
                texts.append(comment_text)

    return texts


def main() -> int:
    args = parse_args()

    workdir = Path(args.workdir).expanduser().resolve()

    if not workdir.exists():
        print(f"[ERROR] Nem létezik: {workdir}")
        return 1

    json_files = sorted(workdir.rglob("*.json"))

    if not json_files:
        print("[INFO] Nincs JSON fájl")
        return 0

    script_dir = Path(__file__).resolve().parent
    output_file = script_dir / "jsonwordsdump.txt"

    print(f"[INFO] Feldolgozás indul... ({len(json_files)} fájl)")
    print(f"[INFO] Mode: {'ROWS' if args.rows else 'WORDS'}")

    try:
        with output_file.open("w", encoding="utf-8") as out:

            for json_file in json_files:
                try:
                    texts = get_all_texts(json_file)

                    print(f"[OK] {json_file.name} -> {len(texts)} data mező")

                    out.write(f"===== {json_file.name} =====\n")

                    if args.rows:
                        # --- ROW MODE ---
                        for text in texts:
                            row = normalize_row(text)
                            if row:
                                out.write(row + "\n")

                    else:
                        # --- WORD MODE ---
                        for text in texts:
                            words = extract_words(text)
                            for w in words:
                                out.write(w + "\n")

                    out.write("\n\n")

                except Exception as e:
                    print(f"[ERROR] {json_file.name}: {e}")

    except Exception as e:
        print(f"[ERROR] Nem sikerült írni: {e}")
        return 1

    print(f"[INFO] Kész. Kimenet: {output_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())