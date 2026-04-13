#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Tuple


def check_json_file(file_path: Path) -> str:
    try:
        with file_path.open("r", encoding="utf-8") as f:
            json.load(f)
        return "complete"
    except Exception:
        return "badformat"


def find_json_files(workdir: Path) -> List[Path]:
    return sorted([p for p in workdir.rglob("*.json") if p.is_file()])


def write_report(report_path: Path, results: List[Tuple[Path, str]], base_dir: Path) -> None:
    total = len(results)
    good = sum(1 for _, status in results if status == "complete")
    bad = sum(1 for _, status in results if status == "badformat")

    with report_path.open("w", encoding="utf-8") as f:
        for file_path, status in results:
            try:
                rel_path = file_path.relative_to(base_dir)
            except ValueError:
                rel_path = file_path
            f.write(f"{rel_path}\t{status}\n")

        f.write("\n")
        f.write(f"total\t{total}\n")
        f.write(f"complete\t{good}\n")
        f.write(f"badformat\t{bad}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="JSON fájlok ellenőrzése egy mappában.")
    parser.add_argument(
        "--workdir",
        required=True,
        help="A mappa, amelyben a .json fájlokat kell ellenőrizni."
    )
    args = parser.parse_args()

    cwd = Path.cwd()
    workdir = Path(args.workdir).expanduser()

    if not workdir.is_absolute():
        workdir = cwd / workdir

    workdir = workdir.resolve()
    script_dir = Path(__file__).resolve().parent
    report_path = script_dir / "goodjson.txt"

    print(f"[DEBUG] Aktuális mappa: {cwd}")
    print(f"[DEBUG] Vizsgált mappa: {workdir}")
    print(f"[DEBUG] Report fájl helye: {report_path}")

    if not workdir.exists():
        print(f"[ERROR] A megadott mappa nem létezik: {workdir}", file=sys.stderr)
        return 1

    if not workdir.is_dir():
        print(f"[ERROR] A megadott útvonal nem mappa: {workdir}", file=sys.stderr)
        return 1

    json_files = find_json_files(workdir)

    results: List[Tuple[Path, str]] = []
    for json_file in json_files:
        status = check_json_file(json_file)
        results.append((json_file, status))

    write_report(report_path, results, workdir)

    total = len(results)
    good = sum(1 for _, status in results if status == "complete")
    bad = sum(1 for _, status in results if status == "badformat")

    print(f"[INFO] Ellenőrzött JSON fájlok száma: {total}")
    print(f"[INFO] Jó fájlok száma: {good}")
    print(f"[INFO] Hibás fájlok száma: {bad}")
    print(f"[INFO] Riport fájl: {report_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
    #python jsonformatcheck.py --workdir /utvonal/a/mappahoz