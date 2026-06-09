import argparse
import sys
from pathlib import Path
from typing import Iterable, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.validate_xlsx_export import main as validate_xlsx_main

DEFAULT_PATTERNS = ["benchmark_output*.xlsx", "benchmark*.xlsx"]


def _iter_matches(directory: Path, pattern: str) -> Iterable[Path]:
    yield from (p for p in directory.glob(pattern) if p.is_file())


def _newest(paths: Iterable[Path]) -> Path | None:
    candidates = list(paths)
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _fmt_time(path: Path) -> str:
    return f"{path.stat().st_mtime:.6f}"


def pick_xlsx_file(directory: Path, patterns: List[str], include_any_xlsx: bool, verbose: bool = False) -> Path:
    if not directory.exists() or not directory.is_dir():
        raise FileNotFoundError(f"Directory does not exist or is not a directory: {directory}")

    all_xlsx = sorted([p for p in directory.glob("*.xlsx") if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)
    if verbose:
        print("All .xlsx candidates:")
        for p in all_xlsx:
            print(f"  {p} (mtime={_fmt_time(p)})")

    if not all_xlsx:
        raise FileNotFoundError(f"No .xlsx files found in {directory}")

    matched = []
    for pattern in patterns:
        matched.extend(list(_iter_matches(directory, pattern)))
    # de-duplicate while preserving newest selection later
    matched = list({p.resolve(): p for p in matched}.values())
    matched = sorted(matched, key=lambda p: p.stat().st_mtime, reverse=True)

    if verbose:
        print("Benchmark-like candidates:")
        for p in matched:
            print(f"  {p} (mtime={_fmt_time(p)})")

    selected = _newest(matched)
    if selected is not None:
        return selected

    if include_any_xlsx:
        return all_xlsx[0]

    patterns_text = ", ".join(patterns)
    raise FileNotFoundError(
        f"No benchmark-like .xlsx files found in {directory} for patterns: {patterns_text}"
    )


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Find newest benchmark XLSX export and validate it.")
    parser.add_argument("--dir", required=True, dest="directory", help="Directory containing exported .xlsx files")
    parser.add_argument(
        "--pattern",
        action="append",
        default=None,
        help="Glob pattern to match benchmark-like files. Can be passed multiple times.",
    )
    parser.add_argument(
        "--include-any-xlsx",
        action="store_true",
        help="If no benchmark-like file matches, fallback to newest *.xlsx file.",
    )
    parser.add_argument("--verbose", action="store_true", help="Print candidate files and modified times")
    args = parser.parse_args(argv)

    patterns = args.pattern if args.pattern else DEFAULT_PATTERNS

    try:
        selected = pick_xlsx_file(Path(args.directory), patterns, args.include_any_xlsx, args.verbose)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}")
        return 2

    print(f"Selected XLSX: {selected}")
    return validate_xlsx_main([str(selected)])


if __name__ == "__main__":
    sys.exit(main())
