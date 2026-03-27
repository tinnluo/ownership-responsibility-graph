import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {".md", ".py", ".toml", ".yml", ".yaml", ".csv", ".json", ".txt"}
BANNED_PATTERNS = [
    re.compile(r"Forward Analytics"),
    re.compile(r"\bFA\b"),
    re.compile(r"fa_"),
    re.compile(r"/Users/lxt/Documents/ForwardAnalytics"),
]
EXCLUDED_PATHS = {
    ROOT / "HANDOFF.md",
    ROOT / "tests" / "test_public_safety.py",
}
EXCLUDED_DIRS = {".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache", "data/output"}


def test_public_files_do_not_leak_private_terms():
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in EXCLUDED_DIRS for part in path.parts):
            continue
        if path in EXCLUDED_PATHS or path.suffix not in TEXT_SUFFIXES:
            continue

        contents = path.read_text(encoding="utf-8")
        for pattern in BANNED_PATTERNS:
            assert not pattern.search(contents), f"{pattern.pattern!r} found in {path}"
