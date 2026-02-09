from __future__ import annotations

import re
from pathlib import Path
from typing import Dict


# Matches {{var}} with optional spaces: {{ var }}
_PLACEHOLDER_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")


def load_sql_file(path: str) -> str:
    """
    Load a SQL file from disk.
    We keep comments and whitespace; Athena accepts them and they help auditing.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"SQL file not found: {p}")
    return p.read_text(encoding="utf-8")


def render_sql(template_sql: str, vars: Dict[str, str], *, strict: bool = True) -> str:
    """
    Render SQL templates using {{var}} placeholders.

    strict=True:
      - raises if any placeholders are missing from vars
      - raises if any placeholders remain after rendering
    """
    if strict:
        required = set(_PLACEHOLDER_RE.findall(template_sql))
        missing = sorted([k for k in required if k not in vars])
        if missing:
            raise ValueError(f"Missing template vars: {missing}")

    def repl(match: re.Match) -> str:
        key = match.group(1)
        if key not in vars:
            return match.group(0)  # leave as-is if not strict
        return str(vars[key])

    rendered = _PLACEHOLDER_RE.sub(repl, template_sql).strip()

    if strict:
        remaining = sorted(set(_PLACEHOLDER_RE.findall(rendered)))
        if remaining:
            raise ValueError(f"Unrendered template vars remain in SQL: {remaining}")

    return rendered
