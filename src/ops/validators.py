from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from ops.sql_templates import render_sql


def load_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip().rstrip(";")


def load_suite(dir_path: str) -> Dict[str, str]:
    p = Path(dir_path)
    suite: Dict[str, str] = {}
    for f in sorted(p.glob("*.sql")):
        suite[f.stem] = load_sql(f)
    return suite


def load_checks(
    dir_path: str,
    *,
    default_severity: str = "blocking",
    severities: Optional[Dict[str, str]] = None,
    descriptions: Optional[Dict[str, str]] = None,
    template_vars: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, str]]:
    """
    Returns the dict shape expected by QualityAgent.validate():
      {name: {"sql": "...", "severity": "...", "description": "..."}}

    This is a minimal extension so handlers stay simple.
    """
    suite_sql = load_suite(dir_path)
    sev = severities or {}
    desc = descriptions or {}

    checks: Dict[str, Dict[str, str]] = {}
    render_vars = template_vars or {}

    for name, sql in suite_sql.items():
        rendered_sql = render_sql(sql, render_vars, strict=False) if render_vars else sql
        checks[name] = {
            "sql": rendered_sql,
            "severity": sev.get(name, default_severity),
        }
        if name in desc:
            checks[name]["description"] = desc[name]
    return checks
