#!/usr/bin/env python3
"""parse_columns.py — sqlglot-based SQL AST diff for dbt models.

Compares the SQL AST between production and development versions of dbt
models by reading raw_code from both manifests, stripping Jinja, parsing
with sqlglot, and outputting a JSON diff to stdout.

Usage:
    python3 parse_columns.py \
        --prod-manifest target_prod/manifest.json \
        --dev-manifest target/manifest.json \
        --models "int_order_pricing int_product_inventory"

Output:
    JSON array to stdout, one entry per model.  Matches the
    ``code_changes`` shape from the data-diff JSON contract.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

try:
    import sqlglot
    from sqlglot import exp
except ImportError:
    json.dump({"error": "sqlglot is not installed"}, sys.stdout)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Jinja stripping
# ---------------------------------------------------------------------------

# Regex for block-level Jinja tags: {% ... %}
# Non-greedy, handles each tag individually (if/endif/for/endfor/set/…).
_RE_BLOCK_TAG = re.compile(r"\{%-?\s*.*?\s*-?%\}", re.DOTALL)

# Regex for {{ config(...) }} — must be removed entirely (not replaced
# with a placeholder) because it sits at statement level.
_RE_CONFIG = re.compile(r"\{\{\s*config\s*\(.*?\)\s*\}\}", re.DOTALL)

# Regex for {{ ref('model') }}
_RE_REF = re.compile(r"\{\{\s*ref\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")

# Regex for {{ source('schema', 'table') }}
_RE_SOURCE = re.compile(
    r"\{\{\s*source\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}"
)

# {{ this }}
_RE_THIS = re.compile(r"\{\{\s*this\s*\}\}")

# Any remaining {{ ... }} (var(), dbt_utils macros, etc.)
_RE_EXPR = re.compile(r"\{\{.*?\}\}", re.DOTALL)

# Collapse excessive blank lines
_RE_BLANK = re.compile(r"\n\s*\n\s*\n")


def strip_jinja(raw: str) -> str:
    """Remove / replace Jinja so sqlglot can parse the SQL."""
    sql = raw
    # Order matters: specific patterns before generic catch-all.
    sql = _RE_REF.sub(r"`project.dataset.\1`", sql)
    sql = _RE_SOURCE.sub(r"`project.\1.\2`", sql)
    sql = _RE_THIS.sub("`project.dataset.__this__`", sql)
    sql = _RE_CONFIG.sub("", sql)
    sql = _RE_BLOCK_TAG.sub("", sql)
    sql = _RE_EXPR.sub("'__jinja_expr__'", sql)
    sql = _RE_BLANK.sub("\n\n", sql)
    return sql.strip()


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _parse(sql: str) -> exp.Expression:
    """Parse SQL with BigQuery dialect."""
    return sqlglot.parse_one(sql, dialect="bigquery")


def extract_columns(parsed: exp.Expression) -> tuple[dict[str, str], bool]:
    """Extract outermost SELECT columns.

    Returns:
        (columns, has_star) where *columns* is ``{name: expression_sql}``
        and *has_star* is True if ``SELECT *`` or ``t.*`` was found.
    """
    columns: dict[str, str] = {}
    has_star = False

    outer_select = parsed.find(exp.Select)
    if outer_select is None:
        return columns, False

    for col_expr in outer_select.expressions:
        if isinstance(col_expr, exp.Star):
            has_star = True
        elif isinstance(col_expr, exp.Alias):
            columns[col_expr.alias] = col_expr.this.sql(dialect="bigquery")
        elif isinstance(col_expr, exp.Column):
            if isinstance(col_expr.this, exp.Star):
                has_star = True
            else:
                columns[col_expr.name] = col_expr.sql(dialect="bigquery")
        else:
            sql_str = col_expr.sql(dialect="bigquery")
            columns[sql_str] = sql_str

    return columns, has_star


def extract_ctes(parsed: exp.Expression) -> dict[str, str]:
    """Extract CTE definitions: ``{name: body_sql}``."""
    ctes: dict[str, str] = {}
    for cte in parsed.find_all(exp.CTE):
        ctes[cte.alias] = cte.this.sql(dialect="bigquery")
    return ctes


def _norm(sql: str) -> str:
    """Normalise SQL for comparison (collapse whitespace, lowercase)."""
    return " ".join(sql.split()).strip().lower()


_JINJA_PLACEHOLDER = "__jinja_expr__"


def _has_jinja_artifacts(parsed: exp.Expression) -> bool:
    """Check whether Jinja placeholder strings leaked into the AST.

    If so, the raw_code stripping was lossy and the compiled SQL
    fallback should be tried instead.
    """
    sql = parsed.sql(dialect="bigquery")
    return _JINJA_PLACEHOLDER in sql


# ---------------------------------------------------------------------------
# Model-level diff
# ---------------------------------------------------------------------------

def diff_model(
    model_name: str,
    prod_raw: str | None,
    dev_raw: str,
    compiled_dev_path: Path | None = None,
) -> dict:
    """Compare prod vs dev SQL and return the diff result.

    * New models (``prod_raw is None``): column list only.
    * Existing models: expression_changes, cte_changes, has_indirect_changes.
    * Parse failures: ``code_changes = null`` with ``parse_error``.
    """
    result: dict = {"model": model_name}

    # -- Parse dev side ------------------------------------------------
    dev_parsed: exp.Expression | None = None
    dev_parse_error: str | None = None
    used_compiled_fallback = False

    try:
        dev_parsed = _parse(strip_jinja(dev_raw))
    except Exception as exc:
        dev_parse_error = str(exc)

    # Fallback: compiled SQL when raw parse fails *or* when Jinja
    # placeholders leak into the AST (common with {% for %} loops).
    if (dev_parsed is None or _has_jinja_artifacts(dev_parsed)) and (
        compiled_dev_path and compiled_dev_path.exists()
    ):
        try:
            dev_parsed = _parse(compiled_dev_path.read_text())
            dev_parse_error = None
            used_compiled_fallback = True
        except Exception as exc2:
            if dev_parse_error:
                dev_parse_error = f"raw: {dev_parse_error}; compiled: {exc2}"
            else:
                dev_parse_error = f"compiled fallback failed: {exc2}"
                # Revert to the original (artifact-laden) parse
                try:
                    dev_parsed = _parse(strip_jinja(dev_raw))
                except Exception:
                    dev_parsed = None

    if dev_parsed is None:
        result["code_changes"] = None
        result["parse_error"] = dev_parse_error
        if prod_raw is None:
            result["is_new"] = True
        return result

    dev_columns, dev_has_star = extract_columns(dev_parsed)
    dev_ctes = extract_ctes(dev_parsed)

    # -- New model (no prod counterpart) -------------------------------
    if prod_raw is None:
        result["is_new"] = True
        result["columns"] = list(dev_columns.keys())
        if dev_has_star:
            result["columns_note"] = (
                "SELECT * detected — column list may be incomplete"
            )
        result["code_changes"] = None
        return result

    # -- Parse prod side -----------------------------------------------
    try:
        prod_parsed = _parse(strip_jinja(prod_raw))
    except Exception as exc:
        result["code_changes"] = None
        result["parse_error"] = f"prod parse failed: {exc}"
        return result

    prod_columns, prod_has_star = extract_columns(prod_parsed)
    prod_ctes = extract_ctes(prod_parsed)

    # -- Column expression diff ----------------------------------------
    expression_changes: list[dict] = []
    added_columns: list[str] = []
    removed_columns: list[str] = []

    all_cols = sorted(set(prod_columns) | set(dev_columns))
    for col in all_cols:
        prod_expr = prod_columns.get(col)
        dev_expr = dev_columns.get(col)

        if prod_expr is None:
            added_columns.append(col)
        elif dev_expr is None:
            removed_columns.append(col)
        elif _norm(prod_expr) != _norm(dev_expr):
            expression_changes.append({
                "column": col,
                "prod_expression": prod_expr,
                "dev_expression": dev_expr,
            })

    # -- CTE diff ------------------------------------------------------
    cte_changes: list[dict] = []
    all_ctes = sorted(set(prod_ctes) | set(dev_ctes))
    for cte_name in all_ctes:
        prod_body = prod_ctes.get(cte_name)
        dev_body = dev_ctes.get(cte_name)

        if prod_body is None:
            cte_changes.append({"cte_name": cte_name, "change_type": "added"})
        elif dev_body is None:
            cte_changes.append({"cte_name": cte_name, "change_type": "removed"})
        elif _norm(prod_body) != _norm(dev_body):
            cte_changes.append({"cte_name": cte_name, "change_type": "modified"})

    # -- Indirect changes flag -----------------------------------------
    # True when a CTE was modified or removed, meaning data flowing
    # through it may differ even if output column expressions are
    # unchanged (e.g. WHERE / JOIN / filter changes).
    has_indirect = any(
        c["change_type"] in ("modified", "removed") for c in cte_changes
    )

    result["is_new"] = False
    result["code_changes"] = {
        "expression_changes": expression_changes,
        "added_columns": added_columns,
        "removed_columns": removed_columns,
        "cte_changes": cte_changes,
        "has_indirect_changes": has_indirect,
    }

    # -- Warnings ------------------------------------------------------
    warnings: list[str] = []
    if prod_has_star or dev_has_star:
        warnings.append(
            "SELECT * detected — column list may be incomplete"
        )
    if used_compiled_fallback:
        warnings.append(
            "Dev side parsed from compiled SQL (raw_code had "
            "unresolvable Jinja). CTE diff may show false positives "
            "due to schema reference differences."
        )
    if warnings:
        result["warnings"] = warnings

    return result


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def _load_manifest(path: str) -> dict:
    """Load a dbt manifest.json."""
    with open(path) as fh:
        return json.load(fh)


def _project_name(manifest: dict) -> str:
    """Extract the dbt project name from manifest metadata."""
    return manifest.get("metadata", {}).get("project_name", "")


def _find_node(manifest: dict, model_name: str) -> dict | None:
    """Find a model node by short name (e.g. 'int_order_pricing').

    Tries ``model.<project>.<name>`` first, then scans all model nodes.
    """
    project = _project_name(manifest)
    if project:
        key = f"model.{project}.{model_name}"
        node = manifest.get("nodes", {}).get(key)
        if node is not None:
            return node

    # Fallback: scan nodes for matching alias or name suffix.
    for node_key, node_val in manifest.get("nodes", {}).items():
        if not node_key.startswith("model."):
            continue
        if (
            node_val.get("alias") == model_name
            or node_key.endswith(f".{model_name}")
        ):
            return node_val

    return None


def _compiled_path(project_root: Path, node: dict) -> Path | None:
    """Derive the compiled SQL path for a dev model node."""
    ofp = node.get("original_file_path")
    if not ofp:
        return None
    # target/compiled/<project>/<original_file_path>
    project_name = node.get("package_name", "")
    if not project_name:
        return None
    return project_root / "target" / "compiled" / project_name / ofp


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare dbt model SQL ASTs between prod and dev.",
    )
    parser.add_argument(
        "--prod-manifest",
        required=True,
        help="Path to the production manifest.json (target_prod/manifest.json)",
    )
    parser.add_argument(
        "--dev-manifest",
        required=True,
        help="Path to the development manifest.json (target/manifest.json)",
    )
    parser.add_argument(
        "--models",
        required=True,
        help="Space-separated list of model names to diff",
    )
    parser.add_argument(
        "--project-root",
        default=".",
        help="dbt project root (for compiled SQL fallback). Default: cwd",
    )

    args = parser.parse_args()
    model_names = args.models.split()
    project_root = Path(args.project_root)

    # Load manifests -------------------------------------------------------
    dev_manifest = _load_manifest(args.dev_manifest)

    prod_manifest: dict | None = None
    prod_path = Path(args.prod_manifest)
    if prod_path.exists():
        prod_manifest = _load_manifest(str(prod_path))
    else:
        print(
            f"[parse_columns] warning: prod manifest not found at "
            f"{prod_path}; all models treated as new",
            file=sys.stderr,
        )

    # Diff each model ------------------------------------------------------
    results: list[dict] = []

    for model_name in model_names:
        dev_node = _find_node(dev_manifest, model_name)
        if dev_node is None:
            results.append({
                "model": model_name,
                "code_changes": None,
                "parse_error": f"model '{model_name}' not found in dev manifest",
            })
            continue

        dev_raw = dev_node.get("raw_code") or dev_node.get("raw_sql", "")

        prod_raw: str | None = None
        if prod_manifest is not None:
            prod_node = _find_node(prod_manifest, model_name)
            if prod_node is not None:
                prod_raw = prod_node.get("raw_code") or prod_node.get(
                    "raw_sql", ""
                )

        compiled = _compiled_path(project_root, dev_node)

        results.append(
            diff_model(model_name, prod_raw, dev_raw, compiled)
        )

    # Output ---------------------------------------------------------------
    json.dump(results, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
