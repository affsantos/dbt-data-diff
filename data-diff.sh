#!/usr/bin/env bash
set -euo pipefail

# ═══════════════════════════════════════════════════════════════════
# dbt Data Diff — compare production vs development model data
#
# Usage:
#   ./data-diff.sh <model_selector>
#   ./data-diff.sh "int_order_pricing int_product_inventory"
#
# Prerequisites:
#   - target_prod/manifest.json and target/manifest.json exist
#   - Modified models already built in dev schema (run validate.sh first)
#   - gcloud authenticated, bq CLI available, jq installed
# ═══════════════════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

PROD_MANIFEST="target_prod/manifest.json"
DEV_MANIFEST="target/manifest.json"
PYTHON="${REPO_ROOT}/.venv/bin/python3"
DIAGRAMS_DIR="${REPO_ROOT}/.data-diff"
NUMERIC_TYPES="INT64|FLOAT64|NUMERIC|BIGNUMERIC|DECIMAL|BIGDECIMAL"

# ── Parse arguments ─────────────────────────────────────────────
if [[ $# -eq 0 ]]; then
    echo "Usage: data-diff.sh <model_selector>" >&2
    echo "" >&2
    echo "Examples:" >&2
    echo "  data-diff.sh int_order_pricing" >&2
    echo "  data-diff.sh \"int_order_pricing int_product_inventory\"" >&2
    exit 1
fi

MODELS="$*"

# ── Preflight checks ───────────────────────────────────────────
check_prereqs() {
    local ok=true
    if [[ ! -f "$PROD_MANIFEST" ]]; then
        echo "ERROR: $PROD_MANIFEST not found. Run validate.sh first." >&2
        ok=false
    fi
    if [[ ! -f "$DEV_MANIFEST" ]]; then
        echo "ERROR: $DEV_MANIFEST not found. Run dbt compile first." >&2
        ok=false
    fi
    if ! command -v bq &>/dev/null; then
        echo "ERROR: bq CLI not found." >&2
        ok=false
    fi
    if ! command -v jq &>/dev/null; then
        echo "ERROR: jq not found." >&2
        ok=false
    fi
    if [[ "$ok" == false ]]; then exit 1; fi
}

log() { echo "  $*" >&2; }
step() { echo "" >&2; echo "── $* ──" >&2; }

check_prereqs

mkdir -p "$DIAGRAMS_DIR"

# Temp directory for intermediate files
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

# ═══════════════════════════════════════════════════════════════════
# STEP 1: Identify modified models from manifests
# ═══════════════════════════════════════════════════════════════════
step "Step 1: Identify models"

# Read project-level settings from manifest (no hardcoded values).
GCP_PROJECT=$(jq -r '[.nodes[] | select(.resource_type == "model") | .database] | first' "$DEV_MANIFEST")
DBT_PROJECT=$(jq -r '.metadata.project_name // "unknown"' "$DEV_MANIFEST")
DEV_SCHEMA=$(jq -r '[.nodes[] | select(.resource_type == "model") | .schema] | first' "$DEV_MANIFEST")
log "GCP project: $GCP_PROJECT"
log "dbt project: $DBT_PROJECT"
log "Dev schema: $DEV_SCHEMA"

# For each requested model, extract metadata from both manifests
# Outputs one JSON file per model into $TMPDIR/models/
mkdir -p "$TMPDIR/models"

for model in $MODELS; do
    model_base="${model%+}"  # strip trailing + if present
    log "Checking $model_base..."

    # Extract from dev manifest (must exist)
    dev_info=$(jq -r --arg name "$model_base" '
        .nodes | to_entries[]
        | select(.value.resource_type == "model" and .value.name == $name)
        | .value
        | {
            name,
            schema,
            database,
            relation_name,
            original_file_path,
            materialized: .config.materialized,
            checksum: .checksum.checksum
          }
    ' "$DEV_MANIFEST" 2>/dev/null || echo "null")

    if [[ "$dev_info" == "null" || -z "$dev_info" ]]; then
        log "  ⚠️  $model_base not found in dev manifest — skipping"
        continue
    fi

    # Determine layer from path
    layer=$(echo "$dev_info" | jq -r '.original_file_path' | sed -n 's|.*models/\([^/]*\)/.*|\1|p')
    materialization=$(echo "$dev_info" | jq -r '.materialized')

    # Check if model exists in prod
    prod_info=$(jq -r --arg name "$model_base" '
        .nodes | to_entries[]
        | select(.value.resource_type == "model" and .value.name == $name)
        | .value
        | {
            name,
            schema,
            database,
            relation_name,
            checksum: .checksum.checksum
          }
    ' "$PROD_MANIFEST" 2>/dev/null || echo "null")

    is_new=false
    prod_schema=""
    prod_relation=""
    if [[ "$prod_info" == "null" || -z "$prod_info" ]]; then
        is_new=true
        log "  ✚ NEW model (no prod counterpart)"
    else
        prod_schema=$(echo "$prod_info" | jq -r '.schema')
        prod_relation=$(echo "$prod_info" | jq -r '.relation_name')
        log "  Found in prod: $prod_schema"
    fi

    dev_relation="\`${GCP_PROJECT}\`.\`${DEV_SCHEMA}\`.\`${model_base}\`"

    # Save model metadata
    jq -n \
        --arg name "$model_base" \
        --arg layer "$layer" \
        --arg mat "$materialization" \
        --arg prod_schema "$prod_schema" \
        --arg prod_relation "$prod_relation" \
        --arg dev_relation "$dev_relation" \
        --argjson is_new "$is_new" \
    '{
        name: $name,
        layer: $layer,
        materialization: $mat,
        prod_schema: (if $prod_schema == "" then null else $prod_schema end),
        prod_relation: (if $prod_relation == "" then null else $prod_relation end),
        dev_relation: $dev_relation,
        is_new: $is_new
    }' > "$TMPDIR/models/${model_base}.json"
done

MODEL_LIST=$(ls "$TMPDIR/models/" 2>/dev/null | sed 's/.json$//' | tr '\n' ' ')
MODEL_COUNT=$(echo "$MODEL_LIST" | wc -w | tr -d ' ')

if [[ "$MODEL_COUNT" -eq 0 ]]; then
    echo "ERROR: No valid models found for selector: $MODELS" >&2
    exit 1
fi
log "Found $MODEL_COUNT model(s): $MODEL_LIST"

# ═══════════════════════════════════════════════════════════════════
# STEP 2: Run sqlglot parser for code changes
# ═══════════════════════════════════════════════════════════════════
step "Step 2: Code diff (sqlglot)"

PARSE_SCRIPT="$SCRIPT_DIR/parse_columns.py"
if [[ -f "$PARSE_SCRIPT" ]]; then
    log "Running sqlglot parser..."
    if "$PYTHON" "$PARSE_SCRIPT" \
        --prod-manifest "$PROD_MANIFEST" \
        --dev-manifest "$DEV_MANIFEST" \
        --models "$MODEL_LIST" \
        > "$TMPDIR/code_changes.json" 2>/dev/null; then
        log "✓ Code diff complete"
    else
        log "⚠️  sqlglot parser failed — continuing without code changes"
        echo '{}' > "$TMPDIR/code_changes.json"
    fi
else
    log "⚠️  parse_columns.py not found — skipping code diff"
    echo '{}' > "$TMPDIR/code_changes.json"
fi

# ═══════════════════════════════════════════════════════════════════
# STEP 3: Schema diff via INFORMATION_SCHEMA
# ═══════════════════════════════════════════════════════════════════
step "Step 3: Schema diff"

for model_base in $MODEL_LIST; do
    meta=$(cat "$TMPDIR/models/${model_base}.json")
    is_new=$(echo "$meta" | jq -r '.is_new')
    prod_schema=$(echo "$meta" | jq -r '.prod_schema // empty')

    # Get dev columns (always available after build)
    log "$model_base: querying dev schema..."
    bq query --use_legacy_sql=false --format=json --max_rows=500 "
        SELECT column_name, data_type, ordinal_position
        FROM \`${GCP_PROJECT}.${DEV_SCHEMA}.INFORMATION_SCHEMA.COLUMNS\`
        WHERE table_name = '${model_base}'
        ORDER BY ordinal_position
    " 2>/dev/null > "$TMPDIR/models/${model_base}_dev_cols.json" || echo '[]' > "$TMPDIR/models/${model_base}_dev_cols.json"

    if [[ "$is_new" == "false" && -n "$prod_schema" ]]; then
        log "$model_base: querying prod schema..."
        bq query --use_legacy_sql=false --format=json --max_rows=500 "
            SELECT column_name, data_type, ordinal_position
            FROM \`${GCP_PROJECT}.${prod_schema}.INFORMATION_SCHEMA.COLUMNS\`
            WHERE table_name = '${model_base}'
            ORDER BY ordinal_position
        " 2>/dev/null > "$TMPDIR/models/${model_base}_prod_cols.json" || echo '[]' > "$TMPDIR/models/${model_base}_prod_cols.json"

        # Compute schema diff
        jq -n --slurpfile prod "$TMPDIR/models/${model_base}_prod_cols.json" \
              --slurpfile dev "$TMPDIR/models/${model_base}_dev_cols.json" '
            ($prod[0] | map({(.column_name): .data_type}) | add // {}) as $p |
            ($dev[0] | map({(.column_name): .data_type}) | add // {}) as $d |
            [
                # Added columns
                ($d | to_entries[] | select(.key as $k | $p[$k] == null)
                    | {column: .key, change_type: "added", old_type: null, new_type: .value}),
                # Removed columns
                ($p | to_entries[] | select(.key as $k | $d[$k] == null)
                    | {column: .key, change_type: "removed", old_type: .value, new_type: null}),
                # Type changed
                ($d | to_entries[] | select(.key as $k | $p[$k] != null and $p[$k] != .value)
                    | {column: .key, change_type: "type_changed", old_type: ($p[.key]), new_type: .value})
            ]
        ' > "$TMPDIR/models/${model_base}_schema_diff.json"
    else
        echo '[]' > "$TMPDIR/models/${model_base}_schema_diff.json"
    fi
done

log "✓ Schema diff complete"

# ═══════════════════════════════════════════════════════════════════
# STEP 4: Extract primary keys from manifest tests
# ═══════════════════════════════════════════════════════════════════
step "Step 4: Extract primary keys"

for model_base in $MODEL_LIST; do
    # Find columns with BOTH unique and not_null tests
    # Use exact model name match in ref() to avoid matching int_addon_catalog when looking for int_addon
    jq -r --arg name "$model_base" '
        [
            .nodes | to_entries[]
            | select(.value.resource_type == "test")
            | select(
                (.value.test_metadata.kwargs.model // "")
                | test("ref\\(.\($name).\\)")
            )
            | {test: .value.test_metadata.name, column: .value.test_metadata.kwargs.column_name}
        ]
        | group_by(.column)
        | map(select(
            (map(.test) | contains(["unique"])) and
            (map(.test) | contains(["not_null"]))
        ))
        | map(.[0].column)
    ' "$DEV_MANIFEST" > "$TMPDIR/models/${model_base}_pk.json" 2>/dev/null || echo '[]' > "$TMPDIR/models/${model_base}_pk.json"

    pk=$(cat "$TMPDIR/models/${model_base}_pk.json" | jq -r 'join(", ")')
    if [[ -n "$pk" && "$pk" != "" ]]; then
        log "$model_base: PK = $pk"
    else
        log "$model_base: no PK detected from tests"
    fi
done

# ═══════════════════════════════════════════════════════════════════
# STEP 5: Profile columns in prod and dev
# ═══════════════════════════════════════════════════════════════════
step "Step 5: Profile columns"

build_profile_query() {
    local cols_json="$1"
    local table="$2"

    # Use python to build the query dynamically from column metadata
    "$PYTHON" -c "
import json, sys
cols = json.load(open('$cols_json'))
NUMERIC = {'INT64','FLOAT64','NUMERIC','BIGNUMERIC','DECIMAL','BIGDECIMAL'}
parts = ['COUNT(*) AS _row_count']
for c in cols:
    name = c['column_name']
    safe = name.replace('-','_')
    parts.append(f'COUNT(DISTINCT \`{name}\`) AS \`{safe}__distinct\`')
    parts.append(f'COUNTIF(\`{name}\` IS NULL) AS \`{safe}__nulls\`')
    if c['data_type'] in NUMERIC:
        parts.append(f'MIN(\`{name}\`) AS \`{safe}__min\`')
        parts.append(f'MAX(\`{name}\`) AS \`{safe}__max\`')
        parts.append(f'ROUND(CAST(AVG(\`{name}\`) AS FLOAT64), 4) AS \`{safe}__mean\`')
q = 'SELECT\n  ' + ',\n  '.join(parts) + f'\nFROM $table'
print(q)
"
}

for model_base in $MODEL_LIST; do
    meta=$(cat "$TMPDIR/models/${model_base}.json")
    is_new=$(echo "$meta" | jq -r '.is_new')
    prod_schema=$(echo "$meta" | jq -r '.prod_schema // empty')
    dev_cols_file="$TMPDIR/models/${model_base}_dev_cols.json"

    # Check we have columns
    col_count=$(jq 'length' "$dev_cols_file")
    if [[ "$col_count" -eq 0 ]]; then
        log "$model_base: ⚠️  no columns found — skipping profile"
        echo '[]' > "$TMPDIR/models/${model_base}_profile_dev.json"
        echo '[]' > "$TMPDIR/models/${model_base}_profile_prod.json"
        continue
    fi

    # Profile dev
    dev_table="\`${GCP_PROJECT}\`.\`${DEV_SCHEMA}\`.\`${model_base}\`"
    dev_query=$(build_profile_query "$dev_cols_file" "$dev_table")
    log "$model_base: profiling dev ($col_count columns)..."
    bq query --use_legacy_sql=false --format=json "$dev_query" 2>/dev/null \
        > "$TMPDIR/models/${model_base}_profile_dev.json" \
        || echo '[{}]' > "$TMPDIR/models/${model_base}_profile_dev.json"

    # Profile prod (if not new)
    if [[ "$is_new" == "false" && -n "$prod_schema" ]]; then
        prod_cols_file="$TMPDIR/models/${model_base}_prod_cols.json"
        prod_col_count=$(jq 'length' "$prod_cols_file")

        if [[ "$prod_col_count" -gt 0 ]]; then
            prod_table="\`${GCP_PROJECT}\`.\`${prod_schema}\`.\`${model_base}\`"
            prod_query=$(build_profile_query "$prod_cols_file" "$prod_table")
            log "$model_base: profiling prod ($prod_col_count columns)..."
            bq query --use_legacy_sql=false --format=json "$prod_query" 2>/dev/null \
                > "$TMPDIR/models/${model_base}_profile_prod.json" \
                || echo '[{}]' > "$TMPDIR/models/${model_base}_profile_prod.json"
        else
            echo '[{}]' > "$TMPDIR/models/${model_base}_profile_prod.json"
        fi
    else
        echo '[{}]' > "$TMPDIR/models/${model_base}_profile_prod.json"
    fi
done

log "✓ Profiling complete"

# ═══════════════════════════════════════════════════════════════════
# STEP 6: Sample changed rows (EXCEPT DISTINCT)
# ═══════════════════════════════════════════════════════════════════
step "Step 6: Sample rows"

for model_base in $MODEL_LIST; do
    meta=$(cat "$TMPDIR/models/${model_base}.json")
    is_new=$(echo "$meta" | jq -r '.is_new')
    prod_schema=$(echo "$meta" | jq -r '.prod_schema // empty')
    dev_table="\`${GCP_PROJECT}\`.\`${DEV_SCHEMA}\`.\`${model_base}\`"

    if [[ "$is_new" == "true" ]]; then
        # For new models, show a few sample rows
        log "$model_base: sampling new model rows..."
        bq query --use_legacy_sql=false --format=json --max_rows=10 \
            "SELECT * FROM ${dev_table} LIMIT 10" 2>/dev/null \
            > "$TMPDIR/models/${model_base}_rows_added.json" \
            || echo '[]' > "$TMPDIR/models/${model_base}_rows_added.json"
        echo '[]' > "$TMPDIR/models/${model_base}_rows_removed.json"
        echo '[]' > "$TMPDIR/models/${model_base}_rows_modified_raw.json"
    elif [[ -n "$prod_schema" ]]; then
        prod_table="\`${GCP_PROJECT}\`.\`${prod_schema}\`.\`${model_base}\`"

        # Build common column list for EXCEPT DISTINCT (handles schema changes)
        common_cols=$(jq -r --slurpfile prod "$TMPDIR/models/${model_base}_prod_cols.json" '
            [.[] | .column_name] as $dev_cols |
            [$prod[0][] | .column_name] as $prod_cols |
            ($dev_cols - ($dev_cols - $prod_cols)) |
            map("`" + . + "`") | join(", ")
        ' "$TMPDIR/models/${model_base}_dev_cols.json")

        if [[ -z "$common_cols" ]]; then
            log "$model_base: ⚠️  no common columns for row diff — skipping"
            echo '[]' > "$TMPDIR/models/${model_base}_rows_added.json"
            echo '[]' > "$TMPDIR/models/${model_base}_rows_removed.json"
            echo '[]' > "$TMPDIR/models/${model_base}_rows_modified_raw.json"
        else

        # Rows in dev but not prod (added/changed)
        log "$model_base: finding added rows..."
        bq query --use_legacy_sql=false --format=json --max_rows=10 "
            SELECT ${common_cols} FROM ${dev_table}
            EXCEPT DISTINCT
            SELECT ${common_cols} FROM ${prod_table}
            LIMIT 10
        " 2>/dev/null > "$TMPDIR/models/${model_base}_rows_added.json" \
            || echo '[]' > "$TMPDIR/models/${model_base}_rows_added.json"

        # Rows in prod but not dev (removed/changed)
        log "$model_base: finding removed rows..."
        bq query --use_legacy_sql=false --format=json --max_rows=10 "
            SELECT ${common_cols} FROM ${prod_table}
            EXCEPT DISTINCT
            SELECT ${common_cols} FROM ${dev_table}
            LIMIT 10
        " 2>/dev/null > "$TMPDIR/models/${model_base}_rows_removed.json" \
            || echo '[]' > "$TMPDIR/models/${model_base}_rows_removed.json"

        # Modified rows — find PKs that exist in both but have different values
        pk_json=$(cat "$TMPDIR/models/${model_base}_pk.json")
        pk_count=$(echo "$pk_json" | jq 'length')

        if [[ "$pk_count" -gt 0 ]]; then
            # Build PK join condition and column list
            pk_cols=$(echo "$pk_json" | jq -r 'map("p.`" + . + "` = d.`" + . + "`") | join(" AND ")')
            pk_select=$(echo "$pk_json" | jq -r 'map("p.`" + . + "`") | join(", ")')

            # Use common non-PK columns for change detection (handles schema diffs)
            non_pk_cols=$(jq -r --argjson pks "$pk_json" \
                --slurpfile prod "$TMPDIR/models/${model_base}_prod_cols.json" '
                [.[] | .column_name] as $dev_cols |
                [$prod[0][] | .column_name] as $prod_cols |
                ($dev_cols - ($dev_cols - $prod_cols)) - $pks |
                map("`" + . + "`") | join(", ")
            ' "$TMPDIR/models/${model_base}_dev_cols.json")

            if [[ -n "$non_pk_cols" ]]; then
                # Build CONCAT for FARM_FINGERPRINT hash using common non-pk columns
                # Use COALESCE to handle NULLs (otherwise CONCAT returns NULL
                # and NULL != NULL is false, missing all rows with any null col)
                non_pk_concat=$(jq -r --argjson pks "$pk_json" \
                    --slurpfile prod "$TMPDIR/models/${model_base}_prod_cols.json" '
                    [.[] | .column_name] as $dev_cols |
                    [$prod[0][] | .column_name] as $prod_cols |
                    ($dev_cols - ($dev_cols - $prod_cols)) - $pks |
                    map("COALESCE(CAST(`" + . + "` AS STRING), \"__NULL__\")") | join(", \"|\", ")
                ' "$TMPDIR/models/${model_base}_dev_cols.json")

                # Build common column select (without _hash) for the output
                common_select=$(jq -r --slurpfile prod "$TMPDIR/models/${model_base}_prod_cols.json" '
                    [.[] | .column_name] as $dev_cols |
                    [$prod[0][] | .column_name] as $prod_cols |
                    ($dev_cols - ($dev_cols - $prod_cols)) |
                    map("`" + . + "`") | join(", ")
                ' "$TMPDIR/models/${model_base}_dev_cols.json")

                log "$model_base: finding modified rows..."
                # Return both prod and dev versions of changed rows, tagged with _source
                bq query --use_legacy_sql=false --format=json --max_rows=20 "
                    WITH prod AS (
                        SELECT ${common_select}, FARM_FINGERPRINT(CONCAT(${non_pk_concat})) AS _hash
                        FROM ${prod_table}
                    ),
                    dev AS (
                        SELECT ${common_select}, FARM_FINGERPRINT(CONCAT(${non_pk_concat})) AS _hash
                        FROM ${dev_table}
                    ),
                    changed_pks AS (
                        SELECT ${pk_select}
                        FROM prod AS p
                        INNER JOIN dev AS d ON ${pk_cols}
                        WHERE p._hash != d._hash
                        LIMIT 10
                    )
                    SELECT 'prod' AS _source, p.* EXCEPT(_hash)
                    FROM prod AS p
                    INNER JOIN changed_pks AS c ON $(echo "$pk_json" | jq -r 'map("p.`" + . + "` = c.`" + . + "`") | join(" AND ")')
                    UNION ALL
                    SELECT 'dev' AS _source, d.* EXCEPT(_hash)
                    FROM dev AS d
                    INNER JOIN changed_pks AS c ON $(echo "$pk_json" | jq -r 'map("d.`" + . + "` = c.`" + . + "`") | join(" AND ")')
                " 2>/dev/null > "$TMPDIR/models/${model_base}_rows_modified_raw.json" \
                    || echo '[]' > "$TMPDIR/models/${model_base}_rows_modified_raw.json"
            else
                echo '[]' > "$TMPDIR/models/${model_base}_rows_modified_raw.json"
            fi
        else
            echo '[]' > "$TMPDIR/models/${model_base}_rows_modified_raw.json"
        fi

        fi  # end common_cols check
    else
        echo '[]' > "$TMPDIR/models/${model_base}_rows_added.json"
        echo '[]' > "$TMPDIR/models/${model_base}_rows_removed.json"
        echo '[]' > "$TMPDIR/models/${model_base}_rows_modified_raw.json"
    fi
done

log "✓ Sample rows complete"

# ═══════════════════════════════════════════════════════════════════
# STEP 7: Assemble JSON
# ═══════════════════════════════════════════════════════════════════
step "Step 7: Assemble results"

BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
CODE_CHANGES=$(cat "$TMPDIR/code_changes.json")

# Assemble per-model JSON using python (complex JSON merging)
"$PYTHON" << PYEOF > "$TMPDIR/result.json"
import json, os, sys

tmpdir = "$TMPDIR"
models_dir = os.path.join(tmpdir, "models")
# parse_columns.py returns a list of {model, code_changes} — convert to dict
_code_raw = json.load(open(os.path.join(tmpdir, "code_changes.json")))
if isinstance(_code_raw, list):
    code_changes_all = {item["model"]: item.get("code_changes") for item in _code_raw if "model" in item}
elif isinstance(_code_raw, dict):
    code_changes_all = _code_raw
else:
    code_changes_all = {}

NUMERIC_TYPES = {"INT64","FLOAT64","NUMERIC","BIGNUMERIC","DECIMAL","BIGDECIMAL"}

models = []
total_row_delta = 0
models_changed = 0
models_new = 0
risk_indicators = []

for fname in sorted(os.listdir(models_dir)):
    if not fname.endswith(".json") or "_" in fname.split(".json")[0][1:]:
        # Only process base model files like "int_order_pricing.json"
        # Skip derivative files like "int_order_pricing_dev_cols.json"
        # Logic: after removing .json, if there's an underscore after the first char
        # of the known model file pattern, it's a derivative
        continue

model_names = [f.replace(".json","") for f in os.listdir(models_dir)
               if f.endswith(".json") and not any(
                   f.endswith(s) for s in [
                       "_dev_cols.json", "_prod_cols.json",
                       "_schema_diff.json", "_pk.json",
                       "_profile_dev.json", "_profile_prod.json",
                       "_rows_added.json", "_rows_removed.json",
                       "_rows_modified_raw.json"
                   ]
               )]

for model_name in sorted(model_names):
    meta = json.load(open(os.path.join(models_dir, f"{model_name}.json")))
    is_new = meta["is_new"]

    # Load all artifacts
    def load(suffix, default="[]"):
        path = os.path.join(models_dir, f"{model_name}{suffix}")
        if os.path.exists(path):
            try:
                return json.load(open(path))
            except:
                return json.loads(default)
        return json.loads(default)

    dev_cols = load("_dev_cols.json")
    prod_cols = load("_prod_cols.json")
    schema_diff = load("_schema_diff.json")
    pk = load("_pk.json")
    profile_dev_raw = load("_profile_dev.json")
    profile_prod_raw = load("_profile_prod.json")
    rows_added_raw = load("_rows_added.json")
    rows_removed_raw = load("_rows_removed.json")
    rows_modified_raw = load("_rows_modified_raw.json")

    # Parse profiles — bq returns a list with one row
    profile_dev = profile_dev_raw[0] if profile_dev_raw else {}
    profile_prod = profile_prod_raw[0] if profile_prod_raw else {}

    # Row counts
    dev_row_count = int(profile_dev.get("_row_count", 0))
    prod_row_count = int(profile_prod.get("_row_count", 0)) if not is_new else None

    if is_new:
        row_delta = None
        row_delta_pct = None
        models_new += 1
    else:
        row_delta = dev_row_count - prod_row_count
        row_delta_pct = round((row_delta / prod_row_count * 100), 2) if prod_row_count else 0
        total_row_delta += row_delta
        models_changed += 1

        if row_delta < 0 and abs(row_delta_pct) > 0.1:
            risk_indicators.append({
                "level": "warning",
                "model": model_name,
                "message": f"Row count decreased by {abs(row_delta):,} ({row_delta_pct:+.2f}%)"
            })

    # Build column profiles
    schema_additions = {s["column"] for s in schema_diff if s["change_type"] == "added"}
    prod_col_map = {c["column_name"]: c["data_type"] for c in prod_cols}

    column_profiles = []
    for col in dev_cols:
        cname = col["column_name"]
        ctype = col["data_type"]
        safe = cname.replace("-", "_")
        is_pk = cname in pk
        is_schema_add = cname in schema_additions

        # Extract dev profile values
        dev_distinct = int(profile_dev.get(f"{safe}__distinct", 0))
        dev_nulls = int(profile_dev.get(f"{safe}__nulls", 0))
        dev_null_pct = round(dev_nulls / dev_row_count * 100, 2) if dev_row_count else 0
        dev_min = profile_dev.get(f"{safe}__min")
        dev_max = profile_dev.get(f"{safe}__max")
        dev_mean = profile_dev.get(f"{safe}__mean")

        # Convert numeric strings
        def to_num(v):
            if v is None: return None
            try: return float(v)
            except: return v

        dev_profile = {
            "distinct_count": dev_distinct,
            "null_count": dev_nulls,
            "null_pct": dev_null_pct,
            "min": to_num(dev_min),
            "max": to_num(dev_max),
            "mean": to_num(dev_mean)
        }

        # Extract prod profile values (if not new and column exists in prod)
        prod_profile = None
        if not is_new and not is_schema_add and cname in prod_col_map:
            prod_distinct = int(profile_prod.get(f"{safe}__distinct", 0))
            prod_nulls = int(profile_prod.get(f"{safe}__nulls", 0))
            prod_null_pct = round(prod_nulls / prod_row_count * 100, 2) if prod_row_count else 0
            prod_min = profile_prod.get(f"{safe}__min")
            prod_max = profile_prod.get(f"{safe}__max")
            prod_mean = profile_prod.get(f"{safe}__mean")

            prod_profile = {
                "distinct_count": prod_distinct,
                "null_count": prod_nulls,
                "null_pct": prod_null_pct,
                "min": to_num(prod_min),
                "max": to_num(prod_max),
                "mean": to_num(prod_mean)
            }

        # Determine if data changed
        is_changed = False
        if prod_profile and not is_schema_add:
            if (prod_profile["distinct_count"] != dev_profile["distinct_count"] or
                prod_profile["null_pct"] != dev_profile["null_pct"] or
                prod_profile["min"] != dev_profile["min"] or
                prod_profile["max"] != dev_profile["max"] or
                prod_profile["mean"] != dev_profile["mean"]):
                is_changed = True

        if is_schema_add:
            is_changed = True

        # Check for null% spikes (risk indicator)
        if prod_profile and not is_schema_add:
            null_delta = dev_profile["null_pct"] - prod_profile["null_pct"]
            if abs(null_delta) > 5:
                risk_indicators.append({
                    "level": "warning",
                    "model": model_name,
                    "message": f"Null % for {cname} changed from {prod_profile['null_pct']}% to {dev_profile['null_pct']}%"
                })

        cp = {
            "column": cname,
            "data_type": ctype,
            "is_primary_key": is_pk,
            "is_changed": is_changed,
            "prod": prod_profile,
            "dev": dev_profile
        }
        if is_schema_add:
            cp["is_schema_addition"] = True
        column_profiles.append(cp)

    # Code changes for this model
    code_changes = code_changes_all.get(model_name)

    # Build sample_rows with before/after pairing for modified rows
    rows_modified = []
    modified_pk_values = set()

    if rows_modified_raw and pk:
        # Separate prod and dev versions (don't mutate with pop —
        # .pop on the first pass empties _source before the second pass)
        prod_rows = [{k: v for k, v in r.items() if k != "_source"}
                     for r in rows_modified_raw if r.get("_source") == "prod"]
        dev_rows = [{k: v for k, v in r.items() if k != "_source"}
                    for r in rows_modified_raw if r.get("_source") == "dev"]

        # Build lookup by PK
        def pk_key(row):
            return tuple(str(row.get(k, "")) for k in pk)

        prod_by_pk = {pk_key(r): r for r in prod_rows}
        dev_by_pk = {pk_key(r): r for r in dev_rows}

        for pkv in prod_by_pk:
            prod_row = prod_by_pk[pkv]
            dev_row = dev_by_pk.get(pkv)
            if dev_row is None:
                continue
            # Find columns that actually differ
            changes = {}
            for col in prod_row:
                if col in pk:
                    continue
                pv = prod_row.get(col)
                dv = dev_row.get(col)
                if str(pv) != str(dv):
                    changes[col] = {"prod": pv, "dev": dv}
            if changes:
                pk_dict = {k: prod_row[k] for k in pk}
                rows_modified.append({"primary_key": pk_dict, "changes": changes})
                modified_pk_values.add(pkv)

    # Deduplicate: remove modified PKs from added/removed
    rows_added = rows_added_raw or []
    rows_removed = rows_removed_raw or []
    if modified_pk_values and pk:
        def row_pk_key(row):
            return tuple(str(row.get(k, "")) for k in pk)
        rows_added = [r for r in rows_added if row_pk_key(r) not in modified_pk_values]
        rows_removed = [r for r in rows_removed if row_pk_key(r) not in modified_pk_values]

    sample_rows = {
        "added": rows_added,
        "removed": rows_removed,
        "modified": rows_modified
    }

    model_obj = {
        "name": model_name,
        "layer": meta["layer"],
        "materialization": meta["materialization"],
        "prod_schema": meta["prod_schema"],
        "prod_relation": meta["prod_relation"],
        "dev_relation": meta["dev_relation"],
        "is_new": is_new,
        "primary_key": pk,
        "row_count": {
            "prod": prod_row_count,
            "dev": dev_row_count,
            "delta": row_delta,
            "delta_pct": row_delta_pct
        },
        "schema_changes": schema_diff,
        "code_changes": code_changes,
        "column_profiles": column_profiles,
        "sample_rows": sample_rows
    }
    models.append(model_obj)

result = {
    "metadata": {
        "generated_at": "$TIMESTAMP",
        "git_branch": "$BRANCH",
        "model_selector": "$MODELS",
        "dbt_project": "$DBT_PROJECT",
        "gcp_project": "$GCP_PROJECT",
        "prod_schema_source": "$PROD_MANIFEST",
        "dev_schema": "$DEV_SCHEMA"
    },
    "summary": {
        "models_changed": models_changed,
        "models_new": models_new,
        "total_row_delta": total_row_delta,
        "total_row_delta_pct": round(total_row_delta / sum(
            m["row_count"]["prod"] for m in models if m["row_count"]["prod"]
        ) * 100, 2) if any(m["row_count"]["prod"] for m in models) else 0,
        "risk_indicators": risk_indicators
    },
    "models": models
}

print(json.dumps(result))
PYEOF

log "✓ JSON assembled"

# ═══════════════════════════════════════════════════════════════════
# STEP 8: Generate HTML and open in browser
# ═══════════════════════════════════════════════════════════════════
step "Step 8: Generate HTML"

TIMESTAMP_SLUG=$(date +"%Y%m%d-%H%M%S")
BRANCH_SLUG=$(echo "$BRANCH" | tr '/' '-' | tr -cd '[:alnum:]-')
OUTPUT_FILE="${DIAGRAMS_DIR}/data-diff-${BRANCH_SLUG}-${TIMESTAMP_SLUG}.html"

# Inject JSON into template
"$PYTHON" -c "
import json
data = json.load(open('$TMPDIR/result.json'))
template = open('$SCRIPT_DIR/template.html').read()
output = template.replace('__DATA_PLACEHOLDER__', json.dumps(data))
with open('$OUTPUT_FILE', 'w') as f:
    f.write(output)
print(f'Written {len(output)} bytes')
"

log "✓ HTML generated: $OUTPUT_FILE"

# Open in browser
if [[ "$(uname)" == "Darwin" ]]; then
    open "$OUTPUT_FILE"
elif command -v xdg-open &>/dev/null; then
    xdg-open "$OUTPUT_FILE"
fi

echo "" >&2
echo "════════════════════════════════════════════════════" >&2
echo "  ✅ Data diff complete" >&2
echo "  📄 $OUTPUT_FILE" >&2
echo "════════════════════════════════════════════════════" >&2

# Also print the file path to stdout for programmatic use
echo "$OUTPUT_FILE"
