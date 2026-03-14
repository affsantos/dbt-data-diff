# dbt Data Diff

Visual data diff for dbt + BigQuery. Compare production vs development
data after model changes and get an interactive HTML report — right in
your local dev loop or CI pipeline.

![dbt Data Diff](examples/screenshot.png)

## What it does

After you modify dbt models and run `dbt build`, this tool compares
your dev tables against production across **three layers**:

| Layer | Tool | What it answers | Cost |
|-------|------|-----------------|------|
| **Code diff** | [sqlglot](https://github.com/tobymao/sqlglot) (local) | What changed in the SQL? Which expressions? Which CTEs? | Zero — parses manifests locally |
| **Schema diff** | `INFORMATION_SCHEMA` | Columns added / removed / retyped? | 1 fast query per model |
| **Data diff** | BigQuery profiling | Row counts, null %, distinct counts, min/max/mean — what actually changed? | 1 query per model per env |

The result is a **self-contained HTML page** you open in your browser
with:

- Summary with risk indicators (row drops, null spikes)
- Per-model cards with row count deltas, schema changes, code diffs
- Side-by-side column profiles (prod vs dev) with changed columns
  highlighted
- Sample rows showing added, removed, and modified records

## Quick Start

### Prerequisites

- A **dbt project** targeting **BigQuery**
- `gcloud` CLI authenticated to your GCP project
- `bq` and `jq` installed
- Python 3.11+ with `sqlglot` (`pip install sqlglot`)
- Both manifests available:
  - `target_prod/manifest.json` (production)
  - `target/manifest.json` (development)
- Models already built in your dev schema (`dbt build`)

### Run

```bash
# Single model
./data-diff.sh int_order_pricing

# Multiple models
./data-diff.sh "int_order_pricing int_product_inventory"
```

The script:
1. Reads your GCP project, dbt project name, and dev schema from the
   manifest — **zero configuration needed**
2. Runs the 8-step pipeline (~30-60 sec for 5 models)
3. Opens the HTML report in your browser

### Typical workflow

```
dbt build --select my_model        # 1. Build your changes
./data-diff.sh my_model            # 2. See what changed
                                   # 3. Review the HTML page
git add . && git commit            # 4. Commit with confidence
```

## How It Works

```
data-diff.sh <model_selector>
  │
  ├─ 1. Manifest diff         → identify modified models (local)
  ├─ 2. sqlglot AST diff      → column expression + CTE changes (local)
  ├─ 3. INFORMATION_SCHEMA    → schema diff (1 query/model)
  ├─ 4. Primary key extract   → from dbt test metadata (local)
  ├─ 5. Column profiling      → all columns, prod & dev (1 query/model/env)
  ├─ 6. Sample rows           → EXCEPT DISTINCT + PK join (1 query/model)
  ├─ 7. JSON assembly         → merge all results
  └─ 8. HTML generation       → inject JSON into template, open browser
```

All project-specific values (GCP project, dbt project name, dev schema,
prod schemas) are **read from the manifests automatically**.

## Reading the Output

### Colour key

| Colour | Meaning |
|--------|---------|
| 🟢 Green | Safe — no data change or expected increase |
| 🟡 Amber | Attention — unexpected metric shift |
| 🔴 Red | Risk — row count drop, columns removed, null spike |
| 🆕 NEW | Model has no production counterpart |

### Risk indicators

The summary flags:
- Row count drops > 1%
- Null % increases > 5 percentage points
- Columns removed from schema
- CTE changes that indirectly affect all columns

## CI Integration

Add data-diff to your GitHub Actions pipeline. It runs after `dbt build`
and posts results as a PR comment:

```yaml
# In your dbt CI workflow, after dbt build succeeds:
- name: Run data diff
  continue-on-error: true  # never blocks the PR
  run: |
    ./data-diff.sh "$CHANGED_MODELS" --format markdown \
      > /tmp/data-diff-comment.md

- name: Post data diff to PR
  uses: marocchino/sticky-pull-request-comment@v2
  with:
    header: data-diff
    path: /tmp/data-diff-comment.md
```

> **Note**: `--format markdown` is a planned feature. See the roadmap
> below.

## AI Agent Integration

This tool includes a `SKILL.md` for integration with
[pi](https://github.com/mariozechner/pi-coding-agent) or Claude Code.
The agent learns when and how to invoke data-diff and summarises the
results in chat.

```
Developer: "validation passed, looks good"
Agent:     "Running data diff..."
Agent:     "3 models diffed. int_order_pricing: -340 rows (-0.22%),
            expression changed on total_price_coalesced.
            See full diff: .data-diff/data-diff-feature-xyz-20260314.html"
```

## File Structure

```
├── data-diff.sh          # Main orchestrator (bash)
├── parse_columns.py      # sqlglot AST diff (python)
├── template.html         # Self-contained HTML/CSS/JS renderer
├── contract.json         # JSON schema reference with sample data
├── SKILL.md              # AI agent skill definition
└── README.md
```

## Limitations

- **BigQuery only** — uses `bq` CLI and BigQuery-specific functions.
  Multi-warehouse support (Snowflake, Databricks) is a future goal.
- **Modified models only** — does not profile downstream dependents
- **sqlglot is best-effort** — complex Jinja (`{% for %}`, `{% if %}`)
  falls back to compiled SQL when available
- **No incremental awareness** — profiles the full table, not the
  incremental slice

## Roadmap

- [ ] **Modified row pairing** — show before/after values for changed
  rows instead of separate added/removed
- [ ] `--format markdown` — output PR-ready Markdown instead of HTML
- [ ] `--upload` — push HTML to GCS and return a shareable URL
- [ ] **Snowflake adapter** — abstract the query layer for
  multi-warehouse support
- [ ] **Downstream profiling** — optionally profile downstream models
  affected by the change

## License

Apache 2.0 — see [LICENSE](LICENSE).

## Contributing

Contributions welcome! Please open an issue to discuss before submitting
large changes. The JSON contract (`contract.json`) is the interface
between data collection and rendering — keep it stable.
