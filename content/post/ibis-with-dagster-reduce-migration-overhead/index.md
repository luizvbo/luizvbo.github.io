+++
title = "Portable data pipelines with Dagster + Ibis: making migrations less painful"
date = 2026-10-05
description = "One Ibis expression, many engines. A small Dagster repo showing how engine migrations can become a config change instead of a rewrite."

[extra]
mermaid = true
+++

# Portable data pipelines with Dagster + Ibis: making migrations less painful

<!-- prettier-ignore-start -->
{% <mermaid> %}
flowchart LR
    code["same pipeline code<br/>assets + ibis expressions<br/>(written once)"]
    code -->|"deployment config<br/>swaps engine + storage"| dep
    subgraph dep["pick a backend"]
        direction TB
        a["duckdb<br/>local csv files"] ~~~ b["polars<br/>same csv files"] ~~~ c["pyspark<br/>parquet lake"] ~~~ d["bigquery, trino, ...<br/>a new connection branch + config"]
    end
{% </mermaid> %}
<!-- prettier-ignore-end -->

Data-platform migrations rarely stop at infrastructure. A team moving to a
new cloud provider eventually faces a harder question: keep the pipelines on
their current engine, or rewrite them for the destination warehouse?

Take a typical PySpark codebase aimed at a warehouse like BigQuery: every Spark
DataFrame call, `spark.sql(...)` string, and `.toPandas()` escape becomes
something that may need to be rewritten. Whether that rewrite is worthwhile
comes down to cost, effort, and long-term maintainability.

Platform choices also keep moving for reasons outside engineering: cost,
capability, regulation, data-sovereignty debates (see [this Reuters
article](https://www.reuters.com/business/eu-targets-big-tech-dependence-with-made-in-europe-drive-2026-06-03/)
for wider industry context). Today's carefully chosen destination can be
tomorrow's starting point for the next migration.

That raises the question this post attacks: _can transformation logic be
written so that switching the execution engine is mostly configuration and
integration work, not a rewrite?_

This post is about a small proof-of-concept repo I built to try to answer this
question. While evaluating alternatives, I found **[Ibis](https://ibis-project.org)**, a potential
solution to minimize the migration effort. It is a dataframe-style expression
API that compiles the same code to DuckDB, Spark SQL, BigQuery, Snowflake,
Polars, and many other backends.

Ibis minimizes the migration effort, but a translation layer alone doesn't
run a pipeline. The experiment also needed structure and orchestration, the
part teams often fill with an orchestrator plus in-house framework code.
**[Dagster](https://dagster.io)** can provide that with many features provided
out-of-the-box. It is an orchestrator built around software-defined assets, giving
the structure dbt users are used to and more: models, lineage, tests, schedules.

With this combination, you can **port your logic to Ibis once, and engine
migrations become mostly config diffs.** Notice the port to Ibis is real work,
but the transformation layer no longer needs a full rewrite on every engine
change.

**You can check the repository in** [github.com/luizvbo/ibis-dagster-example](https://github.com/luizvbo/ibis-dagster-example)

## The demo

A deliberately ordinary bronze → silver → gold pipeline:

<!-- prettier-ignore-start -->
{% <mermaid full_width={true}> %}
flowchart LR
    subgraph sources["External sources:<br>- CSV locally<br>- Parquet/tables in production"]
        events
        products
    end
    subgraph bronze["Bronze:<br>- Land sources into managed tables"]
        raw_events
        raw_products
    end
    subgraph silver["Silver:<br>- Normalize types<br>- Trim/lower strings<br>- Dedupe<br>- Validate"]
        cleaned_events
    end
    subgraph gold
        daily_active_users
        category_revenue
        latest_event_per_user["latest_event_per_user*"]
    end
    events --> raw_events
    products --> raw_products
    raw_events --> cleaned_events
    cleaned_events --> daily_active_users
    cleaned_events --> category_revenue
    raw_products --> category_revenue
    cleaned_events --> latest_event_per_user
{% </mermaid> %}
<!-- prettier-ignore-end -->

\* I'll come back to that asterisk (`latest_event_per_user`). It's the most interesting part.

![Dagster asset lineage: external event and product sources feeding bronze, silver, and gold assets](img/global-asset-lineage.svg)

Each deployment bundles backend and storage configuration. Selecting one takes a single environment variable:

```bash
DAGSTER_DEPLOYMENT_NAME=local  uv run dagster dev   # duckdb + local CSVs
DAGSTER_DEPLOYMENT_NAME=polars uv run dagster dev   # polars + same CSVs
DAGSTER_DEPLOYMENT_NAME=prod   uv run dagster dev   # pyspark + parquet "lake" sources
```

Same code. Same asset graph. Same checks. Different engine and different storage, selected by deployment config, the documented Dagster pattern (`resources_by_deployment`, keyed on `DAGSTER_DEPLOYMENT_NAME` is an application-defined variable the repo reads itself, so configure it per deployment in Dagster+ or on your code location's environment when self-hosting). One honest boundary: a backend the repo doesn't implement yet, say BigQuery, still needs a new connection branch in `IbisResource` plus its own storage config. What stays untouched is the transform code.

## The three pieces

### 1. Transforms are pure Ibis expressions

```python
# transforms.py: no imports from any engine, just ibis
def clean_events(raw_events: ir.Table) -> ir.Table:
    normalized = raw_events.mutate(
        ts=_.ts.cast("timestamp"),
        amount=_.amount.cast("float64"),
        event_type=_.event_type.lower().strip(),
    )
    return (
        normalized.mutate(
            amount=_.amount.fill_null(0.0), date=_.ts.truncate("D")
        )
        .filter(_.event_id.notnull(), _.user_id.notnull())
        .distinct()
    )
```

No `duckdb.`, no `spark.`, no `pl.` anywhere. These functions are lazy `Table -> Table` expressions with no backend or storage logic: they build an expression tree, and execution happens only when the IO manager asks the selected backend to materialize it. They don't even know which backend they'll run on until Dagster binds a connection at runtime.

### 2. An IO manager owns all data I/O

This is the Dagster-native answer to dbt's "where does this model land" or Kedro's catalog. Assets declare inputs/outputs as `ir.Table` and `IbisIOManager` (a `ConfigurableIOManager`) handles persistence:

- `load_input` → `con.table(name)` for pipeline tables, or `con.read_csv/read_parquet/...` for external sources configured per deployment
- `handle_output` → `con.create_table(asset_name, expr, overwrite=True)` plus metadata (row count, preview, **compiled SQL** where the backend produces SQL)

So "the same pipeline, but sources are parquet in a lake in prod" is literally just:

```python
# resources_by_deployment: same logical source, different physical read
"local": IbisIOManager(
    sources={"events": {"format": "csv",     "path": "data/raw_events.csv"}}, ...),
"prod":  IbisIOManager(
    sources={"events": {"format": "parquet", "path": "${DATA_LAKE}/landing/events/"}}, ...),
```

### 3. Quality gates are portable too

The thing dbt users ask about first is `dbt test`. Dagster's `@asset_check` is the direct analog, and because _the checks themselves are Ibis expressions_, they're portable as well:

```python
@dg.asset_check(asset=cleaned_events, blocking=True)
def cleaned_events_no_null_user_ids(ibis: IbisResource) -> dg.AssetCheckResult:
    n = _violations(ibis, "cleaned_events", _.user_id.isnull())
    return dg.AssetCheckResult(passed=n == 0, metadata={"null_user_ids": n})
```

(`_violations` is a small helper: `int(con.table(table).filter(predicate).count().execute())` on the run's backend.)

<!-- IMAGE: screenshot of the Checks tab / a run showing the 8 asset checks green -->

The repo implements the usual dbt generic tests `not_null`, `unique`, `accepted_values`, `relationships` (an anti-join), plus a couple of custom checks. Marking the not-null check `blocking=True` gives behavior analogous to a `dbt build` quality gate: if it fails, downstream assets don't materialize.

And since Dagster is an orchestrator, scheduling is native too: the repo ships a `daily_schedule` (`0 6 * * *`) covering the whole job, stopped by default so it can be toggled on from the UI.

## The "aha": one expression, three dialects

This is the part that sells it. The same `daily_active_users(clean_events(raw_events))` expression is compiled offline. **No running engine, no credentials, no JVM** (the backend packages still have to be installed):

```sql
-- duckdb
... DATE_TRUNC('DAY', "t0"."ts") AS "date" ... TRIM(LOWER("t0"."event_type"), ' ')
-- pyspark
... DATE_TRUNC('day', `t0`.`ts`) AS `date` ... TRIM(' ' FROM LOWER(`t0`.`event_type`))
-- bigquery
... TIMESTAMP_TRUNC(`t0`.`ts`, DAY) AS `date` ... TRIM(LOWER(`t0`.`event_type`), ' ')
```

`DATE_TRUNC` vs `TIMESTAMP_TRUNC`, double quotes vs backticks, `TRIM(x)` vs `TRIM(' ' FROM x)`: all the dialect trivia you never want to hand-maintain, generated from one expression.

<!-- IMAGE (optional): screenshot of a materialization's metadata tab showing the compiled SQL recorded on the run -->

This isn't just a demo trick. `ibis.<backend>.compile()` works without connecting, so a `pytest` file that compiles every transform against every target dialect is a **CI guardrail for translatability**: if someone adds an operation a target compiler can't express, it fails before deployment, not after (semantic and connector differences are what the runtime tests cover). The repo wires this in concretely: a `prek` pre-push hook runs lint (ruff), type-checking (ty), and that test suite on every push.

## The honest part: portability has edges

Here's where the demo stays honest, because the claim above deserves an asterisk: **"backend-agnostic" means portable across the operations each backend can express, not that every expression runs everywhere.**

The repo deliberately includes `latest_event_per_user`, built on a window function:

```python
events.mutate(rn=ibis.row_number().over(
    ibis.window(group_by="user_id", order_by=_.ts.desc()))).filter(_.rn == 0)
```

Ibis's Polars backend has **no** window-function translation (Polars natively has `.over()`, but Ibis doesn't map to it yet). Materializing the graph on Polars gives:

```
latest_event_per_user FAILED:
ibis.common.exceptions.OperationNotDefinedError:
    No translation rule for WindowFunction
```

<!-- IMAGE: screenshot of the Dagster run on the polars deployment: all steps green except latest_event_per_user red, with the OperationNotDefinedError visible in the logs pane -->

Two things make this acceptable, even good:

1. **It fails loudly and early.** The error fires at translate time, before any data is processed, names the exact operation, and is deterministic. No silent wrong answers at 3:00 AM.
2. **The mitigations are boring** (in a good way):
   - **Rewrite portably**: "latest per group" is `group_by` + `join`
     (`latest_event_per_user_portable` in the repo). Clunkier, but runs
     everywhere.
   - **Localize a backend-specific branch**: `ibis.get_backend(t).name` inside a transform
     tells you which engine you're bound to. It's ugly, but contained to one
     function.
   - **Scope the asset to supported engines**: accept that a given deployment can't materialize it.

A more subtle gotcha I hit the hard way: `ibis.row_number()` is **zero-based**. It compiles to `ROW_NUMBER() - 1`. `rn == 1` silently gives you the _second_-latest row. Abstraction means portable _syntax_; you still need to learn the portable _semantics_.

Before betting a codebase on a portability claim, check Ibis's [per-backend operations support matrix](https://ibis-project.org/backends/support/matrix).

## Okay, but why not just dbt?

Fair question. dbt solves a big chunk of this. The honest comparison:

|                                       | dbt (+ adapters)                        | Dagster + Ibis                   |
| ------------------------------------- | --------------------------------------- | -------------------------------- |
| Transform language                    | SQL                                     | Python expressions → SQL/plans   |
| Engine portability                    | per-adapter SQL macros                  | one expression → many dialects   |
| Non-SQL engines                       | no                                      | yes (Polars, DataFusion…)        |
| Python-native logic (ML, APIs, files) | adapter-dependent Python models         | first-class (the same graph)     |
| Tests                                 | `dbt test`                              | `@asset_check`                   |
| Scheduling                            | needs dbt Cloud / external orchestrator | native (schedules, sensors)      |
| Incremental models                    | `dbt run` incremental                   | partitions + automation policies |
| Ecosystem maturity                    | bigger                                  | younger                          |

If your transforms are pure SQL and your targets are SQL warehouses, dbt is simpler and battle-tested and you should consider it. The Dagster + Ibis combo becomes attractive when your logic outgrows SQL, you want one lineage graph spanning tables _and_ non-table work, or (the motivation behind this example) you're tired of paying a rewrite tax on every engine migration.

## Caveats worth stating

- **The port isn't free.** Existing `spark.sql`/DataFrame/UDF code must be rewritten as Ibis expressions. The payoff is that each future engine change becomes smaller, more localized work.
- **Semantics differ subtly per backend** (see the zero-based `row_number` story). Also type inference on file reads (the demo added portable casts to absorb that).
- **Performance isn't portable.** One expression compiles to all engines, but partitioning, clustering, broadcast hints, etc. are still per-engine work.
- **Some plumbing is real**: e.g., Polars tables live per-connection and DuckDB files allow one writer, so the demo uses the in-process executor and a process-scoped cached connection.
- **Writes are full-refresh.** The demo uses `create_table(..., overwrite=True)`; incremental loads, partitioning, and merge semantics would be backend-aware work on top.

## Takeaways

- **Ibis gives you a portability contract for transformation logic**: same expressions, engine as config. Parametrized tests that run every transform on every available backend, plus offline dialect compiles for targets you can't run locally, turn "portable" from a claim into a tested property.
- **Dagster supplies a structure that will feel familiar to dbt users**: assets as models, `deps` ≈ `ref()`, `@asset_check` as tests, schedules instead of cron.
- **The boundary is visible and fails safely**: unsupported ops raise `OperationNotDefinedError` at translate time, and the repo shows three ways to handle it.
- **The real pitch isn't "write once, run anywhere"**, it's "port once, and the next migration rewrites config and integration points, not the transformation logic," plus knowing exactly where "anywhere" ends.

<!-- prettier-ignore-start -->
{% <mermaid> %}
flowchart LR
    code["pipeline code<br/>(write once)"]
    code -->|"engine: config"| eng["duckdb | polars | pyspark | ..."]
    code -->|"storage: config"| st["csv | parquet | warehouse tables"]
{% </mermaid> %}
<!-- prettier-ignore-end -->
