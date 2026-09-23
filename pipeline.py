#!/usr/bin/env python3
"""Config-driven ETL pipeline for hospital encounter records.

Transform, filter, derive and validation rules are read from config.yaml, except the date_of_birth-in-the-future check, which is hardcoded.

Call sequence for one run of `python3 pipeline.py --config config.yaml`
(each function's own docstring has the detail):

    main()
    -> run_pipeline()
       -> load_config()
       -> resolve_relative_to_config()            # source DB path
       -> get_table_columns()
       -> build_query()                           # -> parse_bucket_expr(),
                                                    #    build_age_band_case_sql()
       -> classify_row()                          # once per fetched row
       -> cast_output_value()                     # once per output field per row
       -> resolve_relative_to_config()            # x3, output paths
       -> write_csv() x2 + write_json() (summary), or write_json() x3

"""
import argparse
import csv
import json
import re
import sqlite3
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import yaml


def load_config(path: str) -> dict:
    """Read the pipeline's YAML config file into a plain dict.

    No schema validation happens here on purpose: this script controls
    both the config format and the config files, so we trust our own
    input.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        raise ValueError(f"Config file not found: {path!r}") from None


def resolve_relative_to_config(config_path: str, target_path: str) -> str:
    """Resolve a path from the config file relative to the config file's own
    directory, not the process's current working directory, so the pipeline
    behaves the same whether run locally or from a container with a
    different working directory.
    """
    target = Path(target_path)
    if target.is_absolute():
        # Absolute paths are used as-is, nothing to anchor them to.
        return str(target)
    return str(Path(config_path).resolve().parent / target)


def get_table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    """Ask SQLite what columns the source table actually has.
    """
    cur = conn.execute(f"PRAGMA table_info({table})")
    # PRAGMA table_info returns one row per column: (cid, name, type, ...).
    # Column name is index 1.
    return [row[1] for row in cur.fetchall()]


def parse_bucket_expr(expr: str) -> tuple[str, list[int]]:
    """Parse a "bucket(age(<field>), [e0, e1, ...])" derive expression.
    """
    match = re.match(r"bucket\(age\((?P<field>\w+)\),\s*\[(?P<edges>[^\]]+)\]\)", expr)
    if not match:
        raise ValueError(f"Unsupported derive expression: {expr!r}")
    field = match.group("field")
    edges = [int(x.strip()) for x in match.group("edges").split(",")]
    if len(edges) < 2:
        raise ValueError("bucket() needs at least two edges")
    if any(a >= b for a, b in zip(edges, edges[1:])):
        # build_age_band_case_sql() assumes ascending edges (each WHEN
        # only needs to test the upper bound, relying on earlier WHENs
        # having already ruled out lower values). Out-of-order edges
        # would silently produce wrong band labels instead of an error.
        raise ValueError(f"bucket() edges must be strictly increasing, got {edges!r}")
    return field, edges


def build_age_band_case_sql(age_col: str, edges: list[int]) -> str:
    """Turn bucket edges like [0, 18, 40, 65, 120] into a SQL CASE
    expression producing labels '0-18', '18-40', '40-65', '65+'.

    The last edge is treated as open-ended ("65+" rather than "65-120"),
    matching the README's requirement that the oldest band has no upper
    bound. NULL input (age couldn't be computed, e.g. bad date of birth)
    passes through as NULL rather than falling into a band.
    """
    lines = ["CASE", f"WHEN {age_col} IS NULL THEN NULL"]
    for i in range(len(edges) - 1):
        lower, upper = edges[i], edges[i + 1]
        if i == len(edges) - 2:
            # Last band: no upper bound, e.g. "65+".
            lines.append(f"WHEN {age_col} >= {lower} THEN '{lower}+'")
        else:
            lines.append(f"WHEN {age_col} < {upper} THEN '{lower}-{upper}'")
    lines.append("END")
    return "\n        ".join(lines)


def build_query(config: dict, table_columns: list[str]):
    """Compile the config's transforms/validation into one SQL query.

    The query is built as five stacked CTEs, and the generated SQL reads the same way this
    function is written (rename -> filter -> derive -> band -> validate):

        renamed   -> apply `rename` + `cast` to every source column
        filtered  -> apply every `filter` expression from the config
        derived   -> compute age_years from the derive field, if present
        banded    -> turn age_years into the age_band label
        validated -> add one boolean flag per required-field / allowed-
                      values rule (missing_<field>, invalid_<field>), plus
                      the hardcoded date_of_birth-in-the-future check
                      (future_date_of_birth)

    Returns:
        full_sql:        the complete "WITH ... SELECT * FROM validated"
                          query text, ready to execute.
        params:           SQL bind parameters
        required_fields:  list of field names that must be non-empty,
                          straight from config["validation"]["required_fields"].
        cast_map:         {final_column_name: cast_type}, e.g.
                          {"date_of_birth": "date"}, used later by
                          classify_row() to phrase reject reasons.
        allowed_values:   {field_name: [allowed, values, ...]}, used later
                          by classify_row() too.
    """
    table = config["source"]["table"]
    transforms = config.get("transforms", [])

    # --- Step 1: read the transforms list into three lookup structures ---
    # config.yaml's `transforms` is normally a list of single-key dicts
    # (one per step), but nothing in YAML stops someone writing a step
    # with more than one key.
    rename_map: dict[str, str] = {}
    cast_map: dict[str, str] = {}
    filters: list[str] = []
    derive = None
    unknown_step_keys = []

    for step in transforms:
        recognised = set()
        if "rename" in step:
            rename_map.update(step["rename"])
            recognised.add("rename")
        if "cast" in step:
            cast_map.update(step["cast"])
            recognised.add("cast")
        if "filter" in step:
            filters.append(step["filter"])
            recognised.add("filter")
        if "derive" in step:
            derive = step["derive"]
            recognised.add("derive")
        unknown_step_keys.extend(k for k in step if k not in recognised)

    if unknown_step_keys:
        raise ValueError(
            f"config has unrecognised transform step key(s) {unknown_step_keys!r}; "
            "expected one or more of 'rename', 'cast', 'filter', 'derive' per step"
        )

    # Only "date" casts are actually implemented below. Without this
    # check, a config asking for e.g. `cast: {age: integer}` would be
    # silently ignored (no cast applied, no error), since the select-list
    # loop below only special-cases the string "date".
    supported_casts = {"date"}
    unsupported_casts = {t for t in cast_map.values() if t not in supported_casts}
    if unsupported_casts:
        raise ValueError(
            f"config requests unsupported cast type(s) {sorted(unsupported_casts)!r}; "
            f"only {sorted(supported_casts)!r} is implemented"
        )

    # --- CTE 1, "renamed": apply rename + cast to each source column ---
    # For every column that actually exists in the source table, decide
    # what to SELECT it as:
    #   - if it's cast to "date" (renamed or not): emit two columns, the
    #     SQLite-parsed date under the final name,
    #     plus the original raw text.
    #   - if it's renamed but not cast: `old_name AS new_name`.
    #   - otherwise: pass the column through unchanged.
    select_parts = []
    for col in table_columns:
        final_name = rename_map.get(col, col)
        if cast_map.get(final_name) == "date":
            select_parts.append(f"date({col}) AS {final_name}")
            select_parts.append(f"{col} AS {final_name}_raw")
        elif final_name != col:
            select_parts.append(f"{col} AS {final_name}")
        else:
            select_parts.append(col)
    renamed_sql = f"SELECT {', '.join(select_parts)} FROM {table}"

    # --- CTE 2, "filtered": apply config filter expressions ---
    # Each `filter` entry in config.yaml is a raw boolean SQL expression
    # (e.g. "status != 'cancelled'"). Multiple filter steps are ANDed
    # together. These are trusted, since they come from our own config
    # file, not from end-user input.
    filtered_sql = "SELECT * FROM renamed"
    if filters:
        filtered_sql += " WHERE " + " AND ".join(f"({expr})" for expr in filters)

    # --- CTE 3 + 4, "derived" / "banded": age in years, then age_band ---
    # Only runs if the config actually has a `derive` step. age_years is
    # computed with the strftime('%Y%m%d', ...) subtraction idiom.
    derived_sql = "SELECT * FROM filtered"
    banded_sql = "SELECT * FROM derived"
    if derive:
        age_field, edges = parse_bucket_expr(derive["expr"])
        derived_sql = (
            "SELECT *, "
            f"CASE WHEN {age_field} IS NULL THEN NULL ELSE "
            f"CAST((strftime('%Y%m%d','now') - strftime('%Y%m%d', {age_field})) / 10000 AS INTEGER) "
            "END AS age_years "
            "FROM filtered"
        )
        band_case = build_age_band_case_sql("age_years", edges)
        banded_sql = f"SELECT *, {band_case} AS {derive['field']} FROM derived"

    # --- CTE 5, "validated": one boolean flag per validation rule ---
    # For every required field, add a `missing_<field>` boolean column
    # (true if NULL or blank after trimming whitespace). For every
    # allowed-values rule, add an `invalid_<field>` boolean column (true
    # only if the field is present and not in the allowed set, so a missing value isn't double-counted).
    # The allowed values themselves are passed as bound parameters ("?").
    validation = config.get("validation", {})
    required_fields: list[str] = validation.get("required_fields", [])
    allowed_values: dict[str, list[str]] = validation.get("allowed_values", {})
    not_future: list[str] = ["date_of_birth"] if cast_map.get("date_of_birth") == "date" else []

    # This engine only ever implements "reject" (route bad rows to
    # encounters_rejected.csv rather than dropping or passing them
    # through). config.yaml's on_failure key is checked, not ignored.
    on_failure = validation.get("on_failure", "reject")
    if on_failure != "reject":
        raise ValueError(
            f"config requests on_failure={on_failure!r}; only 'reject' is implemented"
        )

    # Fail fast, with a clear message, if the config references a field
    # that doesn't actually exist after renaming (e.g. a typo). 
    known_fields = {rename_map.get(col, col) for col in table_columns}
    if derive:
        known_fields.add(derive["field"])
    unknown = [f for f in required_fields if f not in known_fields]
    unknown += [f for f in allowed_values if f not in known_fields]
    if unknown:
        raise ValueError(
            f"config references unknown field(s) {unknown!r}; "
            f"known fields after rename/derive are {sorted(known_fields)!r}"
        )

    flag_parts = []
    params: list[str] = []
    for field in required_fields:
        flag_parts.append(f"({field} IS NULL OR TRIM({field}) = '') AS missing_{field}")
    for field in not_future:
        # date('now') is today (UTC) at midnight; a date_of_birth of today
        # is valid (age 0), only a date strictly after today is rejected.
        flag_parts.append(f"({field} IS NOT NULL AND {field} > date('now')) AS future_{field}")
    for field, values in allowed_values.items():
        placeholders = ", ".join(["?"] * len(values))
        # Compare the TRIMMED value against the allowed set, not the raw
        # value, so e.g. " cardiology" (stray whitespace from an upstream
        # export) isn't wrongly flagged invalid just because it doesn't
        # byte-for-byte match "cardiology".
        flag_parts.append(
            f"({field} IS NOT NULL AND TRIM({field}) != '' "
            f"AND TRIM({field}) NOT IN ({placeholders})) AS invalid_{field}"
        )
        params.extend(values)

    validated_sql = "SELECT *"
    if flag_parts:
        validated_sql += ", " + ", ".join(flag_parts)
    validated_sql += " FROM banded"

    full_sql = f"""
WITH renamed AS (
    {renamed_sql}
),
filtered AS (
    {filtered_sql}
),
derived AS (
    {derived_sql}
),
banded AS (
    {banded_sql}
),
validated AS (
    {validated_sql}
)
SELECT * FROM validated
"""
    return full_sql, params, required_fields, cast_map, allowed_values, not_future


def classify_row(row: sqlite3.Row, required_fields, cast_map, allowed_values, not_future):
    """Turn one row's missing_/invalid_/future_ SQL flags into reject reasons.

    Returns a list of (reason_label, reason_detail) tuples:
      - reason_label:  a short, stable machine key (e.g.
                        "missing_or_invalid_date_of_birth"), used to tally
                        counts in the run summary.
      - reason_detail: a human-readable sentence for the rejected-rows
                        CSV (e.g. "invalid department: got 'xyz'").

    An empty list means the row is clean, this is the single switch that
    decides whether a row goes to encounters_clean.csv or
    encounters_rejected.csv in run_pipeline().
    """
    reasons = []
    for field in required_fields:
        if row[f"missing_{field}"]:
            if cast_map.get(field) == "date":
                # A field that's cast to "date" is flagged missing by SQL
                # both when it's genuinely blank AND when date() failed to
                # parse it.
                label = f"missing_or_invalid_{field}"
                detail = f"missing or invalid {field}"
            else:
                label = f"missing_{field}"
                detail = f"missing {field}"
            reasons.append((label, detail))
    for field in allowed_values:
        if row[f"invalid_{field}"]:
            label = f"invalid_{field}"
            # Include the actual bad value here (but NOT in reason_label)
            # so the aggregate reason counts in the summary stay small and
            # stable, while the per-row CSV still shows exactly what was
            # wrong.
            detail = f"invalid {field}: got '{row[field]}'"
            reasons.append((label, detail))
    for field in not_future:
        if row[f"future_{field}"]:
            label = f"future_{field}"
            detail = f"{field} is in the future: got '{row[field]}'"
            reasons.append((label, detail))
    return reasons


def cast_output_value(col: str, value, cast_map: dict[str, str]):
    """Convert a column's SQLite text value into a real Python type for
    the in-memory row, per its cast_map entry (currently just "date").
    """
    if value is not None and cast_map.get(col) == "date":
        return date.fromisoformat(value)
    return value


def run_pipeline(config_path: str) -> None:
    """Orchestrate one full run: load config, query the DB, split rows into
    clean/rejected, and write the three output files.

    See the module docstring for the call sequence.
    """
    # Step A: load config, resolve the source DB path relative to the
    # config file itself (see resolve_relative_to_config's docstring).
    config = load_config(config_path)
    source_cfg = config["source"]

    # The query engine below (get_table_columns' PRAGMA table_info, every
    # CTE build_query() generates) is SQLite-specific, so only
    # source.format: sqlite is supported.
    source_format = source_cfg.get("format", "sqlite")
    if source_format != "sqlite":
        raise ValueError(
            f"config requests source.format={source_format!r}; only 'sqlite' is implemented"
        )

    db_path = resolve_relative_to_config(config_path, source_cfg["path"])
    table = source_cfg["table"]

    # Step B: open the DB, discover its columns, compile and run the
    # config-driven query. 
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        table_columns = get_table_columns(conn, table)
        total_input_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        sql, params, required_fields, cast_map, allowed_values, not_future = build_query(config, table_columns)

        # NOTE: fetchall() pulls every remaining row into memory at once.
        # That's fine for this sample data, but is the first thing that
        # would need to change for a millions-of-rows source: switch to
        # streaming from the cursor in batches, paged by keyset
        # (WHERE id > :last_id) rather than OFFSET, which gets slower as
        # it grows.
        rows = conn.execute(sql, params).fetchall()
        # candidate_rows is just len(rows): the `validated` CTE never drops
        # rows, only `filtered` does.
        candidate_rows = len(rows)
        dropped_by_filter = total_input_rows - candidate_rows
    finally:
        conn.close()

    # Step C: work out the final output column list, e.g.
    # ["encounter_id", "patient_id", "date_of_birth", "status",
    #  "encounter_type", "department", "age_band"], via the rename map plus the derived field.
    rename_map = {}
    for step in config.get("transforms", []):
        if "rename" in step:
            rename_map.update(step["rename"])
    derive = next((s["derive"] for s in config.get("transforms", []) if "derive" in s), None)

    output_columns = [rename_map.get(c, c) for c in table_columns]
    if derive:
        output_columns.append(derive["field"])

    # Step D: classify every row and split it into clean_rows or
    # rejected_rows. This is the only place classify_row() is called.
    clean_rows = []
    rejected_rows = []
    reason_counter: Counter = Counter()  # tallies reason_label -> count, for the summary

    for row in rows:
        reasons = classify_row(row, required_fields, cast_map, allowed_values, not_future)
        if not reasons:
            # Clean row: only the final output columns are kept, with
            # cast_map fields (e.g. date_of_birth) converted to real
            # Python types via cast_output_value().
            clean_rows.append({col: cast_output_value(col, row[col], cast_map) for col in output_columns})
        else:
            # Rejected row: keep the output columns (even if some are
            # NULL, e.g. date_of_birth failed to parse) PLUS any "_raw"
            # column build_query() added.
            record = {col: cast_output_value(col, row[col], cast_map) for col in output_columns}
            for raw_col in row.keys():
                if raw_col.endswith("_raw") and raw_col not in record:
                    record[raw_col] = row[raw_col]
            record["reject_reasons"] = "; ".join(detail for _, detail in reasons)
            rejected_rows.append(record)
            reason_counter.update(label for label, _ in reasons)

    # Step E: resolve output paths (same config-relative logic as the
    # source DB path) and write all three output files.
    output_cfg = config["output"]

    # The clean/rejected files can be CSV or JSON.
    output_format = output_cfg.get("format", "csv")
    supported_output_formats = {"csv", "json"}
    if output_format not in supported_output_formats:
        raise ValueError(
            f"config requests output.format={output_format!r}; "
            f"only {sorted(supported_output_formats)!r} is implemented"
        )

    output_path = resolve_relative_to_config(config_path, output_cfg["path"])
    rejects_path = resolve_relative_to_config(config_path, output_cfg["rejects_path"])
    summary_path = resolve_relative_to_config(config_path, output_cfg["summary_path"])

    if output_format == "csv":
        write_csv(output_path, output_columns, clean_rows)
        rejected_columns = output_columns + [
            c for c in (rejected_rows[0].keys() if rejected_rows else [])
            if c not in output_columns and c != "reject_reasons"
        ] + ["reject_reasons"]
        write_csv(rejects_path, rejected_columns, rejected_rows)
    else:
        write_json(output_path, clean_rows)
        write_json(rejects_path, rejected_rows)

    # Step F: write the run summary, counts plus a per-reason breakdown
    # of the rejected rows.
    summary = {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "source": {"path": db_path, "table": table},
        "counts": {
            "total_input_rows": total_input_rows,
            "dropped_by_filter": dropped_by_filter,
            "candidate_rows": candidate_rows,
            "clean_rows": len(clean_rows),
            "rejected_rows": len(rejected_rows),
        },
        "rejected_by_reason": dict(reason_counter),
    }
    write_json(summary_path, summary)

    # Step G: one-line recap printed to stdout.
    print(
        f"Processed {total_input_rows} rows: "
        f"{dropped_by_filter} dropped by filter, "
        f"{len(clean_rows)} clean, {len(rejected_rows)} rejected."
    )


def write_csv(path: str, fieldnames: list[str], rows: list[dict]) -> None:
    """Write a list of dicts to a CSV file, creating parent directories
    (e.g. output/) if they don't already exist."""
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: str, data) -> None:
    """Write a dict or list to a pretty-printed JSON file, creating
    parent directories if needed.
    """
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


def main() -> None:
    """CLI entry point: `python3 pipeline.py --config path/to/config.yaml`.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.yaml", help="Path to the pipeline config YAML")
    args = parser.parse_args()
    run_pipeline(args.config)


if __name__ == "__main__":
    main()
