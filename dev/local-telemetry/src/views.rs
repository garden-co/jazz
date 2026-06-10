use anyhow::Result;
use duckdb::Connection;
use std::path::{Path, PathBuf};

pub fn refresh_views(conn: &Connection, data_dir: &Path) -> Result<()> {
    create_signal_views(conn, data_dir, "spans", "raw_traces", "resource_spans")?;
    create_signal_views(conn, data_dir, "logs", "raw_logs", "resource_logs")?;
    create_signal_views(conn, data_dir, "metrics", "raw_metrics", "resource_metrics")?;

    conn.execute_batch(SPANS_VIEW_SQL)?;
    conn.execute_batch(LOGS_VIEW_SQL)?;
    conn.execute_batch(NUMBER_POINTS_VIEW_SQL)?;

    Ok(())
}

fn create_signal_views(
    conn: &Connection,
    data_dir: &Path,
    dir_name: &str,
    raw_view: &str,
    fallback_view: &str,
) -> Result<()> {
    let glob = json_glob(data_dir, dir_name);
    if glob_has_matches(&glob) {
        let escaped = glob.to_string_lossy().replace('\'', "''");
        conn.execute_batch(&format!(
            "CREATE OR REPLACE VIEW {raw_view} AS \
             SELECT to_json(json)::JSON AS doc \
             FROM read_json('{escaped}', format='array', records=false, maximum_object_size=104857600)"
        ))?;
    } else {
        conn.execute_batch(&format!(
            "CREATE OR REPLACE VIEW {raw_view} AS SELECT NULL::JSON AS doc WHERE false"
        ))?;
    }
    conn.execute_batch(&format!(
        "CREATE OR REPLACE VIEW {fallback_view} AS SELECT doc FROM {raw_view}"
    ))?;
    Ok(())
}

fn json_glob(data_dir: &Path, dir_name: &str) -> PathBuf {
    data_dir.join(dir_name).join("*.json")
}

fn glob_has_matches(glob: &Path) -> bool {
    let Some(parent) = glob.parent() else {
        return false;
    };
    let Ok(entries) = std::fs::read_dir(parent) else {
        return false;
    };
    entries.filter_map(Result::ok).any(|entry| {
        entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "json")
    })
}

const SPANS_VIEW_SQL: &str = r#"
CREATE OR REPLACE VIEW spans AS
WITH rs AS (
    SELECT doc AS rs FROM raw_traces
),
ss AS (
    SELECT rs, UNNEST(json_extract(rs, '$.scopeSpans')::JSON[]) AS ss FROM rs
),
sp AS (
    SELECT rs, ss, UNNEST(json_extract(ss, '$.spans')::JSON[]) AS span FROM ss
)
SELECT
    json_extract_string(span, '$.traceId') AS trace_id,
    json_extract_string(span, '$.spanId') AS span_id,
    json_extract_string(span, '$.parentSpanId') AS parent_span_id,
    json_extract_string(span, '$.name') AS name,
    TRY_CAST(json_extract(span, '$.kind') AS INTEGER) AS kind,
    TRY_CAST(json_extract_string(span, '$.startTimeUnixNano') AS UBIGINT) AS start_time_unix_nano,
    TRY_CAST(json_extract_string(span, '$.endTimeUnixNano') AS UBIGINT) AS end_time_unix_nano,
    TRY_CAST(json_extract_string(span, '$.endTimeUnixNano') AS UBIGINT) -
        TRY_CAST(json_extract_string(span, '$.startTimeUnixNano') AS UBIGINT) AS duration_ns,
    (
        SELECT json_extract_string(a, '$.value.stringValue')
        FROM UNNEST(json_extract(rs, '$.resource.attributes')::JSON[]) AS u(a)
        WHERE json_extract_string(a, '$.key') = 'service.name'
        LIMIT 1
    ) AS service_name,
    json_extract_string(ss, '$.scope.name') AS scope_name,
    TRY_CAST(json_extract(span, '$.status.code') AS INTEGER) AS status_code,
    json_extract(span, '$.attributes') AS attributes,
    json_extract(span, '$.events') AS events,
    span AS raw_span
FROM sp;
"#;

const LOGS_VIEW_SQL: &str = r#"
CREATE OR REPLACE VIEW logs AS
WITH rl AS (
    SELECT doc AS rl FROM raw_logs
),
sl AS (
    SELECT rl, UNNEST(json_extract(rl, '$.scopeLogs')::JSON[]) AS sl FROM rl
),
rec AS (
    SELECT rl, sl, UNNEST(json_extract(sl, '$.logRecords')::JSON[]) AS rec FROM sl
)
SELECT
    TRY_CAST(json_extract_string(rec, '$.timeUnixNano') AS UBIGINT) AS time_unix_nano,
    TRY_CAST(json_extract_string(rec, '$.observedTimeUnixNano') AS UBIGINT) AS observed_time_unix_nano,
    TRY_CAST(json_extract(rec, '$.severityNumber') AS INTEGER) AS severity_number,
    json_extract_string(rec, '$.severityText') AS severity_text,
    json_extract(rec, '$.body') AS body,
    json_extract_string(rec, '$.traceId') AS trace_id,
    json_extract_string(rec, '$.spanId') AS span_id,
    (
        SELECT json_extract_string(a, '$.value.stringValue')
        FROM UNNEST(json_extract(rl, '$.resource.attributes')::JSON[]) AS u(a)
        WHERE json_extract_string(a, '$.key') = 'service.name'
        LIMIT 1
    ) AS service_name,
    json_extract_string(sl, '$.scope.name') AS scope_name,
    json_extract(rec, '$.attributes') AS attributes,
    rec AS raw_record
FROM rec;
"#;

const NUMBER_POINTS_VIEW_SQL: &str = r#"
CREATE OR REPLACE VIEW number_points AS
WITH rm AS (
    SELECT doc AS rm FROM raw_metrics
),
sm AS (
    SELECT rm, UNNEST(json_extract(rm, '$.scopeMetrics')::JSON[]) AS sm FROM rm
),
m AS (
    SELECT rm, sm, UNNEST(json_extract(sm, '$.metrics')::JSON[]) AS metric FROM sm
),
classified AS (
    SELECT
        rm, sm, metric,
        CASE
            WHEN json_type(metric, '$.gauge') = 'OBJECT' THEN 'gauge'
            WHEN json_type(metric, '$.sum')   = 'OBJECT' THEN 'sum'
            ELSE NULL
        END AS kind
    FROM m
),
typed AS (
    SELECT
        rm, sm, metric, kind,
        CASE kind
            WHEN 'gauge' THEN json_extract(metric, '$.gauge.dataPoints')
            WHEN 'sum'   THEN json_extract(metric, '$.sum.dataPoints')
        END AS dps
    FROM classified
    WHERE kind IS NOT NULL
),
points AS (
    SELECT rm, sm, metric, kind, UNNEST(dps::JSON[]) AS dp FROM typed
)
SELECT
    json_extract_string(metric, '$.name') AS name,
    json_extract_string(metric, '$.description') AS description,
    json_extract_string(metric, '$.unit') AS unit,
    (
        SELECT json_extract_string(a, '$.value.stringValue')
        FROM UNNEST(json_extract(rm, '$.resource.attributes')::JSON[]) AS u(a)
        WHERE json_extract_string(a, '$.key') = 'service.name'
        LIMIT 1
    ) AS service_name,
    json_extract_string(sm, '$.scope.name') AS scope_name,
    kind,
    TRY_CAST(json_extract_string(dp, '$.timeUnixNano') AS UBIGINT) AS time_unix_nano,
    COALESCE(
        TRY_CAST(json_extract_string(dp, '$.asDouble') AS DOUBLE),
        TRY_CAST(json_extract_string(dp, '$.asInt') AS DOUBLE)
    ) AS value,
    json_extract(dp, '$.attributes') AS attributes
FROM points;
"#;
