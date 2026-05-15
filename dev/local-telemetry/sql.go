package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

type sqlRequest struct {
	Query string `json:"query"`
}

type sqlResponse struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

type sqlError struct {
	Error string `json:"error"`
}

func runHTTPServer(ctx context.Context, host string, port int, dataDir string, bundle *uiBundle) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/sql", func(w http.ResponseWriter, r *http.Request) {
		handleSQL(w, r, db, dataDir)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	registerUIHandlers(mux, bundle)

	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", host, port),
		Handler: corsAny(mux),
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("viewer:  http://%s:%d/", host, port)
		log.Printf("sql:     http://%s:%d/sql", host, port)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		<-errCh
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func corsAny(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func handleSQL(w http.ResponseWriter, r *http.Request, db *sql.DB, dataDir string) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "use POST")
		return
	}
	var req sqlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	if req.Query == "" {
		writeErr(w, http.StatusBadRequest, "missing query")
		return
	}

	refreshViews(r.Context(), db, dataDir)

	rows, err := db.QueryContext(r.Context(), req.Query)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := sqlResponse{Columns: cols, Rows: [][]any{}}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				vals[i] = string(b)
			}
		}
		out.Rows = append(out.Rows, vals)
	}
	if err := rows.Err(); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(sqlError{Error: msg})
}

// refreshViews (re)creates raw_* and flattened views for each signal that has
// at least one file on disk. Cheap to call per-request; tolerates missing data.
//
// Views use JSON extraction rather than typed structs so they don't depend on
// auto-inferred schemas — fields missing from sampled records become NULL
// instead of binder errors.
func refreshViews(ctx context.Context, db *sql.DB, dataDir string) {
	for _, s := range []string{"traces", "logs", "metrics"} {
		matches, _ := filepath.Glob(filepath.Join(dataDir, s+"*.jsonl"))
		if len(matches) == 0 {
			continue
		}
		glob := filepath.Join(dataDir, s+"*.jsonl")
		// raw_<signal> = one row per OTLP ExportRequest, doc is JSON.
		raw := fmt.Sprintf(
			"CREATE OR REPLACE VIEW raw_%s AS SELECT json AS doc FROM read_json('%s', format='newline_delimited', records=false, maximum_object_size=104857600)",
			s, glob,
		)
		if _, err := db.ExecContext(ctx, raw); err != nil {
			log.Printf("raw_%s view: %v", s, err)
			continue
		}
		if stmt, ok := flatViewSQL[s]; ok {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				log.Printf("flat %s view: %v", s, err)
			}
		}
	}
}

var flatViewSQL = map[string]string{
	"traces":  spansViewSQL,
	"logs":    logsViewSQL,
	"metrics": numberPointsViewSQL,
}

const spansViewSQL = `
CREATE OR REPLACE VIEW spans AS
WITH rs AS (
    SELECT UNNEST(json_extract(doc, '$.resourceSpans')::JSON[]) AS rs FROM raw_traces
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
`

const logsViewSQL = `
CREATE OR REPLACE VIEW logs AS
WITH rl AS (
    SELECT UNNEST(json_extract(doc, '$.resourceLogs')::JSON[]) AS rl FROM raw_logs
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
`

// number_points covers Gauge + Sum metric data points (the common cases).
// Histograms and exponential histograms can be queried from raw_metrics.
//
// UNNEST(::JSON[]) schema-unifies array elements, so a metric that only sets
// `sum` gets a JSON null `gauge` field after the cast. json_type() = 'OBJECT'
// distinguishes a real payload from the unified null.
const numberPointsViewSQL = `
CREATE OR REPLACE VIEW number_points AS
WITH rm AS (
    SELECT UNNEST(json_extract(doc, '$.resourceMetrics')::JSON[]) AS rm FROM raw_metrics
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
`
