import { useMemo, useState } from "react";
import type React from "react";
import { useQuery } from "@tanstack/react-query";
import { runQuery } from "./api.js";
import { buildTraceListSql } from "./traceQueries.js";
import { traceRowPayloadLabel } from "./traceRows.js";

type TraceSummary = {
  TraceId: string;
  start: string;
  duration_ms: number;
  span_count: number;
  root_span: string;
  root_service: string;
  services: string[];
  has_error: number;
};

type Span = {
  TraceId: string;
  SpanId: string;
  ParentSpanId: string;
  Timestamp: string; // ISO/ClickHouse-format start
  Duration: number; // nanoseconds
  SpanName: string;
  ServiceName: string;
  StatusCode: string;
  attrs: Record<string, string>;
  thread: string;
};

const COLORS = ["#4c8df6", "#3aa57a", "#e07b00", "#a766c8", "#cc4f4f", "#22a3a3", "#7f7f7f"];

function colorForKey(key: string): string {
  let hash = 0;
  for (let i = 0; i < key.length; i++) hash = (hash * 31 + key.charCodeAt(i)) | 0;
  return COLORS[Math.abs(hash) % COLORS.length];
}

function formatTime(s: string): string {
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  const ms = String(d.getMilliseconds()).padStart(3, "0");
  return `${hh}:${mm}:${ss}.${ms}`;
}

function formatDuration(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}µs`;
  if (ms < 1000) return `${ms.toFixed(1)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

// ---------------------------------------------------------------------------
// Trace list
// ---------------------------------------------------------------------------

export function TraceList(props: { minutes: number; onSelect: (id: string) => void }) {
  const { minutes, onSelect } = props;
  const [serviceFilter, setServiceFilter] = useState<string>("");
  const [opFilter, setOpFilter] = useState<string>("");
  const [traceIdFilter, setTraceIdFilter] = useState<string>("");

  const { data, error } = useQuery({
    queryKey: ["traces", minutes, serviceFilter, opFilter, traceIdFilter],
    queryFn: () => fetchTraces(minutes, serviceFilter, opFilter, traceIdFilter),
  });
  const traces = data ?? [];

  return (
    <div>
      <section style={styles.controls}>
        <Field label="Service">
          <select
            value={serviceFilter}
            onChange={(e) => setServiceFilter(e.target.value)}
            style={styles.input}
          >
            <option value="">any</option>
            <option value="jazz-browser">jazz-browser</option>
            <option value="jazz-dev-server">jazz-dev-server</option>
            <option value="jazz-server">jazz-server</option>
          </select>
        </Field>
        <Field label="Operation contains">
          <input
            type="text"
            value={opFilter}
            placeholder="e.g. sync.send"
            onChange={(e) => setOpFilter(e.target.value)}
            style={{ ...styles.input, width: 200 }}
          />
        </Field>
        <Field label="Trace ID contains">
          <input
            type="text"
            value={traceIdFilter}
            placeholder="full or partial id"
            onChange={(e) => setTraceIdFilter(e.target.value)}
            style={{ ...styles.input, width: 220 }}
          />
        </Field>
        <span style={styles.summaryNote}>
          {traces.length} trace{traces.length === 1 ? "" : "s"}
        </span>
      </section>

      {error && <div style={styles.error}>error: {String(error.message ?? error)}</div>}

      <table style={styles.table}>
        <thead>
          <tr>
            <th style={styles.th}>started</th>
            <th style={styles.th}>service · operation</th>
            <th style={styles.th}>trace id</th>
            <th style={{ ...styles.th, textAlign: "right" }}>duration</th>
            <th style={{ ...styles.th, textAlign: "right" }}>spans</th>
            <th style={styles.th}>services</th>
          </tr>
        </thead>
        <tbody>
          {traces.map((t) => (
            <tr
              key={t.TraceId}
              onClick={() => onSelect(t.TraceId)}
              style={{ ...styles.tr, cursor: "pointer" }}
            >
              <td style={styles.td}>{formatTime(t.start)}</td>
              <td style={styles.td}>
                <span
                  style={{
                    ...styles.serviceTag,
                    background: colorForKey(t.root_service),
                  }}
                >
                  {t.root_service || "?"}
                </span>{" "}
                <strong>{t.root_span}</strong>
              </td>
              <td style={styles.td}>
                <code style={styles.code} title={t.TraceId}>
                  {t.TraceId.slice(0, 16)}…
                </code>
              </td>
              <td style={{ ...styles.td, textAlign: "right" }}>{formatDuration(t.duration_ms)}</td>
              <td style={{ ...styles.td, textAlign: "right" }}>{t.span_count}</td>
              <td style={styles.td}>
                {(t.services ?? []).map((s) => (
                  <span
                    key={s}
                    style={{ ...styles.serviceChip, background: colorForKey(s) }}
                    title={s}
                  >
                    {s.replace(/^jazz-/, "")}
                  </span>
                ))}
                {t.has_error > 0 && <span style={styles.errorChip}>error</span>}
              </td>
            </tr>
          ))}
          {traces.length === 0 && !error && (
            <tr>
              <td colSpan={6} style={styles.empty}>
                No traces in the last {minutes} minute(s).
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

async function fetchTraces(
  minutes: number,
  serviceFilter: string,
  opFilter: string,
  traceIdFilter: string,
): Promise<TraceSummary[]> {
  const sql = buildTraceListSql({ minutes, serviceFilter, opFilter, traceIdFilter });
  const rows = await runQuery<any>(sql);
  return rows.map((r) => ({
    TraceId: r.TraceId,
    start: r.start,
    duration_ms: Number(r.duration_ms),
    span_count: Number(r.span_count),
    root_span: r.root_span_explicit || r.root_span_fallback || "(unknown)",
    root_service: r.root_service_explicit || r.root_service_fallback || "",
    services: r.services ?? [],
    has_error: Number(r.has_error ?? 0),
  }));
}

// ---------------------------------------------------------------------------
// Trace detail (waterfall)
// ---------------------------------------------------------------------------

export function TraceDetail(props: { traceId: string; minutes: number; onBack: () => void }) {
  const { traceId, minutes, onBack } = props;
  const [selectedSpanId, setSelectedSpanId] = useState<string | null>(null);

  const { data, error } = useQuery({
    queryKey: ["trace", traceId, minutes],
    queryFn: () => fetchTraceSpans(traceId, minutes),
    // Single trace is immutable once spans have landed; no need to poll.
    refetchInterval: false,
    staleTime: Infinity,
  });
  const spans = data ?? [];

  const tree = useMemo(() => buildTree(spans), [spans]);

  const selectedSpan = useMemo(
    () => spans.find((s) => s.SpanId === selectedSpanId) ?? null,
    [spans, selectedSpanId],
  );

  if (error) {
    return (
      <div>
        <BackButton onClick={onBack} />
        <div style={styles.error}>error: {String(error.message ?? error)}</div>
      </div>
    );
  }

  if (spans.length === 0) {
    return (
      <div>
        <BackButton onClick={onBack} />
        <div style={styles.empty}>loading trace…</div>
      </div>
    );
  }

  const startNs = Math.min(...spans.map(spanStartNs));
  const endNs = Math.max(...spans.map((s) => spanStartNs(s) + s.Duration));
  const totalDurationNs = Math.max(1, endNs - startNs);
  const traceStartIso = spans.slice().sort((a, b) => spanStartNs(a) - spanStartNs(b))[0].Timestamp;

  return (
    <div>
      <BackButton onClick={onBack} />
      <div style={styles.traceHeader}>
        <div>
          <strong>Trace</strong> <code style={styles.code}>{traceId.slice(0, 16)}…</code>
        </div>
        <div>
          <strong>Started</strong> {formatTime(traceStartIso)}
        </div>
        <div>
          <strong>Duration</strong> {formatDuration(totalDurationNs / 1e6)}
        </div>
        <div>
          <strong>Spans</strong> {spans.length}
        </div>
      </div>

      <div style={styles.waterfall}>
        <div style={styles.waterfallHeader}>
          <div style={styles.spanInfoCol}>service · operation</div>
          <div style={styles.spanBarCol}>
            <TickRuler totalMs={totalDurationNs / 1e6} />
          </div>
        </div>
        <div>
          {tree.map((node) =>
            renderSpanNode(node, 0, startNs, totalDurationNs, selectedSpanId, setSelectedSpanId),
          )}
        </div>
      </div>

      {selectedSpan && (
        <SpanDetailPanel
          span={selectedSpan}
          traceStartNs={startNs}
          onClose={() => setSelectedSpanId(null)}
        />
      )}
    </div>
  );
}

function BackButton(props: { onClick: () => void }) {
  return (
    <button type="button" onClick={props.onClick} style={styles.backButton}>
      ← Back to traces
    </button>
  );
}

type Node = { span: Span; children: Node[] };

function buildTree(spans: Span[]): Node[] {
  const byId = new Map<string, Span>();
  for (const span of spans) byId.set(span.SpanId, span);

  const childrenByParent = new Map<string, Span[]>();
  const roots: Span[] = [];
  for (const span of spans) {
    const parent = span.ParentSpanId;
    if (!parent || !byId.has(parent)) {
      roots.push(span);
    } else {
      if (!childrenByParent.has(parent)) childrenByParent.set(parent, []);
      childrenByParent.get(parent)!.push(span);
    }
  }
  const sortByStart = (a: Span, b: Span) => spanStartNs(a) - spanStartNs(b);
  roots.sort(sortByStart);

  const toNode = (span: Span): Node => ({
    span,
    children: (childrenByParent.get(span.SpanId) ?? []).sort(sortByStart).map(toNode),
  });
  return roots.map(toNode);
}

function renderSpanNode(
  node: Node,
  depth: number,
  startNs: number,
  totalDurationNs: number,
  selectedSpanId: string | null,
  onSelect: (id: string) => void,
): React.ReactNode {
  const span = node.span;
  return (
    <div key={span.SpanId}>
      <SpanRow
        span={span}
        depth={depth}
        startNs={startNs}
        totalDurationNs={totalDurationNs}
        selected={selectedSpanId === span.SpanId}
        onClick={() => onSelect(span.SpanId)}
      />
      {node.children.map((child) =>
        renderSpanNode(child, depth + 1, startNs, totalDurationNs, selectedSpanId, onSelect),
      )}
    </div>
  );
}

function SpanRow(props: {
  span: Span;
  depth: number;
  startNs: number;
  totalDurationNs: number;
  selected: boolean;
  onClick: () => void;
}) {
  const { span, depth, startNs, totalDurationNs, selected, onClick } = props;
  const offsetPct = ((spanStartNs(span) - startNs) / totalDurationNs) * 100;
  const widthPct = Math.max(0.1, (span.Duration / totalDurationNs) * 100);
  const color = colorForKey(span.ServiceName);
  const labelService = span.ServiceName.replace(/^jazz-/, "");
  const errored = span.StatusCode === "STATUS_CODE_ERROR";
  const payloadLabel = traceRowPayloadLabel(span.attrs);
  return (
    <div
      onClick={onClick}
      style={{
        ...styles.spanRow,
        background: selected ? "#eef5ff" : undefined,
      }}
    >
      <div
        style={{
          ...styles.spanInfoCol,
          paddingLeft: 8 + depth * 14,
        }}
      >
        <span style={{ ...styles.serviceDot, background: color }} />
        <span style={styles.serviceName}>{labelService}</span>
        <span style={styles.spanName}>{span.SpanName}</span>
        {payloadLabel && (
          <span style={styles.payloadTag} title={`payload: ${payloadLabel}`}>
            {payloadLabel}
          </span>
        )}
        {span.thread && <span style={styles.threadTag}>{span.thread}</span>}
        {errored && <span style={styles.errorChip}>err</span>}
      </div>
      <div style={styles.spanBarCol}>
        <div style={styles.barTrack}>
          <div
            style={{
              ...styles.bar,
              left: offsetPct + "%",
              width: widthPct + "%",
              background: color,
            }}
            title={formatDuration(span.Duration / 1e6)}
          />
          <div
            style={{
              ...styles.barLabel,
              left: `calc(${offsetPct + widthPct}% + 4px)`,
            }}
          >
            {formatDuration(span.Duration / 1e6)}
          </div>
        </div>
      </div>
    </div>
  );
}

function TickRuler(props: { totalMs: number }) {
  const { totalMs } = props;
  const ticks = [0, 0.25, 0.5, 0.75, 1];
  return (
    <div style={styles.ruler}>
      {ticks.map((t) => (
        <div key={t} style={{ ...styles.rulerTick, left: t * 100 + "%" }}>
          <span style={styles.rulerLabel}>{formatDuration(totalMs * t)}</span>
        </div>
      ))}
    </div>
  );
}

function SpanDetailPanel(props: { span: Span; traceStartNs: number; onClose: () => void }) {
  const { span, traceStartNs, onClose } = props;
  const offsetMs = (spanStartNs(span) - traceStartNs) / 1e6;
  const durationMs = span.Duration / 1e6;
  const payloadJson = span.attrs.payload_json;
  const otherAttrs = Object.entries(span.attrs).filter(
    ([k]) => k !== "payload_json" && k !== "jazz.span.fields",
  );
  return (
    <div style={styles.panel}>
      <div style={styles.panelHeader}>
        <strong>{span.SpanName}</strong>
        <span style={styles.panelMuted}> · {span.ServiceName}</span>
        <button type="button" onClick={onClose} style={styles.panelClose}>
          ✕
        </button>
      </div>
      <div style={styles.panelGrid}>
        <Stat label="offset">{formatDuration(offsetMs)}</Stat>
        <Stat label="duration">{formatDuration(durationMs)}</Stat>
        <Stat label="status">{span.StatusCode || "—"}</Stat>
        <Stat label="span id">
          <code style={styles.code}>{span.SpanId.slice(0, 16)}…</code>
        </Stat>
      </div>
      {otherAttrs.length > 0 && (
        <table style={styles.attrTable}>
          <tbody>
            {otherAttrs
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([k, v]) => (
                <tr key={k}>
                  <td style={styles.attrKey}>{k}</td>
                  <td style={styles.attrValue}>{v}</td>
                </tr>
              ))}
          </tbody>
        </table>
      )}
      {payloadJson && (
        <details style={styles.payloadDetails} open>
          <summary style={styles.payloadSummary}>payload_json</summary>
          <pre style={styles.payloadPre}>{prettyJson(payloadJson)}</pre>
        </details>
      )}
    </div>
  );
}

function Stat(props: { label: string; children: React.ReactNode }) {
  return (
    <div style={styles.stat}>
      <div style={styles.statLabel}>{props.label}</div>
      <div style={styles.statValue}>{props.children}</div>
    </div>
  );
}

function Field(props: { label: string; children: React.ReactNode }) {
  return (
    <label style={styles.field}>
      <span style={styles.fieldLabel}>{props.label}</span>
      {props.children}
    </label>
  );
}

function prettyJson(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
  }
}

function spanStartNs(span: Span): number {
  // Timestamp from ClickHouse comes as ISO-ish "YYYY-MM-DD HH:MM:SS.NNNNNNNNN" or
  // ISO 8601. Date.parse only goes to ms — extend with extra precision when present.
  const t = span.Timestamp;
  const baseMs = Date.parse(t.replace(" ", "T")) || 0;
  let extraNs = 0;
  const dot = t.indexOf(".");
  if (dot !== -1) {
    const tail = t.slice(dot + 1).match(/^(\d{1,9})/)?.[1] ?? "";
    const padded = tail.padEnd(9, "0").slice(0, 9);
    const totalNanos = Number(padded);
    extraNs = totalNanos - Math.floor(totalNanos / 1e6) * 1e6;
  }
  return baseMs * 1e6 + extraNs;
}

async function fetchTraceSpans(traceId: string, minutes: number): Promise<Span[]> {
  // Time window helps ClickHouse use the partition; spans within a single trace
  // shouldn't span more than the active window anyway.
  const sql = `
    SELECT
      TraceId,
      SpanId,
      ParentSpanId,
      toString(Timestamp) AS ts_str,
      Duration,
      SpanName,
      ServiceName,
      StatusCode,
      SpanAttributes AS attrs,
      SpanAttributes['jazz.runtime_thread'] AS thread
    FROM otel_traces
    WHERE TraceId = '${traceId.replace(/'/g, "")}'
      AND Timestamp > now() - INTERVAL ${Math.max(minutes, 30)} MINUTE
    ORDER BY Timestamp ASC
    LIMIT 5000
  `;
  const rows = await runQuery<any>(sql);
  return rows.map((r) => ({
    TraceId: r.TraceId,
    SpanId: r.SpanId,
    ParentSpanId: r.ParentSpanId ?? "",
    Timestamp: r.ts_str,
    Duration: Number(r.Duration),
    SpanName: r.SpanName,
    ServiceName: r.ServiceName,
    StatusCode: r.StatusCode ?? "",
    attrs: (r.attrs as Record<string, string>) ?? {},
    thread: r.thread ?? "",
  }));
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles: Record<string, React.CSSProperties> = {
  controls: {
    display: "flex",
    gap: 12,
    flexWrap: "wrap",
    margin: "0 0 12px",
    padding: 10,
    background: "#fafafa",
    border: "1px solid #eee",
    borderRadius: 6,
    alignItems: "flex-end",
  },
  field: { display: "flex", flexDirection: "column", gap: 2 },
  fieldLabel: { fontSize: 11, color: "#666", textTransform: "uppercase" },
  input: {
    padding: "4px 8px",
    border: "1px solid #ccc",
    borderRadius: 4,
    fontSize: 14,
    minWidth: 120,
  },
  summaryNote: { fontSize: 12, color: "#666", marginLeft: "auto" },
  table: {
    width: "100%",
    borderCollapse: "collapse",
    fontFamily: "ui-monospace, SFMono-Regular, monospace",
    fontSize: 12,
  },
  th: {
    textAlign: "left",
    padding: "8px",
    borderBottom: "2px solid #ddd",
    fontSize: 11,
    color: "#555",
    textTransform: "uppercase",
  },
  tr: { borderBottom: "1px solid #f0f0f0" },
  td: { padding: "8px", verticalAlign: "middle" },
  empty: { padding: 24, textAlign: "center", color: "#888" },
  error: {
    background: "#fee",
    color: "#900",
    padding: 8,
    borderRadius: 4,
    margin: "8px 0",
  },
  serviceTag: {
    display: "inline-block",
    padding: "2px 8px",
    borderRadius: 3,
    color: "white",
    fontSize: 11,
    marginRight: 6,
  },
  serviceChip: {
    display: "inline-block",
    padding: "1px 6px",
    borderRadius: 10,
    color: "white",
    fontSize: 10,
    marginRight: 4,
  },
  errorChip: {
    display: "inline-block",
    padding: "1px 6px",
    borderRadius: 10,
    background: "#c33",
    color: "white",
    fontSize: 10,
    marginRight: 4,
  },
  backButton: {
    background: "transparent",
    border: "1px solid #ccc",
    borderRadius: 4,
    padding: "4px 10px",
    cursor: "pointer",
    fontSize: 12,
    marginBottom: 12,
  },
  traceHeader: {
    display: "flex",
    gap: 24,
    flexWrap: "wrap",
    background: "#fafafa",
    border: "1px solid #eee",
    borderRadius: 6,
    padding: "10px 12px",
    fontSize: 13,
    marginBottom: 12,
  },
  code: {
    fontFamily: "ui-monospace, monospace",
    fontSize: 12,
    background: "#fff",
    padding: "1px 4px",
    border: "1px solid #ddd",
    borderRadius: 3,
  },
  waterfall: { border: "1px solid #eee", borderRadius: 6 },
  waterfallHeader: {
    display: "flex",
    alignItems: "center",
    background: "#f5f5f5",
    borderBottom: "1px solid #eee",
    fontSize: 11,
    color: "#555",
    textTransform: "uppercase",
    padding: "4px 0",
  },
  spanInfoCol: {
    flex: "0 0 360px",
    display: "flex",
    alignItems: "center",
    gap: 8,
    padding: "4px 8px",
    fontFamily: "ui-monospace, SFMono-Regular, monospace",
    fontSize: 12,
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
  },
  spanBarCol: {
    flex: 1,
    position: "relative",
    height: 22,
  },
  spanRow: {
    display: "flex",
    alignItems: "center",
    borderBottom: "1px solid #f6f6f6",
    cursor: "pointer",
  },
  serviceDot: { width: 8, height: 8, borderRadius: 4, flexShrink: 0 },
  serviceName: { color: "#888", fontSize: 11 },
  spanName: { color: "#222", fontWeight: 500 },
  threadTag: {
    fontSize: 10,
    color: "#446",
    background: "#eef",
    padding: "1px 5px",
    borderRadius: 8,
  },
  payloadTag: {
    minWidth: 0,
    maxWidth: 160,
    overflow: "hidden",
    textOverflow: "ellipsis",
    fontSize: 10,
    color: "#075",
    background: "#e9f8f2",
    border: "1px solid #cceade",
    padding: "1px 5px",
    borderRadius: 8,
  },
  barTrack: { position: "relative", height: 22, margin: "0 8px" },
  bar: {
    position: "absolute",
    top: 4,
    height: 14,
    minWidth: 2,
    borderRadius: 2,
    opacity: 0.85,
  },
  barLabel: {
    position: "absolute",
    top: 5,
    fontSize: 10,
    color: "#666",
    whiteSpace: "nowrap",
  },
  ruler: { position: "relative", height: 18, width: "100%" },
  rulerTick: {
    position: "absolute",
    top: 0,
    height: "100%",
    borderLeft: "1px dashed #ddd",
  },
  rulerLabel: {
    fontSize: 10,
    color: "#888",
    position: "absolute",
    top: 2,
    left: 4,
    whiteSpace: "nowrap",
  },
  panel: {
    position: "fixed",
    bottom: 16,
    right: 16,
    width: "min(560px, 90vw)",
    maxHeight: "70vh",
    background: "white",
    border: "1px solid #ccc",
    borderRadius: 8,
    boxShadow: "0 8px 24px rgba(0,0,0,0.15)",
    overflow: "auto",
    padding: 16,
    fontSize: 13,
    zIndex: 100,
  },
  panelHeader: { display: "flex", alignItems: "center", marginBottom: 8 },
  panelMuted: { color: "#888", fontSize: 12 },
  panelClose: {
    marginLeft: "auto",
    background: "transparent",
    border: "none",
    fontSize: 14,
    cursor: "pointer",
  },
  panelGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(2, 1fr)",
    gap: 8,
    margin: "8px 0",
  },
  stat: { background: "#f7f7f7", padding: 8, borderRadius: 4 },
  statLabel: {
    fontSize: 10,
    color: "#666",
    textTransform: "uppercase",
    marginBottom: 2,
  },
  statValue: { fontSize: 13, fontFamily: "ui-monospace, monospace" },
  attrTable: {
    width: "100%",
    borderCollapse: "collapse",
    fontFamily: "ui-monospace, monospace",
    fontSize: 11,
    marginTop: 8,
  },
  attrKey: {
    padding: "3px 8px",
    color: "#444",
    background: "#f7f7f7",
    width: 160,
    verticalAlign: "top",
  },
  attrValue: {
    padding: "3px 8px",
    wordBreak: "break-all",
    verticalAlign: "top",
  },
  payloadDetails: { marginTop: 8 },
  payloadSummary: {
    cursor: "pointer",
    fontSize: 11,
    color: "#444",
    textTransform: "uppercase",
  },
  payloadPre: {
    background: "#0b1020",
    color: "#cbd6f0",
    padding: 12,
    borderRadius: 4,
    fontSize: 11,
    overflow: "auto",
    maxHeight: 320,
    margin: "6px 0 0",
  },
};
