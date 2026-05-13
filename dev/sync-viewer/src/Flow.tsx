import { useMemo, useState } from "react";
import type React from "react";
import { useQuery } from "@tanstack/react-query";
import { runQuery } from "./api.js";
import { buildFlowSql, flowPayloadDetails, resolveFlowAttrs, type FlowRow } from "./flowRows.js";

export function FlowList(props: { minutes: number }) {
  const { minutes } = props;
  const [limit, setLimit] = useState(5000);
  const [payloadFilter, setPayloadFilter] = useState("");
  const [layerFilter, setLayerFilter] = useState("");
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});

  const { data, error } = useQuery({
    queryKey: ["flow", minutes, limit, payloadFilter],
    queryFn: () => fetchFlow(minutes, limit, payloadFilter),
  });

  const rows = useMemo(() => {
    const allRows = data ?? [];
    if (!layerFilter) return allRows;
    return allRows.filter((row) => layerLabel(row) === layerFilter);
  }, [data, layerFilter]);

  return (
    <div>
      <section style={styles.controls}>
        <Field label="Limit">
          <input
            type="number"
            min={1}
            value={limit}
            onChange={(event) => setLimit(Number(event.target.value) || 100)}
            style={styles.input}
          />
        </Field>
        <Field label="Payload">
          <input
            type="text"
            placeholder="any"
            value={payloadFilter}
            onChange={(event) => setPayloadFilter(event.target.value)}
            style={styles.input}
          />
        </Field>
        <Field label="Layer">
          <select
            value={layerFilter}
            onChange={(event) => setLayerFilter(event.target.value)}
            style={styles.input}
          >
            <option value="">any</option>
            <option value="browser/main">browser/main</option>
            <option value="browser/worker">browser/worker</option>
            <option value="server">server</option>
          </select>
        </Field>
      </section>

      {error && <div style={styles.error}>error: {String(error.message ?? error)}</div>}

      <table style={styles.table}>
        <thead>
          <tr>
            <th style={styles.th}>time</th>
            <th style={styles.th}>layer</th>
            <th style={styles.th}>dir</th>
            <th style={styles.th}>payload</th>
            <th style={styles.th}>peer</th>
            <th style={styles.th}>tier</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => {
            const rowKey = `${row.Timestamp}-${index}`;
            return (
              <FlowTableRows
                key={rowKey}
                row={row}
                expanded={!!expandedRows[rowKey]}
                onToggle={() => setExpandedRows((prev) => ({ ...prev, [rowKey]: !prev[rowKey] }))}
              />
            );
          })}
          {rows.length === 0 && !error && (
            <tr>
              <td colSpan={6} style={styles.empty}>
                No sync messages in the selected window.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function FlowTableRows(props: { row: FlowRow; expanded: boolean; onToggle: () => void }) {
  const { row, expanded, onToggle } = props;
  const attrs = resolveFlowAttrs(row);
  const payloadDetails = flowPayloadDetails(attrs);
  const canExpand = payloadDetails.length > 0;

  return (
    <>
      <tr
        style={{
          ...styles.tr,
          background: rowBackground(layerLabel(row)),
          cursor: canExpand ? "pointer" : "default",
        }}
        onClick={() => canExpand && onToggle()}
      >
        <td style={styles.td}>{formatTime(row.Timestamp)}</td>
        <td style={styles.td}>{layerLabel(row)}</td>
        <td style={{ ...styles.td, ...directionStyle(row.SpanName) }}>
          {row.SpanName === "sync.send" ? "send" : "recv"}
        </td>
        <td style={{ ...styles.td, fontWeight: 500 }}>{attrs.payload || "-"}</td>
        <td style={styles.td}>{shortPeer(attrs.peer_kind, attrs.peer_id)}</td>
        <td style={styles.td}>{attrs.tier || "-"}</td>
      </tr>
      {expanded && canExpand && (
        <tr>
          <td colSpan={6} style={styles.expanded}>
            {payloadDetails.map((detail) => (
              <div key={detail.label} style={styles.payloadRow}>
                <span style={styles.payloadLabel}>{detail.label}</span>
                {detail.kind === "json" ? (
                  <pre style={styles.pre}>{prettyJson(detail.value)}</pre>
                ) : (
                  <code style={styles.code}>{detail.value}</code>
                )}
              </div>
            ))}
          </td>
        </tr>
      )}
    </>
  );
}

async function fetchFlow(
  minutes: number,
  limit: number,
  payloadFilter: string,
): Promise<FlowRow[]> {
  const sql = buildFlowSql({ minutes, limit, payloadFilter });
  const rows = await runQuery<any>(sql);
  return rows.map((row) => ({ ...row, Timestamp: row.ts_str })) as FlowRow[];
}

function Field(props: { label: string; children: React.ReactNode }) {
  return (
    <label style={styles.field}>
      <span style={styles.fieldLabel}>{props.label}</span>
      {props.children}
    </label>
  );
}

function layerLabel(row: FlowRow): string {
  if (row.ServiceName === "jazz-dev-server" || row.ServiceName === "jazz-server") return "server";
  if (row.thread === "worker") return "browser/worker";
  if (row.thread === "main") return "browser/main";
  return row.ServiceName;
}

function rowBackground(layer: string): string {
  if (layer === "server") return "#f5f0ff";
  if (layer === "browser/worker") return "#f0fff5";
  if (layer === "browser/main") return "#fff8f0";
  return "white";
}

function directionStyle(span: FlowRow["SpanName"]): React.CSSProperties {
  return span === "sync.send"
    ? { color: "#047857", fontWeight: 600 }
    : { color: "#1d4ed8", fontWeight: 600 };
}

function shortPeer(kind: string, id: string): string {
  if (!id) return kind;
  return `${kind || "?"}:${id.slice(0, 8)}`;
}

function formatTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  const hh = String(date.getHours()).padStart(2, "0");
  const mm = String(date.getMinutes()).padStart(2, "0");
  const ss = String(date.getSeconds()).padStart(2, "0");
  const ms = String(date.getMilliseconds()).padStart(3, "0");
  return `${hh}:${mm}:${ss}.${ms}`;
}

function prettyJson(value: string): string {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

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
  fieldLabel: { fontSize: 11, color: "#6b7280", textTransform: "uppercase" },
  input: {
    padding: "4px 8px",
    border: "1px solid #d1d5db",
    borderRadius: 4,
    fontSize: 14,
    width: 130,
  },
  table: {
    width: "100%",
    borderCollapse: "collapse",
    fontFamily: "ui-monospace, SFMono-Regular, monospace",
    fontSize: 12,
  },
  th: {
    textAlign: "left",
    padding: "6px 8px",
    borderBottom: "2px solid #ddd",
    fontSize: 11,
    color: "#4b5563",
    textTransform: "uppercase",
  },
  tr: { borderBottom: "1px solid #f0f0f0" },
  td: { padding: "4px 8px", verticalAlign: "top", whiteSpace: "nowrap" },
  empty: { padding: 24, textAlign: "center", color: "#6b7280" },
  error: {
    background: "#fee2e2",
    color: "#991b1b",
    padding: 8,
    borderRadius: 4,
    margin: "8px 0",
  },
  expanded: { padding: 12, background: "#fafafa" },
  payloadRow: {
    display: "grid",
    gridTemplateColumns: "120px minmax(0, 1fr)",
    gap: 12,
    alignItems: "start",
    marginBottom: 8,
  },
  payloadLabel: {
    color: "#6b7280",
    fontSize: 11,
    textTransform: "uppercase",
    paddingTop: 2,
  },
  code: {
    fontFamily: "ui-monospace, SFMono-Regular, monospace",
    fontSize: 11,
    whiteSpace: "pre-wrap",
    overflowWrap: "anywhere",
  },
  pre: {
    margin: 0,
    fontSize: 11,
    overflowX: "auto",
    whiteSpace: "pre",
  },
};
