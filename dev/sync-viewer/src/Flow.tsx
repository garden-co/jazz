import { useMemo, useState } from "react";
import type React from "react";
import { useQuery } from "@tanstack/react-query";
import { runQuery } from "./api.js";
import { flowPayloadDetails, resolveFlowAttrs, type FlowRow as Row } from "./flowRows.js";

export function FlowList(props: { minutes: number }) {
  const { minutes } = props;
  const [limit, setLimit] = useState(5000);
  const [payloadFilter, setPayloadFilter] = useState("");
  const [layerFilter, setLayerFilter] = useState("");
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  const { data, error } = useQuery({
    queryKey: ["flow", minutes, limit, payloadFilter],
    queryFn: () => fetchFlow(minutes, limit, payloadFilter),
  });
  const rows = data ?? [];

  const filteredRows = useMemo(() => {
    if (!layerFilter) return rows;
    return rows.filter((r) => layerLabel(r) === layerFilter);
  }, [rows, layerFilter]);

  const groups = useMemo(() => groupAdjacent(filteredRows), [filteredRows]);
  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>({});

  const counts = useMemo(() => {
    const c: Record<string, number> = {};
    for (const r of rows) {
      const k = `${layerLabel(r)} · ${r.SpanName}`;
      c[k] = (c[k] ?? 0) + 1;
    }
    return c;
  }, [rows]);

  return (
    <div>
      <section style={styles.controls}>
        <Field label="Limit">
          <input
            type="number"
            min={1}
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value) || 100)}
            style={styles.input}
          />
        </Field>
        <Field label="Payload">
          <input
            type="text"
            placeholder="any"
            value={payloadFilter}
            onChange={(e) => setPayloadFilter(e.target.value)}
            style={styles.input}
          />
        </Field>
        <Field label="Layer">
          <select
            value={layerFilter}
            onChange={(e) => setLayerFilter(e.target.value)}
            style={styles.input}
          >
            <option value="">any</option>
            <option value="browser/main">browser/main</option>
            <option value="browser/worker">browser/worker</option>
            <option value="server">server</option>
          </select>
        </Field>
        <div style={styles.summary}>
          {Object.entries(counts).map(([k, v]) => (
            <span key={k} style={styles.tag}>
              {k} <strong>{v}</strong>
            </span>
          ))}
        </div>
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
          {groups.map((group) => (
            <GroupRows
              key={group.key}
              group={group}
              expanded={!!expandedGroups[group.key]}
              onToggle={() =>
                setExpandedGroups((prev) => ({
                  ...prev,
                  [group.key]: !prev[group.key],
                }))
              }
              expandedRows={expanded}
              onToggleRow={(rowKey) =>
                setExpanded((prev) => ({ ...prev, [rowKey]: !prev[rowKey] }))
              }
            />
          ))}
          {filteredRows.length === 0 && !error && (
            <tr>
              <td colSpan={6} style={styles.empty}>
                No sync messages in the last {minutes} minute(s).
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

async function fetchFlow(minutes: number, limit: number, payloadFilter: string): Promise<Row[]> {
  const where: string[] = [
    `Timestamp > now() - INTERVAL ${minutes} MINUTE`,
    `ServiceName IN ('jazz-browser', 'jazz-dev-server')`,
    `SpanName IN ('sync.send', 'sync.recv')`,
  ];
  if (payloadFilter) {
    where.push(`SpanAttributes['payload'] = '${payloadFilter.replace(/'/g, "''")}'`);
  }
  const sql = `
    SELECT
      toString(Timestamp) AS ts_str,
      ServiceName,
      SpanName,
      SpanAttributes['jazz.runtime_thread'] AS thread,
      SpanAttributes['jazz.span.fields']    AS fields,
      SpanAttributes['payload']             AS payload,
      SpanAttributes['payload_json']        AS payload_json,
      SpanAttributes['peer_kind']           AS peer_kind,
      SpanAttributes['peer_id']             AS peer_id,
      SpanAttributes['tier']                AS tier
    FROM otel_traces
    WHERE ${where.join(" AND ")}
    ORDER BY Timestamp DESC
    LIMIT ${limit}
  `;
  const rows = await runQuery<any>(sql);
  return rows.map((r) => ({ ...r, Timestamp: r.ts_str })) as Row[];
}

// ---------------------------------------------------------------------------
// Grouped rendering: collapse contiguous identical sync messages.
// ---------------------------------------------------------------------------

type Group = {
  key: string;
  rows: Array<{ row: Row; rowKey: string }>;
};

function groupAdjacent(rows: Row[]): Group[] {
  const groups: Group[] = [];
  rows.forEach((row, i) => {
    const attrs = resolveFlowAttrs(row);
    const layer = layerLabel(row);
    const fingerprint = [
      layer,
      row.SpanName,
      attrs.payload,
      attrs.peer_kind,
      attrs.peer_id,
      attrs.tier,
    ].join("|");
    const rowKey = `${row.Timestamp}-${i}`;
    const last = groups[groups.length - 1];
    if (last && last.rows[0] && lastFingerprint(last) === fingerprint) {
      last.rows.push({ row, rowKey });
    } else {
      groups.push({
        key: `${i}-${fingerprint}`,
        rows: [{ row, rowKey }],
      });
    }
  });
  return groups;
}

function lastFingerprint(group: Group): string {
  // The group key embeds the fingerprint after the leading index — trim it.
  const idx = group.key.indexOf("-");
  return idx >= 0 ? group.key.slice(idx + 1) : group.key;
}

function GroupRows(props: {
  group: Group;
  expanded: boolean;
  onToggle: () => void;
  expandedRows: Record<string, boolean>;
  onToggleRow: (key: string) => void;
}) {
  const { group, expanded, onToggle, expandedRows, onToggleRow } = props;
  const first = group.rows[0].row;
  const layer = layerLabel(first);
  const attrs = resolveFlowAttrs(first);

  if (group.rows.length === 1) {
    return (
      <RowDetail
        row={first}
        rowKey={group.rows[0].rowKey}
        expanded={!!expandedRows[group.rows[0].rowKey]}
        onToggle={() => onToggleRow(group.rows[0].rowKey)}
      />
    );
  }

  const startTime = group.rows[0].row.Timestamp;
  // Rows arrive newest-first from SQL; the visual "last" is the oldest.
  const endTime = group.rows[group.rows.length - 1].row.Timestamp;
  const [earlier, later] = (() => {
    const a = formatTime(startTime);
    const b = formatTime(endTime);
    return a <= b ? [a, b] : [b, a];
  })();
  const timeRange = earlier === later ? earlier : `${earlier} → ${later}`;

  return (
    <Frag>
      <tr
        onClick={onToggle}
        style={{
          ...styles.tr,
          ...styles.groupHeader,
          background: rowBackground(layer),
        }}
      >
        <td style={styles.td}>{timeRange}</td>
        <td style={styles.td}>{layer}</td>
        <td style={{ ...styles.td, ...directionStyle(first.SpanName) }}>
          {first.SpanName === "sync.send" ? "→ send" : "recv ←"}
        </td>
        <td style={{ ...styles.td, fontWeight: 500 }}>
          <span style={styles.chevron}>{expanded ? "▾" : "▸"}</span>
          {attrs.payload || "—"}
          <span style={styles.countBadge}>×{group.rows.length}</span>
        </td>
        <td style={styles.td}>{shortPeer(attrs)}</td>
        <td style={styles.td}>{attrs.tier ?? "—"}</td>
      </tr>
      {expanded &&
        group.rows.map(({ row, rowKey }) => (
          <RowDetail
            key={rowKey}
            row={row}
            rowKey={rowKey}
            indented
            expanded={!!expandedRows[rowKey]}
            onToggle={() => onToggleRow(rowKey)}
          />
        ))}
    </Frag>
  );
}

function RowDetail(props: {
  row: Row;
  rowKey: string;
  indented?: boolean;
  expanded: boolean;
  onToggle: () => void;
}) {
  const { row, indented, expanded, onToggle } = props;
  const layer = layerLabel(row);
  const attrs = resolveFlowAttrs(row);
  const payloadDetails = flowPayloadDetails(attrs);
  const canExpandPayload = payloadDetails.length > 0;
  return (
    <Frag>
      <tr
        style={{
          ...styles.tr,
          background: rowBackground(layer),
          cursor: canExpandPayload ? "pointer" : "default",
        }}
        onClick={() => canExpandPayload && onToggle()}
      >
        <td style={{ ...styles.td, paddingLeft: indented ? 24 : undefined }}>
          {formatTime(row.Timestamp)}
        </td>
        <td style={styles.td}>{layer}</td>
        <td style={{ ...styles.td, ...directionStyle(row.SpanName) }}>
          {row.SpanName === "sync.send" ? "→ send" : "recv ←"}
        </td>
        <td style={{ ...styles.td, fontWeight: 500 }}>{attrs.payload || "—"}</td>
        <td style={styles.td}>{shortPeer(attrs)}</td>
        <td style={styles.td}>{attrs.tier ?? "—"}</td>
      </tr>
      {expanded && canExpandPayload && (
        <tr>
          <td colSpan={6} style={styles.expanded}>
            <div style={styles.payloadDetails}>
              {payloadDetails.map((detail) => (
                <div key={detail.label} style={styles.payloadDetail}>
                  <div style={styles.payloadLabel}>{detail.label}</div>
                  {detail.kind === "json" ? (
                    <pre style={styles.pre}>{prettyJson(detail.value)}</pre>
                  ) : (
                    <code style={styles.payloadText}>{detail.value}</code>
                  )}
                </div>
              ))}
            </div>
          </td>
        </tr>
      )}
    </Frag>
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

function Frag(props: { children: React.ReactNode }) {
  return <>{props.children}</>;
}

function layerLabel(row: Row): string {
  if (row.ServiceName === "jazz-dev-server") return "server";
  if (row.thread === "worker") return "browser/worker";
  if (row.thread === "main") return "browser/main";
  return row.ServiceName;
}

function rowBackground(layer: string): string {
  switch (layer) {
    case "server":
      return "#f5f0ff";
    case "browser/worker":
      return "#f0fff5";
    case "browser/main":
      return "#fff8f0";
    default:
      return "white";
  }
}

function directionStyle(span: Row["SpanName"]): React.CSSProperties {
  return span === "sync.send"
    ? { color: "#0a6", fontWeight: 600 }
    : { color: "#06a", fontWeight: 600 };
}

function shortPeer(attrs: ReturnType<typeof resolveFlowAttrs>): string {
  if (!attrs.peer_id) return attrs.peer_kind ?? "";
  return `${attrs.peer_kind ?? "?"}:${attrs.peer_id.slice(0, 8)}`;
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

function prettyJson(s: string): string {
  try {
    return JSON.stringify(JSON.parse(s), null, 2);
  } catch {
    return s;
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
  summary: {
    display: "flex",
    gap: 6,
    flexWrap: "wrap",
    fontSize: 11,
    marginLeft: "auto",
  },
  tag: {
    background: "#eef",
    padding: "2px 8px",
    borderRadius: 12,
    color: "#446",
  },
  field: { display: "flex", flexDirection: "column", gap: 2 },
  fieldLabel: { fontSize: 11, color: "#666", textTransform: "uppercase" },
  input: {
    padding: "4px 8px",
    border: "1px solid #ccc",
    borderRadius: 4,
    fontSize: 14,
    width: 120,
  },
  error: {
    background: "#fee",
    color: "#900",
    padding: 8,
    borderRadius: 4,
    margin: "8px 0",
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
    color: "#555",
    textTransform: "uppercase",
  },
  tr: { borderBottom: "1px solid #f0f0f0" },
  groupHeader: {
    cursor: "pointer",
    fontWeight: 500,
  },
  chevron: {
    display: "inline-block",
    width: 14,
    color: "#666",
  },
  countBadge: {
    marginLeft: 8,
    fontSize: 10,
    background: "#446",
    color: "white",
    padding: "1px 6px",
    borderRadius: 8,
    fontWeight: 600,
  },
  td: { padding: "4px 8px", verticalAlign: "top", whiteSpace: "nowrap" },
  expanded: { padding: 0, background: "#fafafa" },
  payloadDetails: {
    padding: "8px 16px",
    display: "grid",
    gap: 8,
  },
  payloadDetail: {
    display: "grid",
    gridTemplateColumns: "120px minmax(0, 1fr)",
    gap: 12,
    alignItems: "start",
  },
  payloadLabel: {
    color: "#666",
    fontSize: 11,
    textTransform: "uppercase",
    paddingTop: 2,
  },
  payloadText: {
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
  empty: { padding: 24, textAlign: "center", color: "#888" },
};
