import { useMemo, useState } from "react";
import type React from "react";
import { useQuery } from "@tanstack/react-query";
import { runQuery } from "./api.js";
import { buildSessionDetailSql, buildSessionListSql, type SyncLogRow } from "./sessionRows.js";
import { buildSessionSummaries } from "./sessionModel.js";
import { sessionDetailHash, sessionListHash } from "./route.js";

export function SessionListPage(props: {
  minutes: number;
  onMinutesChange: (minutes: number) => void;
}) {
  const { minutes, onMinutesChange } = props;
  const [limit, setLimit] = useState(500);

  const sessionsQuery = useQuery({
    queryKey: ["sync-sessions", minutes, limit],
    queryFn: () => fetchSessionRows(minutes, limit),
  });
  const sessions = useMemo(
    () => buildSessionSummaries(sessionsQuery.data ?? []),
    [sessionsQuery.data],
  );

  return (
    <section style={styles.page}>
      <div style={styles.toolbar}>
        <div style={styles.pageTitle}>Sync Sessions</div>
        <Field label="Window (min)">
          <input
            type="number"
            min={1}
            value={minutes}
            onChange={(event) => onMinutesChange(Math.max(1, Number(event.target.value) || 1))}
            style={styles.input}
          />
        </Field>
        <Field label="Limit">
          <input
            type="number"
            min={1}
            value={limit}
            onChange={(event) => setLimit(Math.max(1, Number(event.target.value) || 1))}
            style={styles.input}
          />
        </Field>
        <button type="button" onClick={() => void sessionsQuery.refetch()} style={styles.button}>
          Refresh
        </button>
        <div style={styles.stats}>
          <span>{sessions.length} sessions</span>
          {sessionsQuery.isFetching && <span>loading</span>}
        </div>
      </div>

      {sessionsQuery.error && (
        <div style={styles.error}>error: {errorText(sessionsQuery.error)}</div>
      )}

      <div style={styles.sessionList}>
        {sessions.map((session) => (
          <a
            key={session.sessionId}
            href={sessionDetailHash(session.sessionId)}
            style={styles.card}
          >
            <div style={styles.cardTitle}>{session.title}</div>
            <div style={styles.cardSubtitle}>{session.subtitle}</div>
            <div style={styles.cardMeta}>
              <span>{formatTime(session.startedAt)}</span>
              <span>{shortSession(session.sessionId)}</span>
            </div>
          </a>
        ))}
        {sessions.length === 0 && (
          <div style={styles.emptyList}>No sync sessions in this window.</div>
        )}
      </div>
    </section>
  );
}

export function SessionDetailPage(props: { sessionId: string }) {
  const { sessionId } = props;
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});

  const logsQuery = useQuery({
    queryKey: ["sync-session-detail", sessionId],
    queryFn: () => fetchSessionLogs(sessionId),
  });
  const logs = logsQuery.data ?? [];

  return (
    <section style={styles.page}>
      <div style={styles.toolbar}>
        <a href={sessionListHash()} style={styles.backLink}>
          Sessions
        </a>
        <div style={styles.pageTitle}>{shortSession(sessionId)}</div>
        <button type="button" onClick={() => void logsQuery.refetch()} style={styles.button}>
          Refresh
        </button>
        <div style={styles.stats}>
          <span>{logs.length} logs</span>
          {logsQuery.isFetching && <span>loading</span>}
        </div>
      </div>

      {logsQuery.error && <div style={styles.error}>error: {errorText(logsQuery.error)}</div>}

      <LogTable
        logs={logs}
        expandedRows={expandedRows}
        onToggle={(rowKey) => setExpandedRows((prev) => ({ ...prev, [rowKey]: !prev[rowKey] }))}
      />
    </section>
  );
}

function LogTable(props: {
  logs: SyncLogRow[];
  expandedRows: Record<string, boolean>;
  onToggle: (rowKey: string) => void;
}) {
  const { logs, expandedRows, onToggle } = props;
  return (
    <table style={styles.table}>
      <thead>
        <tr>
          <th style={styles.th}>time</th>
          <th style={styles.th}>service</th>
          <th style={styles.th}>direction</th>
          <th style={styles.th}>kind</th>
          <th style={styles.th}>summary</th>
        </tr>
      </thead>
      <tbody>
        {logs.map((log, index) => {
          const rowKey = log.LogId || `${log.Timestamp}-${index}`;
          const expanded = !!expandedRows[rowKey];
          return (
            <LogTableRows
              key={rowKey}
              rowKey={rowKey}
              log={log}
              expanded={expanded}
              onToggle={() => onToggle(rowKey)}
            />
          );
        })}
        {logs.length === 0 && (
          <tr>
            <td colSpan={5} style={styles.emptyTable}>
              No sync logs for this session.
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );
}

function LogTableRows(props: {
  rowKey: string;
  log: SyncLogRow;
  expanded: boolean;
  onToggle: () => void;
}) {
  const { log, expanded, onToggle } = props;
  const hasBody = !!stringValue(log.Body);
  return (
    <>
      <tr
        style={{
          ...styles.tr,
          background: rowBackground(log),
          cursor: hasBody ? "pointer" : "default",
        }}
        onClick={() => hasBody && onToggle()}
      >
        <td style={styles.td}>{formatTime(log.Timestamp)}</td>
        <td style={styles.td}>{serviceLabel(log)}</td>
        <td style={{ ...styles.td, ...directionStyle(log.sync_direction) }}>
          {log.sync_direction || "-"}
        </td>
        <td style={styles.td}>{log.sync_message_kind || log.EventName || "-"}</td>
        <td style={{ ...styles.td, ...styles.summaryCell }}>{logSummary(log)}</td>
      </tr>
      {expanded && hasBody && (
        <tr>
          <td colSpan={5} style={styles.expanded}>
            <div style={styles.payloadRow}>
              <span style={styles.payloadLabel}>Log body</span>
              <pre style={styles.pre}>{prettyJson(log.Body)}</pre>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

async function fetchSessionRows(minutes: number, limit: number): Promise<SyncLogRow[]> {
  return await runQuery<SyncLogRow>(buildSessionListSql({ minutes, limit }));
}

async function fetchSessionLogs(sessionId: string): Promise<SyncLogRow[]> {
  return await runQuery<SyncLogRow>(buildSessionDetailSql(sessionId));
}

function Field(props: { label: string; children: React.ReactNode }) {
  return (
    <label style={styles.field}>
      <span style={styles.fieldLabel}>{props.label}</span>
      {props.children}
    </label>
  );
}

function logSummary(log: SyncLogRow): string {
  if (log.sync_operation && log.sync_table && log.sync_row_id) {
    return `${log.sync_operation} ${log.sync_table}:${log.sync_row_id}`;
  }
  if (log.sync_data_records) return log.sync_data_records;
  if (log.sync_read_records) return log.sync_read_records;
  if (log.sync_bundle_tx_ids) return `bundle ${log.sync_bundle_tx_ids}`;
  if (log.sync_tx_id) return `tx ${log.sync_tx_id}`;
  return bodySummary(log.Body) || log.EventName || "";
}

function bodySummary(body: string | undefined): string {
  const text = stringValue(body);
  if (!text) return "";
  try {
    const parsed = JSON.parse(text) as { event?: unknown; message_kind?: unknown };
    return [parsed.event, parsed.message_kind].map(stringValue).filter(Boolean).join(" ");
  } catch {
    return text.slice(0, 120);
  }
}

function serviceLabel(log: SyncLogRow): string {
  const service = stringValue(log.ServiceName);
  if (service.endsWith("-browser")) return "browser";
  if (service.endsWith("-server")) return "server";
  return service || "-";
}

function rowBackground(log: SyncLogRow): string {
  const service = serviceLabel(log);
  if (service === "server") return "#f5f0ff";
  if (service === "browser") return "#fff8f0";
  return "#ffffff";
}

function directionStyle(direction: string | undefined): React.CSSProperties {
  if (stringValue(direction).includes("send")) return { color: "#047857", fontWeight: 600 };
  if (stringValue(direction).includes("receive")) return { color: "#1d4ed8", fontWeight: 600 };
  return { color: "#374151" };
}

function shortSession(sessionId: string | undefined): string {
  const text = stringValue(sessionId);
  return text.length > 8 ? text.slice(0, 8) : text;
}

function prettyJson(value: string | undefined): string {
  const text = stringValue(value);
  if (!text) return "";
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

function formatTime(value: string | undefined): string {
  const date = new Date(stringValue(value));
  if (Number.isNaN(date.getTime())) return stringValue(value) || "-";
  const hh = String(date.getHours()).padStart(2, "0");
  const mm = String(date.getMinutes()).padStart(2, "0");
  const ss = String(date.getSeconds()).padStart(2, "0");
  const ms = String(date.getMilliseconds()).padStart(3, "0");
  return `${hh}:${mm}:${ss}.${ms}`;
}

function stringValue(value: unknown): string {
  if (value === undefined || value === null) return "";
  return String(value);
}

function errorText(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

const styles: Record<string, React.CSSProperties> = {
  page: {
    display: "flex",
    flexDirection: "column",
    minHeight: "calc(100vh - 42px)",
    background: "#f3f4f6",
  },
  toolbar: {
    display: "flex",
    alignItems: "flex-end",
    gap: 12,
    padding: "10px 14px",
    borderBottom: "1px solid #d1d5db",
    background: "#ffffff",
  },
  pageTitle: {
    alignSelf: "center",
    marginRight: 6,
    fontSize: 13,
    fontWeight: 800,
    color: "#0f172a",
  },
  field: { display: "flex", flexDirection: "column", gap: 2 },
  fieldLabel: { fontSize: 10, color: "#6b7280", textTransform: "uppercase", fontWeight: 700 },
  input: {
    padding: "4px 8px",
    border: "1px solid #cbd5e1",
    borderRadius: 3,
    fontSize: 13,
    width: 104,
    background: "#ffffff",
  },
  button: {
    border: "1px solid #cbd5e1",
    background: "#ffffff",
    borderRadius: 3,
    padding: "5px 10px",
    color: "#0f172a",
    cursor: "pointer",
    fontSize: 12,
    textDecoration: "none",
  },
  backLink: {
    border: "1px solid #cbd5e1",
    background: "#ffffff",
    borderRadius: 3,
    padding: "5px 10px",
    color: "#0f172a",
    fontSize: 12,
    textDecoration: "none",
  },
  stats: {
    display: "flex",
    gap: 10,
    color: "#64748b",
    fontSize: 12,
    paddingBottom: 5,
  },
  sessionList: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))",
    gap: 10,
    padding: 12,
  },
  card: {
    display: "block",
    padding: 12,
    border: "1px solid #d1d5db",
    borderRadius: 4,
    background: "#ffffff",
    color: "#111827",
    textDecoration: "none",
  },
  cardTitle: { fontSize: 13, fontWeight: 800, marginBottom: 4, overflowWrap: "anywhere" },
  cardSubtitle: { fontSize: 12, color: "#64748b", marginBottom: 8, overflowWrap: "anywhere" },
  cardMeta: {
    display: "flex",
    justifyContent: "space-between",
    gap: 10,
    fontSize: 11,
    color: "#64748b",
    fontFamily: "ui-monospace, SFMono-Regular, monospace",
  },
  table: {
    width: "calc(100% - 24px)",
    margin: 12,
    borderCollapse: "collapse",
    background: "#ffffff",
    fontFamily: "ui-monospace, SFMono-Regular, monospace",
    fontSize: 12,
  },
  th: {
    textAlign: "left",
    padding: "7px 8px",
    borderBottom: "2px solid #d1d5db",
    fontSize: 11,
    color: "#4b5563",
    textTransform: "uppercase",
  },
  tr: { borderBottom: "1px solid #e5e7eb" },
  td: { padding: "5px 8px", verticalAlign: "top", whiteSpace: "nowrap" },
  summaryCell: { whiteSpace: "normal", overflowWrap: "anywhere" },
  expanded: { padding: 12, background: "#fafafa" },
  payloadRow: {
    display: "grid",
    gridTemplateColumns: "120px minmax(0, 1fr)",
    gap: 12,
    alignItems: "start",
  },
  payloadLabel: {
    color: "#6b7280",
    fontSize: 11,
    textTransform: "uppercase",
    paddingTop: 2,
  },
  pre: {
    margin: 0,
    fontSize: 11,
    overflowX: "auto",
    whiteSpace: "pre",
    fontFamily: "ui-monospace, SFMono-Regular, monospace",
  },
  emptyList: { padding: 24, color: "#64748b", fontSize: 13 },
  emptyTable: { padding: 24, textAlign: "center", color: "#64748b" },
  error: {
    margin: 12,
    padding: 10,
    background: "#fef2f2",
    color: "#991b1b",
    border: "1px solid #fecaca",
    fontSize: 12,
  },
};
