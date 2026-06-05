import type { SyncLogRow } from "./sessionRows.js";

export type SessionSummary = {
  sessionId: string;
  title: string;
  subtitle: string;
  startedAt: string;
  endedAt: string;
  eventCount: number;
};

export function buildSessionSummaries(rows: SyncLogRow[]): SessionSummary[] {
  const groups = new Map<string, SyncLogRow[]>();
  for (const row of rows) {
    const sessionId = stringValue(row.SessionId);
    if (!sessionId) continue;
    groups.set(sessionId, [...(groups.get(sessionId) ?? []), row]);
  }

  return Array.from(groups.entries())
    .map(([sessionId, sessionRows]) => sessionSummary(sessionId, sessionRows))
    .sort((a, b) => compareTimeDesc(a.endedAt, b.endedAt));
}

function sessionSummary(sessionId: string, rows: SyncLogRow[]): SessionSummary {
  const sorted = rows.slice().sort((a, b) => compareTimeAsc(rowTime(a), rowTime(b)));
  const startedAt = stringValue(sorted[0]?.Timestamp);
  const endedAt = stringValue(sorted[sorted.length - 1]?.Timestamp);
  const actors = unique(
    sorted.map((row) => stringValue(row.sync_direction) || serviceLabel(row)).filter(Boolean),
  );

  return {
    sessionId,
    title: sessionTitle(sorted),
    subtitle: `${sorted.length} ${sorted.length === 1 ? "event" : "events"}${
      actors.length ? ` - ${actors.join(" / ")}` : ""
    }`,
    startedAt,
    endedAt,
    eventCount: sorted.length,
  };
}

function sessionTitle(rows: SyncLogRow[]): string {
  for (const row of rows) {
    if (row.sync_operation && row.sync_table && row.sync_row_id) {
      return `${row.sync_operation} ${row.sync_table}:${row.sync_row_id}`;
    }
  }
  for (const row of rows) {
    if (row.sync_data_records) return row.sync_data_records;
  }
  for (const row of rows) {
    if (row.sync_message_kind) return row.sync_message_kind;
  }
  return "sync session";
}

function serviceLabel(row: SyncLogRow): string {
  const service = stringValue(row.ServiceName);
  if (service.endsWith("-browser")) return "browser";
  if (service.endsWith("-server")) return "server";
  return service;
}

function unique(values: string[]): string[] {
  return Array.from(new Set(values));
}

function rowTime(row: SyncLogRow): string {
  return stringValue(row.Timestamp);
}

function compareTimeAsc(a: string, b: string): number {
  return Date.parse(a || "0") - Date.parse(b || "0");
}

function compareTimeDesc(a: string, b: string): number {
  return compareTimeAsc(b, a);
}

function stringValue(value: unknown): string {
  if (value === undefined || value === null) return "";
  return String(value);
}
