export type FlowAttributeSource = {
  payload?: string;
  payload_json?: string;
  peer_kind?: string;
  peer_id?: string;
  tier?: string;
  fields?: string;
};

export type FlowRow = FlowAttributeSource & {
  Timestamp: string;
  ServiceName: string;
  SpanName: "sync.send" | "sync.recv";
  thread?: string;
};

export type FlowAttrs = {
  payload: string;
  peer_kind: string;
  peer_id: string;
  tier: string;
  payload_json: string;
};

export type FlowPayloadDetail = {
  label: string;
  value: string;
  kind: "text" | "json";
};

export type FlowSqlFilters = {
  minutes: number;
  limit: number;
  payloadFilter: string;
};

export function buildFlowSql(filters: FlowSqlFilters): string {
  const where: string[] = [
    `Timestamp > now() - INTERVAL ${Math.max(1, Math.floor(filters.minutes) || 1)} MINUTE`,
    `ServiceName IN ('jazz-browser', 'jazz-dev-server', 'jazz-server')`,
    `SpanName IN ('sync.send', 'sync.recv')`,
  ];
  const payload = filters.payloadFilter.trim();
  if (payload) where.push(`SpanAttributes['payload'] = '${escapeSqlString(payload)}'`);

  return `
    SELECT
      toString(Timestamp) AS ts_str,
      ServiceName,
      SpanName,
      SpanAttributes['jazz.runtime_thread'] AS thread,
      SpanAttributes['jazz.span.fields'] AS fields,
      SpanAttributes['payload'] AS payload,
      SpanAttributes['payload_json'] AS payload_json,
      SpanAttributes['peer_kind'] AS peer_kind,
      SpanAttributes['peer_id'] AS peer_id,
      SpanAttributes['tier'] AS tier
    FROM otel_traces
    WHERE ${where.join(" AND ")}
    ORDER BY Timestamp DESC
    LIMIT ${Math.max(1, Math.floor(filters.limit) || 100)}
  `;
}

export function resolveFlowAttrs(row: FlowAttributeSource): FlowAttrs {
  let payload = row.payload || "";
  let peer_kind = row.peer_kind || "";
  let peer_id = row.peer_id || "";
  let tier = row.tier || "";
  let payload_json = row.payload_json || "";

  if ((!payload || !peer_kind || !payload_json) && row.fields) {
    try {
      const fields = JSON.parse(row.fields) as Record<string, string>;
      payload = payload || fields.payload || "";
      peer_kind = peer_kind || fields.peer_kind || "";
      peer_id = peer_id || fields.peer_id || "";
      tier = tier || fields.tier || "";
      payload_json = payload_json || fields.payload_json || "";
    } catch {
      // Keep direct attributes when the legacy field blob is malformed.
    }
  }

  return { payload, peer_kind, peer_id, tier, payload_json };
}

export function flowPayloadDetails(attrs: FlowAttrs): FlowPayloadDetail[] {
  const details: FlowPayloadDetail[] = [];
  if (attrs.payload) {
    details.push({ label: "payload", value: attrs.payload, kind: "text" });
  }
  if (attrs.payload_json) {
    details.push({ label: "payload_json", value: attrs.payload_json, kind: "json" });
  }
  return details;
}

function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}
