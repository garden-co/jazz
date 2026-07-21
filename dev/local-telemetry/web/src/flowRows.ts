export type FlowAttributeSource = {
  payload?: string;
  summary?: string;
  encoded_len?: string;
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
  summary: string;
  encoded_len: string;
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
  const minutes = Math.max(1, Math.floor(filters.minutes) || 1);
  const limit = Math.max(1, Math.floor(filters.limit) || 100);
  const cutoffNs = (Date.now() - minutes * 60_000) * 1_000_000;

  const where = [
    `start_time_unix_nano > ${cutoffNs}`,
    `service_name IN ('jazz-browser', 'jazz-dev-server', 'jazz-server')`,
    `name IN ('sync.send', 'sync.recv')`,
  ];

  const payload = filters.payloadFilter.trim();
  if (payload) where.push(`${attr("payload")} = '${escapeSqlString(payload)}'`);

  return `
    SELECT
      strftime(to_timestamp(start_time_unix_nano / 1e9), '%Y-%m-%dT%H:%M:%S.%gZ') AS Timestamp,
      service_name AS ServiceName,
      name AS SpanName,
      ${attr("jazz.runtime_thread")} AS thread,
      ${attr("jazz.span.fields")} AS fields,
      ${attr("payload")} AS payload,
      ${attr("summary")} AS summary,
      ${attr("encoded_len")} AS encoded_len,
      ${attr("peer_kind")} AS peer_kind,
      ${attr("peer_id")} AS peer_id,
      ${attr("tier")} AS tier
    FROM spans
    WHERE ${where.join(" AND ")}
    ORDER BY start_time_unix_nano DESC
    LIMIT ${limit}
  `;
}

// Pulls a single attribute value out of the OTLP-shaped `attributes` JSON array
// on a span row, falling back across the common value variants.
function attr(key: string): string {
  return `(
    SELECT COALESCE(
      json_extract_string(a, '$.value.stringValue'),
      CAST(json_extract(a, '$.value.intValue') AS VARCHAR),
      CAST(json_extract(a, '$.value.doubleValue') AS VARCHAR),
      CAST(json_extract(a, '$.value.boolValue') AS VARCHAR)
    )
    FROM UNNEST(attributes::JSON[]) AS u(a)
    WHERE json_extract_string(a, '$.key') = '${escapeSqlString(key)}'
    LIMIT 1
  )`;
}

export function resolveFlowAttrs(row: FlowAttributeSource): FlowAttrs {
  let payload = row.payload || "";
  let peer_kind = row.peer_kind || "";
  let peer_id = row.peer_id || "";
  let tier = row.tier || "";
  let summary = row.summary || "";
  let encoded_len = row.encoded_len || "";

  if ((!payload || !peer_kind || !summary) && row.fields) {
    try {
      const fields = JSON.parse(row.fields) as Record<string, string>;
      payload = payload || fields.payload || "";
      peer_kind = peer_kind || fields.peer_kind || "";
      peer_id = peer_id || fields.peer_id || "";
      tier = tier || fields.tier || "";
      summary = summary || fields.summary || "";
      encoded_len = encoded_len || fields.encoded_len || "";
    } catch {
      // Keep direct attributes when the legacy field blob is malformed.
    }
  }

  return { payload, peer_kind, peer_id, tier, summary, encoded_len };
}

export function flowPayloadDetails(attrs: FlowAttrs): FlowPayloadDetail[] {
  const details: FlowPayloadDetail[] = [];
  if (attrs.payload) {
    details.push({ label: "payload", value: attrs.payload, kind: "text" });
  }
  if (attrs.summary) {
    details.push({ label: "summary", value: attrs.summary, kind: "text" });
  }
  if (attrs.encoded_len) {
    details.push({ label: "encoded_len", value: attrs.encoded_len, kind: "text" });
  }
  return details;
}

function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}
