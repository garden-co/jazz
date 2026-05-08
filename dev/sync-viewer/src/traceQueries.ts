export type TraceListQueryFilters = {
  minutes: number;
  serviceFilter: string;
  opFilter: string;
  traceIdFilter: string;
};

export function buildTraceListSql(filters: TraceListQueryFilters): string {
  const where: string[] = [
    `Timestamp > now() - INTERVAL ${Math.max(1, Math.floor(filters.minutes) || 1)} MINUTE`,
  ];
  const serviceFilter = filters.serviceFilter.trim();
  const opFilter = filters.opFilter.trim();
  const traceIdFilter = filters.traceIdFilter.trim();

  if (serviceFilter) where.push(`ServiceName = '${escapeSqlString(serviceFilter)}'`);
  if (opFilter) where.push(`SpanName ILIKE '%${escapeSearchText(opFilter)}%'`);
  if (traceIdFilter) where.push(`TraceId ILIKE '%${escapeSearchText(traceIdFilter)}%'`);

  // Per-trace summary. For root span info, prefer the span with empty
  // ParentSpanId; otherwise fall back to the earliest span.
  return `
    WITH base AS (
      SELECT
        TraceId,
        Timestamp,
        Duration,
        SpanName,
        ServiceName,
        ParentSpanId,
        StatusCode
      FROM otel_traces
      WHERE ${where.join(" AND ")}
    )
    SELECT
      TraceId,
      toString(min(Timestamp)) AS start,
      (
        toFloat64(max(toUnixTimestamp64Nano(Timestamp) + Duration))
        - toFloat64(min(toUnixTimestamp64Nano(Timestamp)))
      ) / 1e6 AS duration_ms,
      count(*) AS span_count,
      argMinIf(SpanName,    Timestamp, ParentSpanId = '') AS root_span_explicit,
      argMin(SpanName, Timestamp) AS root_span_fallback,
      argMinIf(ServiceName, Timestamp, ParentSpanId = '') AS root_service_explicit,
      argMin(ServiceName, Timestamp) AS root_service_fallback,
      arraySort(groupUniqArray(ServiceName)) AS services,
      countIf(StatusCode = 'STATUS_CODE_ERROR') AS has_error
    FROM base
    GROUP BY TraceId
    ORDER BY min(Timestamp) DESC
    LIMIT 200
  `;
}

function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}

function escapeSearchText(value: string): string {
  return escapeSqlString(value).replace(/%/g, "");
}
