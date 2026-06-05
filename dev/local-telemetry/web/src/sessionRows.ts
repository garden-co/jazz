export type SyncLogSqlFilters = {
  minutes: number;
  limit: number;
};

export type SyncLogRow = {
  SessionId?: string;
  LogId?: string;
  Timestamp?: string;
  LogTimeUnixNano?: string | number;
  TimeUnixNano?: string | number;
  ObservedTimeUnixNano?: string | number;
  ServiceName?: string;
  EventName?: string;
  Body?: string;
  SeverityText?: string;
  Thread?: string;
  payload?: string;
  peer_kind?: string;
  peer_id?: string;
  tier?: string;
  sync_direction?: string;
  sync_message_kind?: string;
  sync_message_index?: string | number;
  sync_message_count?: string | number;
  sync_connection_id?: string;
  sync_subscription_count?: string | number;
  sync_data_record_count?: string | number;
  sync_read_record_count?: string | number;
  sync_bundle_row_count?: string | number;
  sync_bundle_tx_count?: string | number;
  sync_tx_id?: string;
  sync_tx_status?: string;
  sync_tx_rejection_code?: string;
  sync_tx_global_epoch?: string;
  sync_tx_conflict_mode?: string;
  sync_branch_id?: string;
  sync_subscription_id?: string;
  sync_message_id?: string;
  sync_cursor?: string;
  sync_settlement_tier?: string;
  sync_error_code?: string;
  sync_retry_hint?: string;
  sync_close_reason?: string;
  sync_data_records?: string;
  sync_read_records?: string;
  sync_bundle_tx_ids?: string;
  sync_probe_id?: string;
  sync_operation?: string;
  sync_table?: string;
  sync_row_id?: string;
  sync_origin_browser_id?: string;
  sync_phase?: string;
};

export function buildSessionListSql(filters: SyncLogSqlFilters): string {
  const minutes = Math.max(1, Math.floor(filters.minutes) || 1);
  const limit = Math.max(1, Math.floor(filters.limit) || 100);
  const cutoffNs = (Date.now() - minutes * 60_000) * 1_000_000;
  const logTime = logTimeExpr();

  return sessionRowsSql({
    rawWhere: [`${logTime} > ${cutoffNs}`, relevantSessionPredicate()],
    rowWhere: [`SessionId <> ''`],
    orderBy: "LogTimeUnixNano DESC",
    limit,
  });
}

export function buildSessionDetailSql(sessionId: string): string {
  return sessionRowsSql({
    rawWhere: [relevantSessionPredicate()],
    rowWhere: [`SessionId = '${escapeSqlString(sessionId)}'`],
    orderBy: "LogTimeUnixNano ASC",
    limit: 1000,
  });
}

function sessionRowsSql(options: {
  rawWhere: string[];
  rowWhere: string[];
  orderBy: string;
  limit: number;
}): string {
  const sessionId = fieldAttr("sync.session_id");
  const eventName = `COALESCE(
    json_extract_string(raw_record, '$.eventName'),
    json_extract_string(raw_record, '$.event_name'),
    ${fieldAttr("sync.event")}
  )`;
  const logTime = logTimeExpr();

  return `
    SELECT *
    FROM (
      SELECT
        ${sessionId} AS SessionId,
        CONCAT(
          COALESCE(${logTime}::VARCHAR, ''),
          '-',
          COALESCE(observed_time_unix_nano::VARCHAR, ''),
          '-',
          COALESCE(service_name, ''),
          '-',
          COALESCE(${eventName}, '')
        ) AS LogId,
        strftime(to_timestamp(${logTime} / 1e9), '%Y-%m-%dT%H:%M:%S.%gZ') AS Timestamp,
        ${logTime} AS LogTimeUnixNano,
        time_unix_nano,
        observed_time_unix_nano,
        time_unix_nano AS TimeUnixNano,
        observed_time_unix_nano AS ObservedTimeUnixNano,
        service_name AS ServiceName,
        severity_text AS SeverityText,
        ${eventName} AS EventName,
        COALESCE(
          json_extract_string(body, '$.stringValue'),
          json_extract_string(body, '$.intValue'),
          json_extract_string(body, '$.doubleValue'),
          json_extract_string(body, '$.boolValue'),
          body::VARCHAR
        ) AS Body,
        ${fieldAttr("jazz.runtime_thread")} AS Thread,
        ${fieldAttr("payload")} AS payload,
        ${fieldAttr("peer_kind")} AS peer_kind,
        ${fieldAttr("peer_id")} AS peer_id,
        ${fieldAttr("tier")} AS tier,
        ${fieldAttr("sync.direction")} AS sync_direction,
        ${fieldAttr("sync.message_kind")} AS sync_message_kind,
        ${fieldAttr("sync.message_index")} AS sync_message_index,
        ${fieldAttr("sync.message_count")} AS sync_message_count,
        ${fieldAttr("sync.connection_id")} AS sync_connection_id,
        ${fieldAttr("sync.subscription_count")} AS sync_subscription_count,
        ${fieldAttr("sync.data_record_count")} AS sync_data_record_count,
        ${fieldAttr("sync.read_record_count")} AS sync_read_record_count,
        ${fieldAttr("sync.bundle_row_count")} AS sync_bundle_row_count,
        ${fieldAttr("sync.bundle_tx_count")} AS sync_bundle_tx_count,
        ${fieldAttr("sync.tx_id")} AS sync_tx_id,
        ${fieldAttr("sync.tx_status")} AS sync_tx_status,
        ${fieldAttr("sync.tx_rejection_code")} AS sync_tx_rejection_code,
        ${fieldAttr("sync.tx_global_epoch")} AS sync_tx_global_epoch,
        ${fieldAttr("sync.tx_conflict_mode")} AS sync_tx_conflict_mode,
        ${fieldAttr("sync.branch_id")} AS sync_branch_id,
        ${fieldAttr("sync.subscription_id")} AS sync_subscription_id,
        ${fieldAttr("sync.message_id")} AS sync_message_id,
        ${fieldAttr("sync.cursor")} AS sync_cursor,
        ${fieldAttr("sync.settlement_tier")} AS sync_settlement_tier,
        ${fieldAttr("sync.error_code")} AS sync_error_code,
        ${fieldAttr("sync.retry_hint")} AS sync_retry_hint,
        ${fieldAttr("sync.close_reason")} AS sync_close_reason,
        ${fieldAttr("sync.data_records")} AS sync_data_records,
        ${fieldAttr("sync.read_records")} AS sync_read_records,
        ${fieldAttr("sync.bundle_tx_ids")} AS sync_bundle_tx_ids,
        ${fieldAttr("sync.probe.id")} AS sync_probe_id,
        ${fieldAttr("sync.operation")} AS sync_operation,
        ${fieldAttr("sync.table")} AS sync_table,
        ${fieldAttr("sync.row_id")} AS sync_row_id,
        ${fieldAttr("sync.origin_browser_id")} AS sync_origin_browser_id,
        ${fieldAttr("sync.phase")} AS sync_phase
      FROM logs
      WHERE ${options.rawWhere.join(" AND ")}
    ) AS session_logs
    WHERE ${options.rowWhere.join(" AND ")}
    ORDER BY ${options.orderBy}
    LIMIT ${Math.max(1, Math.floor(options.limit) || 100)}
  `;
}

function logTimeExpr(): string {
  return "COALESCE(time_unix_nano, observed_time_unix_nano)";
}

function relevantSessionPredicate(): string {
  const eventName = `COALESCE(
    json_extract_string(raw_record, '$.eventName'),
    json_extract_string(raw_record, '$.event_name'),
    ${fieldAttr("sync.event")}
  )`;
  return `(
    service_name IN (
      'mini-sqlite-todo-yew-browser',
      'mini-sqlite-todo-yew-server'
    )
    OR ${eventName} IN (
      'sync.message',
      'sync.frame.sent',
      'sync.frame.received',
      'sync.frame.applied',
      'todo.action.start',
      'todo.remote_subscription_delta',
      'server.upload_bundle_export',
      'server.change_broadcast_plan'
    )
    OR scope_name LIKE 'mini_sqlite_todo_yew%'
    OR ${fieldAttr("sync.session_id")} <> ''
  )`;
}

function fieldAttr(key: string): string {
  const underscoredKey = key.replace(/\./g, "_");
  const dotted = attr(key);
  if (underscoredKey === key) {
    return `COALESCE(${dotted}, json_extract_string(${attr("jazz.log.fields")}, '$.${key}'))`;
  }

  return `COALESCE(
    ${dotted},
    ${attr(underscoredKey)},
    json_extract_string(${attr("jazz.log.fields")}, '$.${key}'),
    json_extract_string(${attr("jazz.log.fields")}, '$.${underscoredKey}')
  )`;
}

// Pulls a single attribute value out of the OTLP-shaped `attributes` JSON array
// on a log row, falling back across the common value variants.
function attr(key: string): string {
  return `(
    SELECT COALESCE(
      json_extract_string(a, '$.value.stringValue'),
      json_extract_string(a, '$.value.intValue'),
      json_extract_string(a, '$.value.doubleValue'),
      json_extract_string(a, '$.value.boolValue')
    )
    FROM UNNEST(attributes::JSON[]) AS u(a)
    WHERE json_extract_string(a, '$.key') = '${escapeSqlString(key)}'
    LIMIT 1
  )`;
}

function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}
