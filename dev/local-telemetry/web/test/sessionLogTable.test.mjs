import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const { buildSessionDetailSql, buildSessionListSql } = await import("../.test-build/sessionRows.js");
const { buildSessionSummaries } = await import("../.test-build/sessionModel.js");
const { parseSessionRoute, sessionDetailHash, sessionListHash } =
  await import("../.test-build/route.js");

const rows = [
  {
    SessionId: "session-a",
    LogId: "log-1",
    Timestamp: "2026-06-04T10:00:00.010Z",
    TimeUnixNano: "1780567200010000000",
    ServiceName: "mini-sqlite-todo-yew-browser",
    EventName: "todo.action.start",
    Body: "{\"event\":\"todo.action.start\",\"operation\":\"insert\",\"title\":\"Buy milk\"}",
    sync_operation: "insert",
    sync_table: "todos",
    sync_row_id: "todo-42",
  },
  {
    SessionId: "session-a",
    LogId: "log-2",
    Timestamp: "2026-06-04T10:00:00.020Z",
    TimeUnixNano: "1780567200020000000",
    ServiceName: "mini-sqlite-todo-yew-server",
    EventName: "sync.message",
    Body: "{\"event\":\"sync.message\",\"message\":{\"Data\":{\"bundle\":{\"rows\":[{\"values\":{\"title\":\"Buy milk\"}}]}}}}",
    sync_direction: "server.send",
    sync_message_kind: "server.data",
    sync_data_records: "todos:todo-42:insert",
  },
  {
    SessionId: "session-b",
    LogId: "log-3",
    Timestamp: "2026-06-04T10:00:01.000Z",
    TimeUnixNano: "1780567201000000000",
    ServiceName: "mini-sqlite-todo-yew-browser",
    EventName: "todo.action.start",
    Body: "{\"event\":\"todo.action.start\",\"operation\":\"delete\"}",
    sync_operation: "delete",
    sync_table: "todos",
    sync_row_id: "todo-99",
  },
];

const summaries = buildSessionSummaries(rows);
assert.equal(summaries.length, 2);
assert.equal(summaries[0].sessionId, "session-b");
assert.equal(summaries[0].title, "delete todos:todo-99");
assert.equal(summaries[0].eventCount, 1);
assert.equal(summaries[1].sessionId, "session-a");
assert.equal(summaries[1].title, "insert todos:todo-42");
assert.equal(summaries[1].eventCount, 2);

assert.deepEqual(parseSessionRoute(""), { page: "sessions" });
assert.deepEqual(parseSessionRoute("#/sessions"), { page: "sessions" });
assert.deepEqual(parseSessionRoute("#/sessions/session-a"), {
  page: "session",
  sessionId: "session-a",
});
assert.equal(sessionListHash(), "#/sessions");
assert.equal(sessionDetailHash("session/with spaces"), "#/sessions/session%2Fwith%20spaces");

const listSql = buildSessionListSql({ minutes: 15, limit: 25 });
assert.match(listSql, /FROM logs/);
assert.match(listSql, /COALESCE\(time_unix_nano, observed_time_unix_nano\) > \d+/);
assert.match(listSql, /SessionId <> ''/);
assert.match(listSql, /ORDER BY LogTimeUnixNano DESC/);
assert.match(listSql, /LIMIT 25/);
assert.match(listSql, /AS Body/);
assert.match(listSql, /COALESCE\(time_unix_nano, observed_time_unix_nano\) AS LogTimeUnixNano/);
assert.match(
  listSql,
  /strftime\(to_timestamp\(COALESCE\(time_unix_nano, observed_time_unix_nano\) \/ 1e9\)/,
);
assert.match(listSql, /sync\.session_id/);
assert.match(listSql, /sync_message_kind/);
assert.doesNotMatch(listSql, /FROM spans/);
assert.doesNotMatch(listSql, /trace_id/);

const detailSql = buildSessionDetailSql("session-'quoted");
assert.match(detailSql, /FROM logs/);
assert.match(detailSql, /SessionId = 'session-''quoted'/);
assert.match(detailSql, /ORDER BY LogTimeUnixNano ASC/);
assert.doesNotMatch(detailSql, /FROM spans/);
assert.doesNotMatch(detailSql, /trace_id/);

const appSource = readFileSync(new URL("../src/App.tsx", import.meta.url), "utf8");
assert.match(appSource, /parseSessionRoute/);
assert.match(appSource, /SessionDetailPage/);
assert.match(appSource, /SessionListPage/);
assert.doesNotMatch(appSource, /FlowList/);

const explorerSource = readFileSync(
  new URL("../src/SessionExplorer.tsx", import.meta.url),
  "utf8",
);
assert.match(explorerSource, /Sync Sessions/);
assert.match(explorerSource, /Log body/);
assert.match(explorerSource, /buildSessionSummaries/);
assert.match(explorerSource, /SessionDetailPage/);
assert.match(explorerSource, /buildSessionDetailSql/);
assert.doesNotMatch(explorerSource, /layoutSessionEvents/);

for (const sourcePath of ["src/main.tsx", "src/SessionExplorer.tsx"]) {
  const source = readFileSync(new URL(`../${sourcePath}`, import.meta.url), "utf8");
  assert.doesNotMatch(
    source,
    /refetchInterval/,
    `${sourcePath} should not configure automatic query reloads`,
  );
}

console.log("session log table tests passed");
