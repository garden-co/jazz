// The parts of the Inspector only a backend-served Inspector has: who you
// are and whose permissions you view with, the backend's sessions, and the
// exact rows a deep link points at.

import type { WasmSchema } from "jazz-tools";
import {
  fromJsonForColumn,
  type InspectAs,
  type InspectExplainAnswer,
  type InspectQuery,
  type InspectRowsAnswer,
  type InspectSessionInfo,
} from "jazz-tools/inspect";
import { useEffect, useState } from "react";
import { Link } from "react-router";
import { useDevtoolsContext } from "../contexts/devtools-context.js";
import { stringifyForPresentation } from "../utility/presentation-serialization.js";
import { decodeRow } from "./api-data.js";
import { asForApi, useBackendContext } from "./backend-context.js";
import styles from "./backend.module.css";

const VIA_LABEL = { hook: "app sign-in", token: "agent token", dev: "local development" } as const;

function claimsText(claims: Record<string, unknown> | undefined): string {
  return claims === undefined ? "" : JSON.stringify(claims);
}

/** The claims a person typed: undefined for none, null when it isn't a JSON object. */
function parseClaims(text: string): Record<string, unknown> | undefined | null {
  if (!text.trim()) return undefined;
  try {
    const parsed = JSON.parse(text) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

export function BackendIdentity() {
  const backend = useBackendContext();
  const [draft, setDraft] = useState(backend?.viewAs?.userId ?? "");
  const [claimsDraft, setClaimsDraft] = useState(claimsText(backend?.viewAs?.claims));
  useEffect(() => {
    setDraft(backend?.viewAs?.userId ?? "");
    setClaimsDraft(claimsText(backend?.viewAs?.claims));
  }, [backend?.viewAs]);
  if (!backend) return null;
  const { whoami } = backend;
  const claims = parseClaims(claimsDraft);
  const apply = () => {
    const userId = draft.trim();
    if (!userId) return backend.setViewAs(null);
    if (claims === null) return;
    backend.setViewAs(claims === undefined ? { userId } : { userId, claims });
  };
  const missingClaims =
    backend.permissionsReadClaims && backend.viewAs !== null && !backend.viewAs.claims;
  return (
    <>
      <form
        className={styles.viewAs}
        onSubmit={(event) => {
          event.preventDefault();
          apply();
        }}
      >
        <label htmlFor="jazz-inspect-view-as">View as user</label>
        <input
          id="jazz-inspect-view-as"
          className={styles.viewAsInput}
          value={draft}
          placeholder="everything (backend)"
          onChange={(event) => setDraft(event.target.value)}
          onBlur={apply}
        />
        {draft.trim() ? (
          <>
            <label htmlFor="jazz-inspect-view-as-claims">with claims</label>
            <input
              id="jazz-inspect-view-as-claims"
              className={`${styles.viewAsInput} ${claims === null ? styles.viewAsInvalid : ""}`}
              value={claimsDraft}
              placeholder='{"role":"manager"}'
              aria-invalid={claims === null}
              title={claims === null ? "Claims must be a JSON object" : undefined}
              onChange={(event) => setClaimsDraft(event.target.value)}
              onBlur={apply}
            />
          </>
        ) : null}
        {missingClaims ? (
          <span
            className={`${styles.badge} ${styles.badgeWarn}`}
            role="status"
            title="This app's permissions read session claims, and none are given for this user, so you see what a session with no claims would: rows or writes a claim would allow are missing or refused."
          >
            no claims
          </span>
        ) : null}
      </form>
      <span className={styles.identity} title={`Signed in through ${VIA_LABEL[whoami.via]}`}>
        {whoami.by}
        <span className={`${styles.badge} ${whoami.write ? styles.badgeWarn : ""}`}>
          {whoami.write ? "can write" : "read-only"}
        </span>
      </span>
    </>
  );
}

function formatValue(value: unknown): string {
  if (value === null) return "null";
  if (value === undefined) return "";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "string") return value;
  return stringifyForPresentation(value, undefined, { bigint: "raw" });
}

export function BackendSessions() {
  const backend = useBackendContext();
  const [answer, setAnswer] = useState<{ scope: string; sessions: InspectSessionInfo[] } | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    if (!backend) return;
    let stopped = false;
    const load = () =>
      backend.call<{ scope: string; sessions: InspectSessionInfo[] }>({ op: "sessions" }).then(
        (next) => {
          if (!stopped) {
            setAnswer(next);
            setError(null);
          }
        },
        (failure: Error) => !stopped && setError(failure.message),
      );
    void load();
    const interval = setInterval(load, 5_000);
    return () => {
      stopped = true;
      clearInterval(interval);
    };
  }, [backend]);
  return (
    <div className={styles.page}>
      <h1 className={styles.heading}>Sessions</h1>
      <p className={styles.note}>
        Sync sessions open on the backend instance that answered ({answer?.scope ?? "…"}). A backend
        that runs as many instances shows only the ones on this instance.
      </p>
      {error ? <p className={styles.error}>{error}</p> : null}
      {answer && answer.sessions.length === 0 ? <p className={styles.note}>None open.</p> : null}
      {answer && answer.sessions.length > 0 ? (
        <table className={styles.table}>
          <thead>
            <tr>
              <th>User</th>
              <th>Kind</th>
              <th>Transport</th>
              <th>Opened</th>
            </tr>
          </thead>
          <tbody>
            {answer.sessions.map((session, index) => (
              <tr key={index}>
                <td>{session.userId ?? "—"}</td>
                <td>{session.kind}</td>
                <td>{session.transport}</td>
                <td>{session.openedAt}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}
    </div>
  );
}

/** The data explorer's URL for a linked query's table and filters, when its filters fit there. */
export function explorerHrefFor(query: InspectQuery, schema: WasmSchema): string | null {
  const table = schema[query.table];
  if (!table) return null;
  const columns = new Map(table.columns.map((column) => [column.name, column]));
  const filters: Array<{ id: string; column: string; operator: string; value: unknown }> = [];
  for (const [column, condition] of Object.entries(query.where ?? {})) {
    const type = columns.get(column)?.column_type;
    const entries =
      typeof condition === "object" &&
      condition !== null &&
      !Array.isArray(condition) &&
      !("$bytes" in condition)
        ? Object.entries(condition as Record<string, unknown>)
        : [["eq", condition] as const];
    for (const [operator, value] of entries) {
      filters.push({
        id: `link-${column}-${operator}`,
        column,
        operator,
        value: fromJsonForColumn(type, value),
      });
    }
  }
  const params = new URLSearchParams();
  if (filters.length) {
    params.set(
      "filters",
      JSON.stringify(filters, (_key, value) =>
        typeof value === "bigint" ? value.toString() : value,
      ),
    );
  }
  const [sort] = query.orderBy ?? [];
  if (sort) {
    params.set("sort", sort[0]);
    params.set("dir", sort[1] === "desc" ? "DESC" : "ASC");
  }
  const search = params.toString();
  return `/data-explorer/${encodeURIComponent(query.table)}/data${search ? `?${search}` : ""}`;
}

/** Why the user viewed as can or can't read each row of `query`: the backend's `explain`. */
function ExplainRows({ query, as }: { query: InspectQuery; as: InspectAs }) {
  const backend = useBackendContext();
  const [answer, setAnswer] = useState<InspectExplainAnswer | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  if (!backend) return null;
  const run = () => {
    setLoading(true);
    setError(null);
    backend.call<InspectExplainAnswer>({ op: "explain", query, as }).then(
      (next) => {
        setAnswer(next);
        setLoading(false);
      },
      (failure: Error) => {
        setError(failure.message);
        setLoading(false);
      },
    );
  };
  const clause = (index: number) => {
    const text = answer?.clauses[index]?.text;
    return text ? `${index}: ${text}` : `clause ${index}`;
  };
  return (
    <section aria-label="Explain">
      <button type="button" onClick={run} disabled={loading}>
        {answer ? "Explain again" : "Explain why"}
      </button>
      {error ? <p className={styles.error}>{error}</p> : null}
      {answer ? (
        <>
          {answer.warning ? <p className={styles.error}>{answer.warning}</p> : null}
          {answer.note ? <p className={styles.note}>{answer.note}</p> : null}
          {answer.clauses.length ? (
            <ol start={0} className={styles.note} aria-label="Read policy clauses">
              {answer.clauses.map((entry) => (
                <li key={entry.index}>{entry.text ?? `clause ${entry.index}`}</li>
              ))}
            </ol>
          ) : null}
          <table className={styles.table}>
            <thead>
              <tr>
                <th>id</th>
                <th>{answer.as} may read</th>
                <th>because</th>
              </tr>
            </thead>
            <tbody>
              {answer.rows.map((row) => (
                <tr key={row.id}>
                  <td>{row.id}</td>
                  <td>{row.allowed ? "yes" : "no"}</td>
                  <td>
                    {row.allowed
                      ? (row.by?.map(clause).join("; ") ?? "not shown to this token")
                      : answer.clauses.length
                        ? "no clause holds"
                        : "no read policy"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {answer.truncated ? (
            <p className={styles.note}>More rows matched; raise the limit to explain them.</p>
          ) : null}
        </>
      ) : null}
    </section>
  );
}

export function LinkedQuery() {
  const backend = useBackendContext();
  const { wasmSchema } = useDevtoolsContext();
  const [answer, setAnswer] = useState<InspectRowsAnswer | null>(null);
  const [error, setError] = useState<string | null>(null);
  const query = backend?.link?.query;
  const viewAs = backend?.viewAs ?? null;
  const as = asForApi(viewAs);
  const asKey = JSON.stringify(as ?? null);
  useEffect(() => {
    if (!backend || !query) return;
    let stopped = false;
    backend.call<InspectRowsAnswer>({ op: "query", query, ...(as ? { as } : {}) }).then(
      (next) => !stopped && setAnswer(next),
      (failure: Error) => !stopped && setError(failure.message),
    );
    return () => {
      stopped = true;
    };
    // `asKey` captures `as`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [backend, query, asKey]);
  if (!backend || !query)
    return <p className={styles.note}>This Inspector was opened without a query.</p>;
  const sourceTable = query.table.startsWith("history.") ? query.table.slice(8) : query.table;
  const columns = new Map(
    (wasmSchema[sourceTable]?.columns ?? []).map((column) => [column.name, column]),
  );
  const rows = answer?.rows.map((row) => decodeRow(columns, row)) ?? [];
  const keys = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const explorerHref = explorerHrefFor(query, wasmSchema);
  return (
    <div className={styles.page}>
      <h1 className={styles.heading}>Linked query</h1>
      <p className={styles.note}>
        Exactly the rows this link&apos;s query matches
        {viewAs ? `, as ${viewAs.userId} sees them` : ", with the backend's authority"}.{" "}
        {explorerHref ? (
          <Link to={explorerHref}>Open {query.table} in the data explorer</Link>
        ) : null}
      </p>
      <pre className={styles.code}>{JSON.stringify(query, null, 2)}</pre>
      {error ? <p className={styles.error}>{error}</p> : null}
      {answer?.warning ? <p className={styles.error}>{answer.warning}</p> : null}
      {as && !query.table.startsWith("history.") ? <ExplainRows query={query} as={as} /> : null}
      {answer ? (
        <>
          <p className={styles.note}>
            {rows.length} row{rows.length === 1 ? "" : "s"}
            {answer.truncated ? " (more matched; raise the limit to see them)" : ""}
          </p>
          <table className={styles.table}>
            <thead>
              <tr>
                {keys.map((key) => (
                  <th key={key}>{key}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={`${row.id ?? ""}-${index}`}>
                  {keys.map((key) => (
                    <td key={key}>{formatValue(row[key])}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </>
      ) : error ? null : (
        <p className={styles.note}>Loading…</p>
      )}
    </div>
  );
}
