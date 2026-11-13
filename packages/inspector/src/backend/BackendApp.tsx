// The Inspector an app's backend serves at `<basePath>/inspect/`. It reads
// and writes through that backend's Inspector API, signed in however the
// app's `inspect` hook admits people (or locally in development), and opens
// on whatever a deep link names: `#q=` (a query), `#table=`, `#row=`, `#as=`,
// or `#grant=` (an agent's request for a token).

import type { TablePolicies, WasmSchema } from "jazz-tools";
import {
  decodeGrant,
  inspectorLink,
  parseInspectorLink,
  readsClaims,
  type InspectWhoami,
  type InspectorLinkTarget,
} from "jazz-tools/inspect";
import { useEffect, useMemo, useRef, useState } from "react";
import { MemoryRouter } from "react-router";
import appStyles from "../App.module.css";
import { DevtoolsProvider } from "../contexts/devtools-context.js";
import { InspectorDataProvider } from "../contexts/inspector-data.js";
import { buildRelationFilterHref } from "../components/data-explorer/relation-navigation.js";
import { InspectorRoutes } from "../routes.js";
import { apiUrlFor, createInspectorCall, InspectorApiError, type InspectorCall } from "./api.js";
import { createApiDataSource } from "./api-data.js";
import { ApprovalPage } from "./ApprovalPage.js";
import {
  BackendProvider,
  asForApi,
  type BackendContextValue,
  type ViewAs,
} from "./backend-context.js";

interface SchemaAnswer {
  tables: Record<string, WasmSchema[string] & { redacted?: string[] }>;
  permissions?: Record<string, TablePolicies>;
}

type Boot =
  | { kind: "loading" }
  | { kind: "refused"; message: string }
  | { kind: "failed"; message: string }
  | {
      kind: "ready";
      whoami: InspectWhoami;
      schema: WasmSchema;
      permissions: Record<string, TablePolicies> | null;
    };

/** Where a deep link opens the data explorer. */
export function initialRouteFor(link: InspectorLinkTarget, schema: WasmSchema): string {
  if (link.query) return "/linked";
  const table = link.table;
  if (table && Object.hasOwn(schema, table)) {
    return link.row ? buildRelationFilterHref(table, link.row) : `/data-explorer/${table}/data`;
  }
  return "/data-explorer";
}

/**
 * Keep the address bar's fragment on whom the page views as (`#as=…&claims=…`),
 * so a copied address opens the same view. replaceState: no reload, no new
 * history entry.
 */
function keepViewAsInAddress(viewAs: ViewAs | null): void {
  if (typeof window === "undefined") return;
  const { grant: _grant, ...target } = parseInspectorLink(window.location.hash);
  const base = window.location.href.split("#")[0]!;
  const as = asForApi(viewAs);
  const { as: _as, ...rest } = target;
  window.history.replaceState(
    null,
    "",
    inspectorLink(base, as === undefined ? rest : { ...rest, as }),
  );
}

export function BackendApp({
  pageUrl = window.location.href,
  call: givenCall,
}: {
  pageUrl?: string;
  call?: InspectorCall;
}) {
  const call = useMemo(
    () => givenCall ?? createInspectorCall(apiUrlFor(pageUrl)),
    [givenCall, pageUrl],
  );
  const fragment = useMemo(() => new URL(pageUrl).hash, [pageUrl]);
  const link = useMemo(() => parseInspectorLink(fragment), [fragment]);
  const [grantText, setGrantText] = useState(link.grant ?? null);
  const grant = useMemo(() => (grantText ? decodeGrant(grantText) : null), [grantText]);
  const [boot, setBoot] = useState<Boot>({ kind: "loading" });
  const [viewAs, setViewAsState] = useState<ViewAs | null>(
    typeof link.as === "string" ? { userId: link.as } : (link.as ?? null),
  );
  const [warning, setWarning] = useState<string | null>(null);
  const viewAsRef = useRef(viewAs);
  viewAsRef.current = viewAs;

  // Each deep link opens its own Inspector: pasting one into this tab starts over on it.
  useEffect(() => {
    const restart = () => window.location.reload();
    window.addEventListener("hashchange", restart);
    return () => window.removeEventListener("hashchange", restart);
  }, []);

  useEffect(() => {
    let stopped = false;
    void (async () => {
      try {
        const whoami = await call<InspectWhoami>({ op: "whoami" });
        const answer = await call<SchemaAnswer>({ op: "schema" });
        const schema: WasmSchema = {};
        for (const [name, table] of Object.entries(answer.tables)) {
          const { redacted: _redacted, ...rest } = table;
          schema[name] = rest as WasmSchema[string];
        }
        if (!stopped)
          setBoot({ kind: "ready", whoami, schema, permissions: answer.permissions ?? null });
      } catch (error) {
        if (stopped) return;
        if (error instanceof InspectorApiError && (error.status === 401 || error.status === 403)) {
          setBoot({ kind: "refused", message: error.message });
        } else {
          setBoot({
            kind: "failed",
            message: error instanceof Error ? error.message : String(error),
          });
        }
      }
    })();
    return () => {
      stopped = true;
    };
  }, [call]);

  const data = useMemo(
    () =>
      boot.kind === "ready"
        ? createApiDataSource({
            call,
            schema: boot.schema,
            as: () => asForApi(viewAsRef.current),
            onWarning: setWarning,
          })
        : null,
    [boot, call],
  );

  if (boot.kind === "loading") {
    return (
      <main className={appStyles.statePage}>
        <section className={appStyles.stateCard}>
          <p className={appStyles.loadingText}>Loading…</p>
        </section>
      </main>
    );
  }
  if (boot.kind === "refused" || boot.kind === "failed") {
    return (
      <main className={appStyles.statePage}>
        <section className={appStyles.stateCard}>
          <h2 className={appStyles.stateTitle}>
            {boot.kind === "refused" ? "Not allowed in" : "The Inspector couldn't start"}
          </h2>
          <p role="alert" className={appStyles.errorText}>
            {boot.message}
          </p>
          {boot.kind === "refused" ? (
            <p className={appStyles.loadingText}>
              This app's backend decides who may use its Inspector, in the `inspect` option of
              createJazz. Sign in the way it expects (for example through your hosting provider's
              login), then reload.
            </p>
          ) : null}
        </section>
      </main>
    );
  }

  if (grantText !== null) {
    if (!grant) {
      return (
        <main className={appStyles.statePage}>
          <section className={appStyles.stateCard}>
            <h2 className={appStyles.stateTitle}>Damaged approval link</h2>
            <p className={appStyles.errorText}>
              This approval link is damaged. Ask the agent for a new one.
            </p>
          </section>
        </main>
      );
    }
    return (
      <ApprovalPage
        grant={grant}
        whoami={boot.whoami}
        call={call}
        endpoint={new URL(boot.whoami.inspectorUrl).href.replace(/\/inspect\/?$/, "")}
        onDone={() => {
          history.replaceState(null, "", window.location.pathname);
          setGrantText(null);
        }}
      />
    );
  }

  const value: BackendContextValue = {
    call,
    whoami: boot.whoami,
    data: data!,
    viewAs,
    setViewAs: (next) => {
      if (JSON.stringify(next) === JSON.stringify(viewAsRef.current)) return;
      viewAsRef.current = next;
      setViewAsState(next);
      setWarning(null);
      keepViewAsInAddress(next);
      data!.refresh();
    },
    warning,
    permissionsReadClaims: readsClaims(boot.permissions),
    link: link.query || link.table ? link : null,
  };
  return (
    <BackendProvider value={value}>
      <InspectorDataProvider source={data!}>
        <DevtoolsProvider
          wasmSchema={boot.schema}
          backendPermissions={boot.permissions}
          runtime="backend"
        >
          <MemoryRouter initialEntries={[initialRouteFor(link, boot.schema)]}>
            <InspectorRoutes />
          </MemoryRouter>
        </DevtoolsProvider>
      </InspectorDataProvider>
    </BackendProvider>
  );
}
