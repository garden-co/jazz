import { useEffect, useMemo, useRef, useState } from "react";
import { BrowserRouter } from "react-router";
import { JazzClientProvider } from "jazz-tools/react";
import { createInspectorSessionClient } from "jazz-tools/_dev/inspector-client";
import {
  fetchSchemaHashes,
  fetchStoredPermissions,
  fetchStoredWasmSchema,
  type WasmSchema,
} from "jazz-tools";
import { DevtoolsProvider } from "../contexts/devtools-context.js";
import { StandaloneProvider } from "../contexts/standalone-context.js";
import { InspectorRoutes } from "../routes.js";
import { normalizeSchemaHashInfos, type SchemaHashInfo } from "../utility/schema-hash-display.js";
import {
  DashboardInspectorSession,
  receiveCliSession,
  type InspectorCapability,
  type InspectorSession,
} from "./browser-session.js";

type Client = Awaited<ReturnType<typeof createInspectorSessionClient>>;
export interface SessionConnection {
  appId: string;
  dashboard?: string;
  handoff?: string;
  launchCode?: string;
  restore?: boolean;
}
const metadataKey = "jazz-inspector-session-connection";

function persistConnectionMetadata(value: object): void {
  try {
    localStorage.setItem(metadataKey, JSON.stringify(value));
  } catch {
    // Quota or blocked storage cannot retain authority or make login fail.
    // Remove stale restorable metadata when replacing it with signed-out state fails.
    try {
      localStorage.removeItem(metadataKey);
    } catch {
      /* storage unavailable */
    }
  }
}

/** Persist connection metadata only. The trusted dashboard comes from the deployment. */
export function readSessionConnection(dashboard?: string): SessionConnection | null {
  const fragment = new URLSearchParams(window.location.hash.slice(1));
  const appId = fragment.get("appId");
  const handoff = fragment.get("handoff");
  const launchCode = fragment.get("launch");
  const cloud = fragment.get("login") === "dashboard";
  if (handoff || cloud) {
    window.history.replaceState(null, "", window.location.pathname);
    if (!appId || (!handoff && !dashboard)) return null;
    const connection = {
      appId,
      ...(handoff ? { handoff, launchCode: launchCode ?? "" } : { dashboard }),
    };
    if (!handoff) persistConnectionMetadata({ appId, mode: "dashboard" });
    return connection;
  }
  if (dashboard && !window.location.hash) {
    try {
      const stored = JSON.parse(localStorage.getItem(metadataKey) ?? "null");
      if (stored?.mode === "dashboard" && typeof stored.appId === "string")
        return { appId: stored.appId, dashboard, restore: stored.signedOut !== true };
    } catch {
      /* invalid connection metadata */
    }
  }
  return null;
}

export default function SessionApp({ connection }: { connection: SessionConnection }) {
  const auth = useMemo(
    () =>
      connection.dashboard
        ? new DashboardInspectorSession(
            connection.dashboard,
            connection.appId,
            new URL("/inspector/callback", window.location.origin).href,
          )
        : null,
    [connection.dashboard, connection.appId],
  );
  const [session, setSession] = useState<InspectorSession | null>(null);
  const [requested, setRequested] = useState<InspectorCapability[]>(["inspector:read"]);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [schemaHash, setSchemaHash] = useState<string | null>(null);
  const [catalogue, setCatalogue] = useState<SchemaHashInfo[]>([]);
  const [loaded, setLoaded] = useState<{
    client: Client;
    hash: string;
    schema: WasmSchema;
    permissions: Awaited<ReturnType<typeof fetchStoredPermissions>> | null;
  } | null>(null);
  const generation = useRef(0);
  const handoffAttempt = useRef<Promise<InspectorSession> | null>(null);
  const clientRef = useRef<Client | null>(null);
  const closed = useRef(new WeakSet<object>());
  const closeClient = (client: Client | null) => {
    if (client && !closed.current.has(client)) {
      closed.current.add(client);
      try {
        void Promise.resolve(client.shutdown()).catch(() => undefined);
      } catch {
        /* cleanup must still clear authority and the active view */
      }
    }
  };
  const clear = () => {
    closeClient(clientRef.current);
    clientRef.current = null;
    setLoaded(null);
    setSession(null);
  };
  const logout = () => {
    generation.current++;
    try {
      auth?.logout();
    } catch {
      /* local client cleanup must still happen */
    }
    clear();
    if (auth)
      persistConnectionMetadata({ appId: connection.appId, mode: "dashboard", signedOut: true });
    setBusy(false);
    setError(null);
  };

  useEffect(
    () => () => {
      generation.current++;
      auth?.logout();
      closeClient(clientRef.current);
    },
    [auth],
  );
  useEffect(() => {
    if (!connection.handoff) return;
    let active = true;
    const current = generation.current;
    handoffAttempt.current ??= receiveCliSession(
      connection.handoff,
      connection.appId,
      connection.launchCode ?? "",
    );
    setBusy(true);
    handoffAttempt.current
      .then(
        (value) => {
          if (active && current === generation.current) setSession(value);
        },
        () => {
          if (active && current === generation.current)
            setError("Inspector handoff failed or expired. Run the CLI again.");
        },
      )
      .finally(() => {
        if (active && current === generation.current) setBusy(false);
      });
    return () => {
      active = false;
    };
  }, [connection.handoff, connection.appId]);

  useEffect(() => {
    if (!auth || !connection.restore) return;
    let active = true;
    const current = generation.current;
    setBusy(true);
    void auth
      .renew()
      .then(
        (value) => {
          if (active && current === generation.current) setSession(value);
        },
        () => {
          if (active && current === generation.current) setError("Session restore needs sign-in.");
        },
      )
      .finally(() => {
        if (active && current === generation.current) setBusy(false);
      });
    return () => {
      active = false;
    };
  }, [auth, connection.restore]);

  const login = async () => {
    if (!auth) return;
    const current = ++generation.current;
    clear();
    setBusy(true);
    setError(null);
    try {
      const value = await auth.authorize(requested);
      if (current === generation.current) {
        persistConnectionMetadata({ appId: connection.appId, mode: "dashboard" });
        setSession(value);
      }
    } catch {
      if (current === generation.current)
        setError("Sign-in failed. Allow the popup and try again.");
    } finally {
      if (current === generation.current) setBusy(false);
    }
  };

  useEffect(() => {
    if (!session) return;
    let active = true;
    const expire = () => {
      if (active) {
        generation.current++;
        auth?.logout();
        clear();
        setError("Session ended. Sign in again.");
      }
    };
    const remaining = Math.max(0, session.expiresAt * 1000 - Date.now());
    const expiry = setTimeout(expire, Math.max(0, session.expiresAt * 1000 - Date.now()));
    const renewal = auth
      ? setTimeout(
          () => {
            void auth.renew(session.capabilities).then((value) => {
              if (active) {
                generation.current++;
                closeClient(clientRef.current);
                clientRef.current = null;
                setLoaded(null);
                setSession(value);
              }
            }, expire);
          },
          remaining - Math.min(60_000, remaining / 2),
        )
      : undefined;
    return () => {
      active = false;
      clearTimeout(expiry);
      clearTimeout(renewal);
    };
  }, [session, auth]);

  useEffect(() => {
    if (!session) return;
    let active = true;
    const current = generation.current;
    let owned: Client | null = null;
    const run = async () => {
      const authority = { appId: session.appId, inspectorToken: session.accessToken };
      const hashes = await fetchSchemaHashes(session.serverUrl, authority);
      const list = normalizeSchemaHashInfos(hashes.hashes, hashes.schemas);
      const selected =
        schemaHash && list.some((x) => x.hash === schemaHash) ? schemaHash : list[0]?.hash;
      if (!selected) throw new Error("No published schema");
      const schema = await fetchStoredWasmSchema(session.serverUrl, {
        ...authority,
        schemaHash: selected,
      });
      const permissions = await fetchStoredPermissions(session.serverUrl, authority);
      if (!active || current !== generation.current) return;
      owned = await createInspectorSessionClient({ ...authority, serverUrl: session.serverUrl });
      if (!active || current !== generation.current) {
        closeClient(owned);
        return;
      }
      clientRef.current = owned;
      setCatalogue(list);
      setLoaded({ client: owned, hash: selected, schema: schema.schema, permissions });
    };
    void run().catch(() => {
      if (active && current === generation.current) {
        generation.current++;
        clear();
        setError("Unable to open Inspector session. Sign in again.");
      }
    });
    return () => {
      active = false;
      closeClient(owned);
    };
  }, [session, schemaHash]);

  if (!session || !loaded)
    return (
      <main style={{ padding: 32 }}>
        <h1>Jazz Inspector</h1>
        <p>App: {connection.appId}</p>
        {error && <p role="alert">{error}</p>}
        {!session && auth && (
          <>
            <p>Read access is included. Additional permissions must be explicitly requested.</p>
            {(["inspector:edit", "inspector:admin"] as const).map((cap) => (
              <label key={cap} style={{ display: "block" }}>
                <input
                  type="checkbox"
                  checked={requested.includes(cap)}
                  onChange={(event) =>
                    setRequested((previous) =>
                      event.target.checked
                        ? [...previous, cap]
                        : previous.filter((value) => value !== cap),
                    )
                  }
                />
                {cap === "inspector:edit"
                  ? "Edit application data"
                  : "Publish schemas, permissions and migrations"}
              </label>
            ))}
            <button disabled={busy} onClick={() => void login()}>
              Sign in with dashboard
            </button>
          </>
        )}
        {(busy || session) && <p>Connecting…</p>}
        <button onClick={logout}>Log out</button>
      </main>
    );
  return (
    <JazzClientProvider client={loaded.client}>
      <div style={{ padding: 8 }}>
        {session.capabilities.includes("inspector:edit") ? "Editing enabled" : "Read-only session"}{" "}
        · {session.capabilities.join(", ")} <button onClick={logout}>Log out</button>
      </div>
      <DevtoolsProvider
        wasmSchema={loaded.schema}
        storedPermissions={loaded.permissions}
        runtime="standalone"
        readOnly={!session.capabilities.includes("inspector:edit")}
      >
        <StandaloneProvider
          onManageConnections={logout}
          schemaHashes={catalogue}
          selectedSchemaHash={loaded.hash}
          onSelectSchema={(hash) => {
            generation.current++;
            closeClient(clientRef.current);
            clientRef.current = null;
            setLoaded(null);
            setSchemaHash(hash);
          }}
          isSwitchingSchema={false}
          connection={{
            serverUrl: session.serverUrl,
            appId: session.appId,
            inspectorToken: session.accessToken,
          }}
        >
          <BrowserRouter>
            <InspectorRoutes />
          </BrowserRouter>
        </StandaloneProvider>
      </DevtoolsProvider>
    </JazzClientProvider>
  );
}
