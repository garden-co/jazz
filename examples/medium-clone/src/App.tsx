import * as React from "react";
import {
  JazzProvider,
  attachDevTools,
  useDb,
  useJazzClient,
  useLocalFirstAuth,
  useSession,
} from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { app } from "../schema.js";
import { FrontPage } from "./FrontPage.js";
import { ArticleEditor } from "./ArticleEditor.js";
import { ArticleView } from "./ArticleView.js";
import { useRoute } from "./router.js";
import { logEvent, shortId, telemetryCollectorUrl } from "./telemetry.js";

const devToolsAttachedClients = new WeakSet<object>();

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;

function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  return {
    appId,
    env: "dev",
    userBranch: "main",
    serverUrl,
    telemetryCollectorUrl,
    devMode: true,
    secret,
    ...overrides,
  };
}

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: React.ReactNode;
};

function DevToolsRegistration() {
  const client = useJazzClient();
  React.useEffect(() => {
    if (devToolsAttachedClients.has(client as object)) return;
    void attachDevTools(client, app.wasmSchema);
    devToolsAttachedClients.add(client as object);
    if (location.origin.includes("localhost")) {
      Object.defineProperty(window, "jazzClient", { value: client, writable: true });
    }
  }, [client]);
  return null;
}

function shortUser(id: string | null | undefined) {
  if (!id) return "anonymous";
  return id.slice(0, 8);
}

function Header({ onLogoClick }: { onLogoClick: () => void }) {
  const session = useSession();
  return (
    <header className="site">
      <a
        className="wordmark"
        href="#/"
        onClick={(event) => {
          event.preventDefault();
          onLogoClick();
        }}
      >
        Medium-ish
      </a>
      <span className="who">
        signed in as <code>{shortUser(session?.user_id)}</code>
      </span>
    </header>
  );
}

function Inner() {
  const [route, navigate] = useRoute();
  const session = useSession();
  const db = useDb();

  React.useEffect(
    () =>
      db.onMutationError((event) => {
        logEvent("medium.jazz.mutation_error", {
          "batch.id": shortId(event.batch.batchId),
          "batch.mode": event.batch.mode,
          "batch.settlement": event.batch.latestSettlement?.kind ?? "",
        });
      }),
    [db],
  );

  React.useEffect(() => {
    logEvent("medium.route.changed", {
      "route.name": route.name,
      "route.article_id": route.name === "view" ? shortId(route.articleId) : "",
      "route.draft_id": route.name === "edit" ? shortId(route.draftId) : "",
      "user.id": shortId(session?.user_id),
    });
  }, [route, session?.user_id]);

  return (
    <div className="shell">
      <Header onLogoClick={() => navigate({ name: "home" })} />
      {route.name === "home" && <FrontPage navigate={navigate} />}
      {route.name === "edit" && (
        <ArticleEditor draftId={route.draftId} onDone={() => navigate({ name: "home" })} />
      )}
      {route.name === "view" && (
        <ArticleView articleId={route.articleId} onBack={() => navigate({ name: "home" })} />
      )}
    </div>
  );
}

export function App({ config, fallback }: AppProps = {}) {
  const { secret, isLoading } = useLocalFirstAuth();
  if (isLoading || !secret) return <>{fallback ?? <p>Loading...</p>}</>;
  const resolvedConfig = defaultConfig(secret, config);
  return (
    <JazzProvider config={resolvedConfig} fallback={fallback ?? <p>Loading...</p>}>
      <DevToolsRegistration />
      <Inner />
    </JazzProvider>
  );
}
