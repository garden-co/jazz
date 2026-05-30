import { Show, type JSX } from "solid-js";
import { JazzProvider, createSolidJazzClient, useLocalFirstAuth } from "jazz-tools/solid";
import type { DbConfig } from "jazz-tools";
import { Toaster } from "solid-sonner";
import { TodoList } from "./TodoList.js";

type AppProps = {
  config?: Partial<DbConfig>;
  fallback?: JSX.Element;
};

function readEnv(name: string): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env?.[name];
}

function defaultConfig(secret: string, overrides: Partial<DbConfig> = {}): DbConfig {
  const appId = overrides.appId ?? readEnv("VITE_JAZZ_APP_ID");
  const serverUrl = overrides.serverUrl ?? readEnv("VITE_JAZZ_SERVER_URL");

  if (!appId) {
    throw new Error("Missing appId: add jazzPlugin() to vite.config.ts or set VITE_JAZZ_APP_ID");
  }

  return {
    appId,
    env: "dev",
    userBranch: "main",
    secret,
    ...(serverUrl ? { serverUrl } : {}),
    ...overrides,
  };
}

function ReadyApp(props: { secret: string; config?: Partial<DbConfig>; fallback?: JSX.Element }) {
  const resolvedConfig = () => defaultConfig(props.secret, props.config ?? {});
  const client = createSolidJazzClient(resolvedConfig);

  return (
    <JazzProvider client={client} fallback={props.fallback ?? <p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
      <Toaster />
    </JazzProvider>
  );
}

export function App(props: AppProps = {}) {
  const auth = useLocalFirstAuth();

  return (
    <Show when={!auth.isLoading && auth.secret} fallback={props.fallback ?? <p>Loading...</p>}>
      {(secret) => <ReadyApp secret={secret()} config={props.config} fallback={props.fallback} />}
    </Show>
  );
}
