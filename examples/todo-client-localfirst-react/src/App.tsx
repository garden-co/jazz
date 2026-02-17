import { JazzProvider, type JazzProviderProps } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

function readEnvAppId(): string | undefined {
  return (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env
    ?.JAZZ_APP_ID;
}

function defaultConfig(): NonNullable<JazzProviderProps["config"]> {
  return {
    appId: readEnvAppId() ?? "todo-react-example",
    env: "dev",
    userBranch: "main",
  };
}

export function App({ config, fallback }: Partial<JazzProviderProps> = {}) {
  return (
    <JazzProvider config={config ?? defaultConfig()} fallback={fallback ?? <p>Loading...</p>}>
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
