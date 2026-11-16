import { JazzProvider, type JazzClientConfig } from "jazz-tools/solid";
import { TodoList } from "./TodoList.js";

// Everyone starts as a guest: a device account on this device.
export function App(props: { config: JazzClientConfig }) {
  return (
    <JazzProvider
      config={{
        initial: "device",
        // Local data, so the app works offline once it has loaded.
        client: () => import("jazz-tools/local-first"),
        ...props.config,
      }}
    >
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
