// #region context-setup-react-minimal
import { JazzProvider } from "jazz-tools/react";
import { TodoList } from "./TodoList.js";

// Your backend serves Jazz at /jazz; everyone starts as a guest (a device account).
export default function App() {
  return (
    <JazzProvider config={{ serverUrl: "/jazz", initial: "device" }}>
      <h1>Todos</h1>
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react-minimal

// #region context-setup-react-runtime-sources
export function AppWithRuntimeSources() {
  return (
    <JazzProvider
      config={{
        serverUrl: "https://my-jazz-server.example.com",
        initial: "device",
        runtimeSources: {
          baseUrl: "/assets/jazz/",
          wasmVersion: "2026-08-25", // Change this for every deployed asset build.
        },
      }}
      loading={<p>Loading...</p>}
    >
      {/* Your app's main component */}
      <TodoList />
    </JazzProvider>
  );
}
// #endregion context-setup-react-runtime-sources
