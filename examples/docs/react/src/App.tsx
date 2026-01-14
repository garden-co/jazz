import type { WasmDatabaseLike } from "@jazz/react";
import { useEffect, useState } from "react";

//#region jazz-provider-setup
import { JazzProvider } from "@jazz/react";
import { TaskList } from "./components/TaskList.js";

// Mock WASM database type for documentation
declare function initWasmDatabase(): Promise<WasmDatabaseLike>;

export function App() {
  const [db, setDb] = useState<WasmDatabaseLike | null>(null);

  // biome-ignore lint/correctness/useExhaustiveDependencies: initWasmDatabase is a declared mock function
  useEffect(() => {
    // Initialize WASM database
    initWasmDatabase().then(setDb);
  }, []);

  if (!db) return <div>Loading...</div>;

  return (
    <JazzProvider database={db}>
      <TaskList />
    </JazzProvider>
  );
}
//#endregion
