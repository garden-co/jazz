import { useEffect, useRef, useState } from "react";

// Generate a unique tab ID
const tabId = Math.random().toString(36).substring(7);

interface LogEntry {
  time: string;
  message: string;
}

interface TestItem {
  id: string;
  name: string;
  created_by: string;
}

export function SyncTest() {
  const [status, setStatus] = useState("Initializing...");
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [items, setItems] = useState<TestItem[]>([]);
  const [isReady, setIsReady] = useState(false);
  const dbRef = useRef<any>(null);
  const queryHandleRef = useRef<any>(null);

  const log = (msg: string) => {
    const time = new Date().toISOString().substring(11, 19);
    setLogs((prev) => [...prev, { time, message: msg }]);
    console.log(`[${time}] ${msg}`);
  };

  useEffect(() => {
    async function init() {
      try {
        log("Loading WASM module...");
        const wasm = await import("groove-wasm");
        await wasm.default();

        log("Creating WasmSyncedLocalNode...");
        // Use a shared catalog ID so all clients have the same schema object IDs
        const SHARED_CATALOG_ID = "sync-test-catalog-v1";
        const db = new wasm.WasmSyncedLocalNode(
          "http://localhost:8080",
          `token-${tabId}`,
          SHARED_CATALOG_ID
        );
        dbRef.current = db;

        // Set up callbacks
        db.setOnStateChange((state: string) => {
          log(`Sync state: ${state}`);
          setStatus(`Sync: ${state}`);
        });

        db.setOnError((msg: string) => {
          log(`Sync error: ${msg}`);
        });

        log("Initializing schema...");
        db.initSchema(`
          CREATE TABLE test_items (
            name STRING NOT NULL,
            created_by STRING NOT NULL
          )
        `);

        // Set up reactive query subscription with full row data
        // The callback receives the complete result set on each change
        queryHandleRef.current = db.subscribeRows(
          "SELECT * FROM test_items",
          (rows: TestItem[]) => {
            // Defer state update to avoid RefCell borrow conflict
            queueMicrotask(() => setItems(rows));
          }
        );

        setStatus("Ready (disconnected)");
        setIsReady(true);
        log("Initialization complete");
      } catch (e: any) {
        const errorMsg = e?.message || String(e);
        log(`Error: ${errorMsg}`);
        console.error("Init error:", e);
        setStatus("Error");
      }
    }

    init();

    // Cleanup subscription on unmount
    return () => {
      if (queryHandleRef.current) {
        queryHandleRef.current.unsubscribe();
      }
    };
  }, []);

  const handleConnect = async () => {
    if (!dbRef.current) return;
    try {
      log("Connecting to sync server...");
      await dbRef.current.connect("SELECT * FROM test_items");
      log("Connected!");
    } catch (e: any) {
      const errorMsg = e?.message || String(e);
      log(`Connection error: ${errorMsg}`);
      console.error("Connect error:", e);
    }
  };

  const handleInsert = () => {
    if (!dbRef.current) return;
    try {
      const name = `Item-${Date.now()}`;
      log(`Inserting: ${name}`);
      const result = dbRef.current.execute(
        `INSERT INTO test_items (name, created_by) VALUES ('${name}', '${tabId}')`
      );
      log(`Insert result: ${result}`);
      // No need to refresh - reactive subscription updates automatically
    } catch (e: any) {
      log(`Insert error: ${e.message}`);
    }
  };

  return (
    <div style={{ fontFamily: "sans-serif", padding: "20px" }}>
      <h1>Groove Sync Test</h1>
      <p>
        Status: <span data-testid="status">{status}</span>
      </p>
      <p>
        Tab ID: <span data-testid="tabId">{tabId}</span>
      </p>

      <div>
        <button
          data-testid="connectBtn"
          onClick={handleConnect}
          disabled={!isReady}
        >
          Connect to Server
        </button>
        <button
          data-testid="insertBtn"
          onClick={handleInsert}
          disabled={!isReady}
          style={{ marginLeft: "10px" }}
        >
          Insert Test Row
        </button>
      </div>

      <div style={{ marginTop: "20px" }}>
        <strong>Table Contents ({items.length} rows):</strong>
        <pre data-testid="tableContents" style={{ background: "#f0f0f0", padding: "10px", color: "#000" }}>
          {JSON.stringify(items, null, 2)}
        </pre>
      </div>

      <div
        style={{ background: "#f0f0f0", padding: "10px", marginTop: "20px", color: "#000" }}
      >
        <strong>Log:</strong>
        <pre data-testid="log" style={{ color: "#000" }}>
          {logs.map((entry, i) => (
            <div key={i}>
              [{entry.time}] {entry.message}
            </div>
          ))}
        </pre>
      </div>
    </div>
  );
}

export default SyncTest;
