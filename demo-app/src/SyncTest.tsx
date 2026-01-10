import { useEffect, useRef, useState } from "react";

// Generate a unique tab ID
const tabId = Math.random().toString(36).substring(7);

interface LogEntry {
  time: string;
  message: string;
}

export function SyncTest() {
  const [status, setStatus] = useState("Initializing...");
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [tableContents, setTableContents] = useState<string>("(not loaded)");
  const [isReady, setIsReady] = useState(false);
  const dbRef = useRef<any>(null);

  const log = (msg: string) => {
    const time = new Date().toISOString().substring(11, 19);
    setLogs((prev) => [...prev, { time, message: msg }]);
    console.log(`[${time}] ${msg}`);
  };

  const refreshTable = () => {
    if (!dbRef.current) return;
    try {
      const result = dbRef.current.execute("SELECT * FROM test_items");
      setTableContents(JSON.stringify(result, null, 2));
    } catch (e: any) {
      setTableContents(`Error: ${e.message}`);
    }
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

        setStatus("Ready (disconnected)");
        setIsReady(true);
        log("Initialization complete");
        refreshTable();
      } catch (e: any) {
        const errorMsg = e?.message || String(e);
        log(`Error: ${errorMsg}`);
        console.error("Init error:", e);
        setStatus("Error");
      }
    }

    init();

    // Refresh table every second
    const interval = setInterval(refreshTable, 1000);
    return () => clearInterval(interval);
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
      refreshTable();
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
        <strong>Table Contents:</strong>
        <pre data-testid="tableContents" style={{ background: "#f0f0f0", padding: "10px", color: "#000" }}>
          {tableContents}
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
