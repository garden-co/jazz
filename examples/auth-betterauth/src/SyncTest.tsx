/**
 * Sync Test component for authenticated users.
 *
 * This component demonstrates:
 * 1. Connecting to groove-server with a JWT token
 * 2. Creating documents with owner-based access control
 * 3. Real-time sync with policy-filtered data
 */

import { useEffect, useRef, useState } from "react";

interface LogEntry {
  time: string;
  message: string;
}

interface Document {
  id: string;
  title: string;
  content: string;
  owner_id: string;
}

interface SyncTestProps {
  token: string;
  userId: string;
}

export function SyncTest({ token, userId }: SyncTestProps) {
  const [status, setStatus] = useState("Initializing...");
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [documents, setDocuments] = useState<Document[]>([]);
  const [isReady, setIsReady] = useState(false);
  const [newTitle, setNewTitle] = useState("");
  const dbRef = useRef<any>(null);
  const queryHandleRef = useRef<any>(null);

  const log = (msg: string) => {
    const time = new Date().toISOString().substring(11, 19);
    setLogs((prev) => [...prev.slice(-19), { time, message: msg }]);
    console.log(`[SyncTest ${time}] ${msg}`);
  };

  // biome-ignore lint/correctness/useExhaustiveDependencies: log is stable (only uses setLogs)
  useEffect(() => {
    let mounted = true;

    async function init() {
      try {
        log("Loading WASM module...");
        const wasm = await import("groove-wasm");
        await wasm.default();

        if (!mounted) return;

        log("Creating WasmSyncedLocalNode with JWT...");
        // Use a shared catalog ID so all clients have the same schema
        const SHARED_CATALOG_ID = "auth-sync-demo-v1";
        const db = new wasm.WasmSyncedLocalNode(
          "http://localhost:8080",
          token, // Use the JWT token from BetterAuth
          SHARED_CATALOG_ID,
        );
        dbRef.current = db;

        // Set up callbacks
        db.setOnStateChange((state: string) => {
          log(`Sync state: ${state}`);
          setStatus(state);
        });

        db.setOnError((msg: string) => {
          log(`Sync error: ${msg}`);
        });

        log("Initializing schema with policies...");
        // Create schema with owner-based policy
        // Documents can only be seen by their owner
        db.initSchema(`
          CREATE TABLE users (
            name STRING,
            external_id STRING
          );

          CREATE TABLE documents (
            title STRING NOT NULL,
            content STRING NOT NULL,
            owner_id REFERENCES users NOT NULL
          );

          CREATE POLICY ON documents FOR SELECT
            WHERE owner_id = @viewer;

          CREATE POLICY ON documents FOR INSERT
            CHECK (owner_id = @viewer);
        `);

        // Subscribe to documents - will only receive docs we're allowed to see
        queryHandleRef.current = db.subscribeRows(
          "SELECT * FROM documents",
          (rows: Document[]) => {
            if (mounted) setDocuments(rows);
          },
        );

        if (mounted) {
          setStatus("Ready (disconnected)");
          setIsReady(true);
          log("Initialization complete");
        }
      } catch (e: any) {
        const errorMsg = e?.message || String(e);
        log(`Error: ${errorMsg}`);
        console.error("Init error:", e);
        if (mounted) setStatus("Error");
      }
    }

    init();

    return () => {
      mounted = false;
      if (queryHandleRef.current) {
        queryHandleRef.current.unsubscribe();
      }
    };
  }, [token]);

  const handleConnect = async () => {
    if (!dbRef.current) return;
    try {
      log("Connecting to sync server...");
      await dbRef.current.connect("SELECT * FROM documents");
      log("Connected!");
    } catch (e: any) {
      const errorMsg = e?.message || String(e);
      log(`Connection error: ${errorMsg}`);
      console.error("Connect error:", e);
    }
  };

  const handleCreateDocument = () => {
    if (!dbRef.current || !newTitle.trim()) return;
    try {
      const title = newTitle.trim();
      const content = `Created at ${new Date().toISOString()}`;
      log(`Creating document: ${title}`);

      // The policy ensures owner_id must be @viewer
      const result = dbRef.current.execute(
        `INSERT INTO documents (title, content, owner_id) VALUES ('${title}', '${content}', @viewer)`,
      );
      log(`Insert result: ${result}`);
      setNewTitle("");
    } catch (e: any) {
      log(`Insert error: ${e.message}`);
    }
  };

  return (
    <div className="card">
      <h3>Sync Test</h3>
      <p>
        Status: <span data-testid="sync-status">{status}</span>
      </p>
      <p>
        <small>User ID: {userId}</small>
      </p>

      <div style={{ marginBottom: "1rem" }}>
        <button
          data-testid="connect-btn"
          onClick={handleConnect}
          disabled={!isReady}
        >
          Connect to Sync Server
        </button>
      </div>

      <div style={{ marginBottom: "1rem" }}>
        <input
          type="text"
          data-testid="doc-title-input"
          placeholder="Document title"
          value={newTitle}
          onChange={(e) => setNewTitle(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleCreateDocument()}
          style={{ marginRight: "0.5rem" }}
        />
        <button
          data-testid="create-doc-btn"
          onClick={handleCreateDocument}
          disabled={!isReady || !newTitle.trim()}
        >
          Create Document
        </button>
      </div>

      <div>
        <strong>Your Documents ({documents.length}):</strong>
        <div
          data-testid="documents-list"
          style={{
            background: "#f5f5f5",
            padding: "0.5rem",
            marginTop: "0.5rem",
            minHeight: "100px",
            maxHeight: "200px",
            overflow: "auto",
          }}
        >
          {documents.length === 0 ? (
            <p style={{ color: "#666", margin: 0 }}>No documents yet</p>
          ) : (
            documents.map((doc) => (
              <div
                key={doc.id}
                data-testid={`doc-${doc.id}`}
                style={{
                  padding: "0.5rem",
                  borderBottom: "1px solid #ddd",
                  background: "#fff",
                  marginBottom: "0.25rem",
                }}
              >
                <strong>{doc.title}</strong>
                <br />
                <small style={{ color: "#666" }}>{doc.content}</small>
              </div>
            ))
          )}
        </div>
      </div>

      <details style={{ marginTop: "1rem" }}>
        <summary>Sync Log</summary>
        <pre
          data-testid="sync-log"
          style={{
            background: "#f0f0f0",
            padding: "0.5rem",
            fontSize: "0.75rem",
            maxHeight: "150px",
            overflow: "auto",
          }}
        >
          {logs.map((entry, i) => (
            <div key={`${entry.time}-${i}`}>
              [{entry.time}] {entry.message}
            </div>
          ))}
        </pre>
      </details>
    </div>
  );
}

export default SyncTest;
