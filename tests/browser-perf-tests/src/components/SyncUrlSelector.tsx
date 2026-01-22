import { useState } from "react";
import {
  getConnectionHistory,
  type ConnectionHistory,
} from "../utils/connectionStorage";

export function SyncUrlSelector() {
  const [connectionHistory, setConnectionHistory] = useState<ConnectionHistory>(
    () => (window as any).__getConnectionHistory?.() || getConnectionHistory(),
  );
  const [syncUrl, setSyncUrlLocal] = useState(connectionHistory.current);

  const handleSyncUrlChange = (url: string) => {
    const newUrl = url.trim();
    if (newUrl && (newUrl.startsWith("ws://") || newUrl.startsWith("wss://"))) {
      (window as any).__setSyncUrl?.(newUrl);
      // Update local state to reflect new history
      const newHistory =
        (window as any).__getConnectionHistory?.() || getConnectionHistory();
      setConnectionHistory(newHistory);
      setSyncUrlLocal(newUrl);
    }
  };

  return (
    <div
      style={{
        marginTop: "48px",
        padding: "24px",
        background: "linear-gradient(145deg, #1a1a2e, #16162a)",
        border: "1px solid #2a2a4a",
        borderRadius: "12px",
        maxWidth: "600px",
        width: "100%",
      }}
    >
      <h3
        style={{
          color: "#e0e0e0",
          fontSize: "1rem",
          fontWeight: "600",
          marginBottom: "16px",
        }}
      >
        Sync Server
      </h3>

      <div style={{ display: "flex", gap: "8px" }}>
        <input
          type="text"
          value={syncUrl}
          onChange={(e) => setSyncUrlLocal(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              handleSyncUrlChange(syncUrl);
            }
          }}
          placeholder="ws://localhost:4200"
          style={{
            flex: 1,
            padding: "12px 16px",
            background: "#0a0a0f",
            border: "1px solid #3a3a5a",
            borderRadius: "8px",
            color: "#e0e0e0",
            fontSize: "0.875rem",
            fontFamily: "monospace",
          }}
        />
        <button
          type="button"
          onClick={() => handleSyncUrlChange(syncUrl)}
          style={{
            padding: "12px 20px",
            background: "linear-gradient(135deg, #7c3aed, #00d4ff)",
            border: "none",
            borderRadius: "8px",
            color: "#fff",
            fontSize: "0.875rem",
            fontWeight: "600",
            cursor: "pointer",
            whiteSpace: "nowrap",
          }}
        >
          Connect
        </button>
      </div>

      {/* Recent Connections */}
      {connectionHistory.history.length > 0 && (
        <div style={{ marginTop: "16px" }}>
          <div
            style={{
              fontSize: "0.75rem",
              color: "#6b7280",
              marginBottom: "8px",
            }}
          >
            Recent connections:
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
            {connectionHistory.history.map((url) => (
              <button
                key={url}
                type="button"
                onClick={() => {
                  setSyncUrlLocal(url);
                  handleSyncUrlChange(url);
                }}
                style={{
                  padding: "8px 12px",
                  background:
                    url === connectionHistory.current ? "#1a3a1a" : "#0a0a0f",
                  border: `1px solid ${url === connectionHistory.current ? "#2a5a2a" : "#3a3a5a"}`,
                  borderRadius: "6px",
                  color:
                    url === connectionHistory.current ? "#4ade80" : "#a0a0a0",
                  fontSize: "0.75rem",
                  cursor: "pointer",
                  fontFamily: "monospace",
                  maxWidth: "100%",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                  transition: "all 0.2s ease",
                }}
                title={url}
              >
                {url === connectionHistory.current && (
                  <span style={{ marginRight: "6px" }}>●</span>
                )}
                {url}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
