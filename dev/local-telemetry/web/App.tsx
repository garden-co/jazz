import { useState } from "react";
import type React from "react";
import { FlowList } from "./Flow.js";

export function App() {
  const [minutes, setMinutes] = useState(30);

  return (
    <div style={styles.app}>
      <header style={styles.header}>
        <h1 style={styles.title}>Sync Flow</h1>
        <label style={styles.timeWindow}>
          <span style={styles.fieldLabel}>Window (min)</span>
          <input
            type="number"
            min={1}
            value={minutes}
            onChange={(event) => setMinutes(Math.max(1, Number(event.target.value) || 1))}
            style={styles.smallInput}
          />
        </label>
      </header>

      <main>
        <FlowList minutes={minutes} />
      </main>
    </div>
  );
}

const styles: Record<string, React.CSSProperties> = {
  app: {
    fontFamily: "ui-sans-serif, system-ui, -apple-system, sans-serif",
    padding: 16,
    maxWidth: 1400,
    margin: "0 auto",
    color: "#1f2937",
  },
  header: {
    display: "flex",
    alignItems: "center",
    gap: 24,
    flexWrap: "wrap",
    borderBottom: "1px solid #e5e7eb",
    paddingBottom: 8,
    marginBottom: 16,
  },
  title: { fontSize: 18, margin: 0, marginRight: "auto" },
  timeWindow: { display: "flex", alignItems: "center", gap: 8 },
  fieldLabel: { fontSize: 11, color: "#6b7280", textTransform: "uppercase" },
  smallInput: {
    padding: "4px 8px",
    border: "1px solid #d1d5db",
    borderRadius: 4,
    fontSize: 14,
    width: 70,
  },
};
