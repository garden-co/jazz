import { useEffect, useMemo, useState } from "react";
import type React from "react";
import { TraceList, TraceDetail } from "./Traces.js";
import { FlowList } from "./Flow.js";

type View = "traces" | "flow";

export function App() {
  const [view, setView] = useState<View>("traces");
  const [selectedTraceId, setSelectedTraceId] = useState<string | null>(null);
  const [minutes, setMinutes] = useState(30);

  return (
    <div style={styles.app}>
      <header style={styles.header}>
        <h1 style={styles.title}>Jazz Sync Viewer</h1>
        <nav style={styles.tabs}>
          <Tab
            active={view === "traces"}
            onClick={() => {
              setView("traces");
              setSelectedTraceId(null);
            }}
          >
            Traces
          </Tab>
          <Tab active={view === "flow"} onClick={() => setView("flow")}>
            Flow
          </Tab>
        </nav>
        <label style={styles.timeWindow}>
          <span style={styles.fieldLabel}>Window (min)</span>
          <input
            type="number"
            min={1}
            value={minutes}
            onChange={(e) => setMinutes(Math.max(1, Number(e.target.value) || 1))}
            style={styles.smallInput}
          />
        </label>
      </header>

      <main>
        {view === "traces" ? (
          selectedTraceId ? (
            <TraceDetail
              traceId={selectedTraceId}
              minutes={minutes}
              onBack={() => setSelectedTraceId(null)}
            />
          ) : (
            <TraceList minutes={minutes} onSelect={setSelectedTraceId} />
          )
        ) : (
          <FlowList minutes={minutes} />
        )}
      </main>
    </div>
  );
}

function Tab(props: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      type="button"
      onClick={props.onClick}
      style={{
        ...styles.tab,
        background: props.active ? "#fff" : "transparent",
        borderBottomColor: props.active ? "#06f" : "transparent",
        fontWeight: props.active ? 600 : 400,
      }}
    >
      {props.children}
    </button>
  );
}

const styles: Record<string, React.CSSProperties> = {
  app: {
    fontFamily: "ui-sans-serif, system-ui, -apple-system, sans-serif",
    padding: 16,
    maxWidth: 1400,
    margin: "0 auto",
  },
  header: {
    display: "flex",
    alignItems: "center",
    gap: 24,
    flexWrap: "wrap",
    borderBottom: "1px solid #e6e6e6",
    paddingBottom: 8,
    marginBottom: 16,
  },
  title: { fontSize: 18, margin: 0, marginRight: "auto" },
  tabs: { display: "flex", gap: 4 },
  tab: {
    padding: "8px 16px",
    border: "none",
    borderBottom: "2px solid transparent",
    background: "transparent",
    cursor: "pointer",
    fontSize: 14,
  },
  timeWindow: { display: "flex", alignItems: "center", gap: 8 },
  fieldLabel: { fontSize: 11, color: "#666", textTransform: "uppercase" },
  smallInput: {
    padding: "4px 8px",
    border: "1px solid #ccc",
    borderRadius: 4,
    fontSize: 14,
    width: 70,
  },
};
