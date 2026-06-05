import { useEffect, useState } from "react";
import type React from "react";
import { parseSessionRoute, sessionListHash } from "./route.js";
import { SessionDetailPage, SessionListPage } from "./SessionExplorer.js";

export function App() {
  const [minutes, setMinutes] = useState(30);
  const route = useHashRoute();

  return (
    <div style={styles.app}>
      <header style={styles.header}>
        <h1 style={styles.title}>Sync Sessions</h1>
      </header>

      <main>
        {route.page === "session" ? (
          <SessionDetailPage sessionId={route.sessionId} />
        ) : (
          <SessionListPage minutes={minutes} onMinutesChange={setMinutes} />
        )}
      </main>
    </div>
  );
}

function useHashRoute() {
  const [route, setRoute] = useState(() => parseSessionRoute(window.location.hash));

  useEffect(() => {
    if (!window.location.hash) {
      window.history.replaceState(null, "", sessionListHash());
      setRoute(parseSessionRoute(window.location.hash));
    }
    const onHashChange = () => setRoute(parseSessionRoute(window.location.hash));
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  return route;
}

const styles: Record<string, React.CSSProperties> = {
  app: {
    fontFamily: "ui-sans-serif, system-ui, -apple-system, sans-serif",
    minHeight: "100vh",
    color: "#1f2937",
    background: "#f3f4f6",
  },
  header: {
    display: "flex",
    alignItems: "center",
    borderBottom: "1px solid #d1d5db",
    padding: "0 12px",
    height: 41,
    background: "#ffffff",
  },
  title: { fontSize: 18, margin: 0, marginRight: "auto" },
};
