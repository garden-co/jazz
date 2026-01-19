import ReactDOM from "react-dom/client";
import React, { useState } from "react";
import {
  RouterProvider,
  createBrowserRouter,
  useNavigate,
} from "react-router-dom";

import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider, useAccount } from "jazz-tools/react";
import { AppAccount } from "./schema";
import {
  getCurrentSyncUrl,
  saveConnection,
  getConnectionHistory,
} from "./utils/connectionStorage";

// Scenario components
import { TodoHome } from "./scenarios/todo/TodoHome";
import { ProjectScreen } from "./scenarios/todo/ProjectScreen";
import { GridHome } from "./scenarios/grid/GridHome";
import { GridScreen } from "./scenarios/grid/GridScreen";
import { SyncUrlSelector } from "./components/SyncUrlSelector";

function JazzAndAuth({
  children,
  syncUrl,
}: {
  children: React.ReactNode;
  syncUrl: string;
}) {
  return (
    <JazzReactProvider
      authSecretStorageKey="stress-test"
      sync={{
        peer: syncUrl as `ws://${string}` | `wss://${string}`,
      }}
      AccountSchema={AppAccount}
    >
      {children}
    </JazzReactProvider>
  );
}

function ScenarioSelector() {
  const navigate = useNavigate();

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "linear-gradient(135deg, #0a0a0f 0%, #1a1a2e 100%)",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        fontFamily:
          '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        padding: "40px 20px",
      }}
    >
      <h1
        style={{
          fontSize: "3rem",
          fontWeight: "700",
          background: "linear-gradient(135deg, #00d4ff, #7c3aed)",
          WebkitBackgroundClip: "text",
          WebkitTextFillColor: "transparent",
          marginBottom: "16px",
          textAlign: "center",
        }}
      >
        Stress Test
      </h1>
      <p
        style={{
          color: "#a0a0a0",
          fontSize: "1.25rem",
          marginBottom: "48px",
          textAlign: "center",
        }}
      >
        Choose a test scenario
      </p>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
          gap: "24px",
          maxWidth: "800px",
          width: "100%",
        }}
      >
        {/* Todo Scenario */}
        <div
          onClick={() => navigate("/todo")}
          style={{
            background: "linear-gradient(145deg, #1a1a2e, #16162a)",
            border: "1px solid #2a2a4a",
            borderRadius: "16px",
            padding: "32px",
            cursor: "pointer",
            transition: "all 0.3s ease",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.transform = "translateY(-4px)";
            e.currentTarget.style.borderColor = "#007AFF";
            e.currentTarget.style.boxShadow =
              "0 12px 40px rgba(0, 122, 255, 0.2)";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = "translateY(0)";
            e.currentTarget.style.borderColor = "#2a2a4a";
            e.currentTarget.style.boxShadow = "none";
          }}
        >
          <div
            style={{
              width: "48px",
              height: "48px",
              background: "linear-gradient(135deg, #007AFF, #5856D6)",
              borderRadius: "12px",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              marginBottom: "20px",
            }}
          >
            <svg
              width="24"
              height="24"
              viewBox="0 0 24 24"
              fill="none"
              stroke="white"
              strokeWidth="2"
            >
              <path d="M9 12l2 2 4-4" />
              <path d="M21 12c0 4.97-4.03 9-9 9s-9-4.03-9-9 4.03-9 9-9 9 4.03 9 9z" />
            </svg>
          </div>
          <h2
            style={{
              color: "#e0e0e0",
              fontSize: "1.5rem",
              fontWeight: "600",
              marginBottom: "12px",
            }}
          >
            Todo Projects
          </h2>
          <p
            style={{
              color: "#6b7280",
              fontSize: "0.875rem",
              lineHeight: "1.6",
            }}
          >
            Generate random todo projects with configurable task counts for
            testing list rendering and sync performance.
          </p>
        </div>

        {/* Grid Scenario */}
        <div
          onClick={() => navigate("/grid")}
          style={{
            background: "linear-gradient(145deg, #1a1a2e, #16162a)",
            border: "1px solid #2a2a4a",
            borderRadius: "16px",
            padding: "32px",
            cursor: "pointer",
            transition: "all 0.3s ease",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.transform = "translateY(-4px)";
            e.currentTarget.style.borderColor = "#7c3aed";
            e.currentTarget.style.boxShadow =
              "0 12px 40px rgba(124, 58, 237, 0.2)";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = "translateY(0)";
            e.currentTarget.style.borderColor = "#2a2a4a";
            e.currentTarget.style.boxShadow = "none";
          }}
        >
          <div
            style={{
              width: "48px",
              height: "48px",
              background: "linear-gradient(135deg, #00d4ff, #7c3aed)",
              borderRadius: "12px",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              marginBottom: "20px",
            }}
          >
            <svg
              width="24"
              height="24"
              viewBox="0 0 24 24"
              fill="none"
              stroke="white"
              strokeWidth="2"
            >
              <rect x="3" y="3" width="7" height="7" />
              <rect x="14" y="3" width="7" height="7" />
              <rect x="3" y="14" width="7" height="7" />
              <rect x="14" y="14" width="7" height="7" />
            </svg>
          </div>
          <h2
            style={{
              color: "#e0e0e0",
              fontSize: "1.5rem",
              fontWeight: "600",
              marginBottom: "12px",
            }}
          >
            Pixel Grid
          </h2>
          <p
            style={{
              color: "#6b7280",
              fontSize: "0.875rem",
              lineHeight: "1.6",
            }}
          >
            Generate NxN pixel grids with random colors and configurable payload
            sizes for testing canvas rendering and data loading.
          </p>
        </div>
      </div>

      {/* Sync URL Selector */}
      <SyncUrlSelector />
    </div>
  );
}

function App() {
  const router = createBrowserRouter([
    {
      path: "/",
      element: <ScenarioSelector />,
    },
    {
      path: "/todo",
      element: <TodoHome />,
    },
    {
      path: "/todo/:projectId",
      element: <ProjectScreen />,
    },
    {
      path: "/grid",
      element: <GridHome />,
    },
    {
      path: "/grid/:gridId",
      element: <GridScreen />,
    },
  ]);

  const me = useAccount(AppAccount, {
    resolve: { root: true },
  });

  if (!me) return null;

  return <RouterProvider router={router} />;
}

function getSyncUrlFromQueryString(): string | null {
  const params = new URLSearchParams(window.location.search);
  const sync = params.get("sync");
  if (sync && (sync.startsWith("ws://") || sync.startsWith("wss://"))) {
    return sync;
  }
  return null;
}

function Root() {
  // Priority: query string > localStorage
  const [syncUrl, setSyncUrl] = useState(() => {
    const qsUrl = getSyncUrlFromQueryString();
    if (qsUrl) {
      saveConnection(qsUrl);
      return qsUrl;
    }
    return getCurrentSyncUrl();
  });
  const [key, setKey] = useState(0);

  // Store functions in window for components to access
  React.useEffect(() => {
    (window as any).__setSyncUrl = (url: string) => {
      saveConnection(url);
      setSyncUrl(url);
      setKey((k) => k + 1);
    };
    (window as any).__getSyncUrl = () => syncUrl;
    (window as any).__getConnectionHistory = () => getConnectionHistory();
    return () => {
      delete (window as any).__setSyncUrl;
      delete (window as any).__getSyncUrl;
      delete (window as any).__getConnectionHistory;
    };
  }, [syncUrl]);

  return (
    <JazzAndAuth key={key} syncUrl={syncUrl}>
      <App />
      <JazzInspector />
    </JazzAndAuth>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Root />
  </React.StrictMode>,
);
