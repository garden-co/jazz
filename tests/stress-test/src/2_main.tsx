import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider, useAccount } from "jazz-tools/react";
import React from "react";
import ReactDOM from "react-dom/client";
import {
  createBrowserRouter,
  RouterProvider,
  useNavigate,
} from "react-router-dom";
import { TodoAccount } from "./1_schema";
import { ProjectGenerator } from "./ProjectGenerator";
import { ProjectScreen } from "./ProjectScreen";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  return (
    <JazzReactProvider
      sync={{
        peer: `ws://localhost:4200`,
      }}
      AccountSchema={TodoAccount}
    >
      {children}
    </JazzReactProvider>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <JazzAndAuth>
      <App />
      <JazzInspector />
    </JazzAndAuth>
  </React.StrictMode>,
);

function App() {
  const router = createBrowserRouter([
    {
      path: "/",
      element: <HomeScreen />,
    },
    {
      path: "/project/:projectId",
      element: <ProjectScreen />,
    },
  ]);

  const { me } = useAccount(TodoAccount, {
    resolve: { root: true },
  });

  if (!me) return null;

  return <RouterProvider router={router} />;
}

function HomeScreen() {
  const { me } = useAccount(TodoAccount, {
    resolve: { root: { projects: { $each: { $onError: null } } } },
  });

  const navigate = useNavigate();

  return (
    <div
      style={{
        maxWidth: "1200px",
        margin: "0 auto",
        padding: "20px",
        fontFamily:
          '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      }}
    >
      <ProjectGenerator />

      {/* Profiling Toggle */}
      {me?.root && (
        <div
          style={{
            marginTop: "20px",
            padding: "16px",
            background: "white",
            border: "1px solid #e1e5e9",
            borderRadius: "8px",
            display: "flex",
            alignItems: "center",
            gap: "12px",
          }}
        >
          <input
            type="checkbox"
            id="profiling-toggle"
            checked={me.root.profilingEnabled}
            onChange={(e) => {
              me.root.profilingEnabled = e.target.checked;
            }}
            style={{
              width: "18px",
              height: "18px",
              cursor: "pointer",
            }}
          />
          <label
            htmlFor="profiling-toggle"
            style={{
              fontSize: "1rem",
              fontWeight: "500",
              color: "#1a1a1a",
              cursor: "pointer",
              userSelect: "none",
            }}
          >
            Enable Profiling
          </label>
        </div>
      )}
      {me?.root.projects.length ? (
        <h1
          style={{
            marginTop: "40px",
            marginBottom: "30px",
            fontSize: "2.5rem",
            fontWeight: "700",
            color: "#1a1a1a",
            textAlign: "center",
          }}
        >
          My Projects
        </h1>
      ) : null}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))",
          gap: "20px",
          marginTop: "20px",
        }}
      >
        {me?.root.projects.map((project) => {
          if (!project) return null;

          return (
            <div
              key={project.id}
              onClick={() => {
                navigate(`/project/${project.id}`);
              }}
              style={{
                background: "white",
                border: "1px solid #e1e5e9",
                borderRadius: "12px",
                padding: "24px",
                cursor: "pointer",
                transition: "all 0.2s ease-in-out",
                boxShadow: "0 2px 8px rgba(0, 0, 0, 0.06)",
                position: "relative",
                overflow: "hidden",
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = "translateY(-4px)";
                e.currentTarget.style.boxShadow =
                  "0 8px 25px rgba(0, 0, 0, 0.12)";
                e.currentTarget.style.borderColor = "#007AFF";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = "translateY(0)";
                e.currentTarget.style.boxShadow =
                  "0 2px 8px rgba(0, 0, 0, 0.06)";
                e.currentTarget.style.borderColor = "#e1e5e9";
              }}
            >
              <div
                style={{
                  position: "absolute",
                  top: "0",
                  left: "0",
                  right: "0",
                  height: "4px",
                  background: "linear-gradient(90deg, #007AFF, #5856D6)",
                  borderRadius: "12px 12px 0 0",
                }}
              />
              <h3
                style={{
                  margin: "0 0 12px 0",
                  fontSize: "1.25rem",
                  fontWeight: "600",
                  color: "#1a1a1a",
                  lineHeight: "1.4",
                }}
              >
                {project.title}
              </h3>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  marginTop: "16px",
                }}
              >
                <span
                  style={{
                    fontSize: "0.875rem",
                    color: "#6b7280",
                    display: "flex",
                    alignItems: "center",
                    gap: "6px",
                  }}
                >
                  <svg
                    width="16"
                    height="16"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                  >
                    <path d="M9 12l2 2 4-4" />
                    <path d="M21 12c0 4.97-4.03 9-9 9s-9-4.03-9-9 4.03-9 9-9 9 4.03 9 9z" />
                  </svg>
                  Click to open
                </span>
                <svg
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  style={{ color: "#007AFF" }}
                >
                  <path d="M9 18l6-6-6-6" />
                </svg>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
