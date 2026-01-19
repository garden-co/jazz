import { useAccount } from "jazz-tools/react";
import { useNavigate } from "react-router-dom";
import { AppAccount } from "../../schema";
import { ConfigPanel } from "./ConfigPanel";

export function GridHome() {
  const me = useAccount(AppAccount, {
    resolve: { root: { grids: { $each: { $onError: "catch" } } } },
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
        background: "#0a0a0f",
        minHeight: "100vh",
        color: "#e0e0e0",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: "24px",
        }}
      >
        <h1
          style={{
            fontSize: "2rem",
            fontWeight: "700",
            background: "linear-gradient(135deg, #00d4ff, #7c3aed)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
          }}
        >
          Grid Load Test
        </h1>
        <button
          onClick={() => navigate("/")}
          style={{
            padding: "10px 20px",
            background: "#2a2a4a",
            border: "1px solid #3a3a5a",
            borderRadius: "8px",
            color: "#e0e0e0",
            fontSize: "0.875rem",
            cursor: "pointer",
          }}
        >
          Back to Scenarios
        </button>
      </div>

      <ConfigPanel />

      {me.$isLoaded && me.root.grids.length > 0 && (
        <>
          <h2
            style={{
              marginTop: "40px",
              marginBottom: "20px",
              fontSize: "1.5rem",
              fontWeight: "600",
              color: "#a0a0a0",
            }}
          >
            Generated Grids
          </h2>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))",
              gap: "16px",
            }}
          >
            {me.root.grids.map((grid) => {
              if (!grid.$isLoaded) return null;

              return (
                <div
                  key={grid.$jazz.id}
                  onClick={() => navigate(`/grid/${grid.$jazz.id}`)}
                  style={{
                    background: "linear-gradient(145deg, #1a1a2e, #16162a)",
                    border: "1px solid #2a2a4a",
                    borderRadius: "12px",
                    padding: "20px",
                    cursor: "pointer",
                    transition: "all 0.2s ease",
                  }}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.transform = "translateY(-2px)";
                    e.currentTarget.style.borderColor = "#7c3aed";
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.transform = "translateY(0)";
                    e.currentTarget.style.borderColor = "#2a2a4a";
                  }}
                >
                  <div
                    style={{
                      fontSize: "1.25rem",
                      fontWeight: "600",
                      color: "#00d4ff",
                    }}
                  >
                    {grid.size}x{grid.size}
                  </div>
                  <div
                    style={{
                      fontSize: "0.875rem",
                      color: "#6b7280",
                      marginTop: "8px",
                    }}
                  >
                    {grid.size * grid.size} cells
                  </div>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      me.root.grids.$jazz.remove(
                        (g) => g.$jazz.id === grid.$jazz.id,
                      );
                    }}
                    style={{
                      marginTop: "12px",
                      padding: "6px 12px",
                      background: "#3a1a1a",
                      border: "1px solid #5a2a2a",
                      borderRadius: "6px",
                      color: "#ff6b6b",
                      fontSize: "0.75rem",
                      cursor: "pointer",
                    }}
                  >
                    Delete
                  </button>
                </div>
              );
            })}
          </div>
        </>
      )}
    </div>
  );
}
