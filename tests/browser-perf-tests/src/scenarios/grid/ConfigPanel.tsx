import { FormEvent, useState } from "react";
import { generateGrid } from "./generate";
import { AppAccount } from "../../schema";

export function ConfigPanel() {
  const [isGenerating, setIsGenerating] = useState(false);

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    if (isGenerating) return;
    e.preventDefault();

    const formData = new FormData(e.currentTarget);
    const size = Math.max(1, parseInt(formData.get("size") as string) || 10);
    const minPadding = Math.max(
      0,
      parseInt(formData.get("minPadding") as string) || 0,
    );
    const maxPadding = Math.max(
      minPadding,
      parseInt(formData.get("maxPadding") as string) || 100,
    );

    setIsGenerating(true);
    const { grid, done } = generateGrid(size, minPadding, maxPadding);

    const { root } = await AppAccount.getMe().$jazz.ensureLoaded({
      resolve: {
        root: {
          grids: true,
        },
      },
    });

    root.grids.$jazz.push(grid);
    await done;
    setIsGenerating(false);
  };

  return (
    <div
      style={{
        background: "linear-gradient(145deg, #1a1a2e, #16162a)",
        border: "1px solid #2a2a4a",
        borderRadius: "12px",
        padding: "24px",
      }}
    >
      <h2
        style={{
          fontSize: "1.25rem",
          fontWeight: "600",
          marginBottom: "20px",
          color: "#e0e0e0",
        }}
      >
        Generate Grid
      </h2>

      {/* Grid Generation Form */}
      <form onSubmit={handleSubmit}>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))",
            gap: "16px",
            marginBottom: "20px",
          }}
        >
          <div>
            <label
              htmlFor="size"
              style={{
                display: "block",
                fontSize: "0.875rem",
                fontWeight: "500",
                color: "#a0a0a0",
                marginBottom: "8px",
              }}
            >
              Grid Size (NxN)
            </label>
            <input
              id="size"
              name="size"
              type="number"
              min="1"
              max="100"
              defaultValue={10}
              style={{
                width: "100%",
                padding: "10px 14px",
                background: "#0a0a0f",
                border: "1px solid #3a3a5a",
                borderRadius: "8px",
                color: "#e0e0e0",
                fontSize: "0.875rem",
              }}
            />
          </div>

          <div>
            <label
              htmlFor="minPadding"
              style={{
                display: "block",
                fontSize: "0.875rem",
                fontWeight: "500",
                color: "#a0a0a0",
                marginBottom: "8px",
              }}
            >
              Min Padding (bytes)
            </label>
            <input
              id="minPadding"
              name="minPadding"
              type="number"
              min="0"
              defaultValue={0}
              style={{
                width: "100%",
                padding: "10px 14px",
                background: "#0a0a0f",
                border: "1px solid #3a3a5a",
                borderRadius: "8px",
                color: "#e0e0e0",
                fontSize: "0.875rem",
              }}
            />
          </div>

          <div>
            <label
              htmlFor="maxPadding"
              style={{
                display: "block",
                fontSize: "0.875rem",
                fontWeight: "500",
                color: "#a0a0a0",
                marginBottom: "8px",
              }}
            >
              Max Padding (bytes)
            </label>
            <input
              id="maxPadding"
              name="maxPadding"
              type="number"
              min="0"
              defaultValue={100}
              style={{
                width: "100%",
                padding: "10px 14px",
                background: "#0a0a0f",
                border: "1px solid #3a3a5a",
                borderRadius: "8px",
                color: "#e0e0e0",
                fontSize: "0.875rem",
              }}
            />
          </div>
        </div>

        <button
          type="submit"
          disabled={isGenerating}
          style={{
            width: "100%",
            padding: "12px 20px",
            background: isGenerating
              ? "#2a2a4a"
              : "linear-gradient(135deg, #7c3aed, #00d4ff)",
            border: "none",
            borderRadius: "8px",
            color: "#fff",
            fontSize: "1rem",
            fontWeight: "600",
            cursor: isGenerating ? "not-allowed" : "pointer",
            transition: "all 0.2s ease",
          }}
        >
          {isGenerating ? "Generating..." : "Generate Grid"}
        </button>
      </form>
    </div>
  );
}
