import { useCoState, useSuspenseAccount } from "jazz-tools/react-core";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { GridRoot, PixelCell } from "./schema";
import { getCurrentSyncUrl } from "../../utils/connectionStorage";
import { MaybeLoaded } from "jazz-tools";

// Canvas pixel size - larger for smaller grids, 1:1 for larger grids
const getPixelSize = (gridSize: number) => (gridSize <= 50 ? 8 : 1);

export function GridScreen() {
  const { gridId } = useParams();
  const me = useSuspenseAccount();
  const navigate = useNavigate();
  const canvasRef = useRef<HTMLCanvasElement>(null);

  // Track load timing
  const startTime = useRef(performance.now());
  const [loadTimeMs, setLoadTimeMs] = useState<number | null>(null);
  const [allValuesSynced, setAllValuesSynced] = useState<boolean>(false);

  // Track loaded cells count with ref (no re-renders)
  const loadedCountRef = useRef(0);
  const totalCellsRef = useRef(0);

  // Store cell colors for canvas rendering
  const cellColorsRef = useRef<Map<string, string>>(new Map());

  // Load the grid root
  const grid = useCoState(GridRoot, gridId, {
    resolve: {
      cells: true,
    },
  });

  // Get all cell IDs once the grid is loaded
  const cellIds = useMemo(() => {
    if (!grid.$isLoaded) return [];
    const ids = Array.from(grid.cells.$jazz.refs).map((ref) => ref.id);
    totalCellsRef.current = ids.length;
    return ids;
  }, [grid]);

  // Get grid size for rendering
  const size = grid.$isLoaded ? grid.size : 0;
  const pixelSize = getPixelSize(size);

  // Handle cell load callback - updates canvas directly
  const handleCellLoad = useCallback(
    (cell: MaybeLoaded<PixelCell>, index: number) => {
      const color = cell.$isLoaded ? cell.color : "#FF0000";
      cellColorsRef.current.set(cell.$jazz.id, color);

      // Draw this pixel on the canvas
      const canvas = canvasRef.current;
      if (canvas && size > 0) {
        const ctx = canvas.getContext("2d");
        if (ctx) {
          const x = (index % size) * pixelSize;
          const y = Math.floor(index / size) * pixelSize;
          ctx.fillStyle = color;
          ctx.fillRect(x, y, pixelSize, pixelSize);
        }
      }

      loadedCountRef.current += 1;

      if (
        loadedCountRef.current === totalCellsRef.current &&
        totalCellsRef.current > 0
      ) {
        setLoadTimeMs(performance.now() - startTime.current);
        me.$jazz.waitForAllCoValuesSync().then(() => {
          setAllValuesSynced(true);
        });
      }
    },
    [size, pixelSize],
  );

  useEffect(() => {
    if (cellIds.length > 0) {
      cellIds.forEach((cellId, index) => {
        PixelCell.load(cellId).then((cell) => {
          handleCellLoad(cell, index);
        });
      });
    }
  }, [cellIds.length > 0]);

  // Initialize canvas with background color
  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas && size > 0) {
      const ctx = canvas.getContext("2d");
      if (ctx) {
        ctx.fillStyle = "#1a1a2e";
        ctx.fillRect(0, 0, canvas.width, canvas.height);
      }
    }
  }, [size]);

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "#0a0a0f",
        padding: "20px",
        fontFamily:
          '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
        color: "#e0e0e0",
      }}
    >
      {/* Sync URL Display */}
      <div
        style={{
          maxWidth: "1200px",
          margin: "0 auto 12px",
          padding: "8px 16px",
          background: "linear-gradient(145deg, #1a1a2e, #16162a)",
          border: "1px solid #2a2a4a",
          borderRadius: "8px",
          fontFamily: "monospace",
          fontSize: "0.75rem",
          color: "#6b7280",
          textAlign: "center",
        }}
      >
        <span style={{ color: "#a0a0a0" }}>Sync: </span>
        <span style={{ color: "#00d4ff" }}>{getCurrentSyncUrl()}</span>
      </div>

      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: "24px",
          maxWidth: "1200px",
          margin: "0 auto 24px",
          gap: "12px",
        }}
      >
        <button
          onClick={() => navigate("/grid")}
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
          Back
        </button>

        {/* Load Time Display */}
        <div
          data-testid="load-time"
          data-load-time-ms={allValuesSynced ? loadTimeMs : null}
          style={{
            padding: "12px 20px",
            background:
              loadTimeMs !== null
                ? "linear-gradient(135deg, #1a3a1a, #1a2a1a)"
                : "linear-gradient(145deg, #1a1a2e, #16162a)",
            border: `1px solid ${loadTimeMs !== null ? "#2a5a2a" : "#2a2a4a"}`,
            borderRadius: "8px",
            fontFamily: "monospace",
            fontSize: "1rem",
          }}
        >
          {typeof loadTimeMs === "number" ? (
            <span style={{ color: "#4ade80" }}>
              Loaded in {loadTimeMs.toFixed(0)}ms
            </span>
          ) : (
            <span style={{ color: "#a0a0a0" }}>Loading...</span>
          )}
        </div>

        {/* Grid Info */}
        <div
          style={{
            padding: "12px 20px",
            background: "linear-gradient(145deg, #1a1a2e, #16162a)",
            border: "1px solid #2a2a4a",
            borderRadius: "8px",
          }}
        >
          <span style={{ color: "#00d4ff", fontWeight: "600" }}>
            {size}x{size}
          </span>
          <span style={{ color: "#6b7280", marginLeft: "8px" }}>
            ({cellIds.length} cells)
          </span>
        </div>
      </div>

      {/* Canvas Grid */}
      <div
        style={{
          display: "flex",
          justifyContent: "center",
          padding: "20px",
        }}
      >
        <canvas
          ref={canvasRef}
          width={size * pixelSize}
          height={size * pixelSize}
          style={{
            border: "2px solid #3a3a5a",
            borderRadius: "4px",
            imageRendering: "pixelated",
          }}
        />
      </div>
    </div>
  );
}
