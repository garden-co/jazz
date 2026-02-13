import { useRef, useEffect } from "react";
import { CANVAS_WIDTH, CANVAS_HEIGHT } from "./game/constants.js";

/**
 * Game — top-level game component.
 *
 * Phase 1 stub: renders a canvas, nothing else.
 * Game logic (physics, input, rendering) will be added to make tests pass.
 */
export function Game() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    // Clear to background colour
    ctx.fillStyle = "#0a0a0f";
    ctx.fillRect(0, 0, CANVAS_WIDTH, CANVAS_HEIGHT);
  }, []);

  return (
    <div data-testid="game-container">
      <canvas
        ref={canvasRef}
        data-testid="game-canvas"
        width={CANVAS_WIDTH}
        height={CANVAS_HEIGHT}
      />
    </div>
  );
}
