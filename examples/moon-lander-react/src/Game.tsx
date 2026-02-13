import { useRef, useEffect, useState } from "react";
import {
  CANVAS_WIDTH,
  CANVAS_HEIGHT,
  INITIAL_ALTITUDE,
  GRAVITY,
  type PlayerMode,
} from "./game/constants.js";

export function Game() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  // Physics state lives in refs (60fps, no re-renders)
  const posXRef = useRef(CANVAS_WIDTH / 2);
  const posYRef = useRef(INITIAL_ALTITUDE);
  const velXRef = useRef(0);
  const velYRef = useRef(0);
  const modeRef = useRef<PlayerMode>("descending");
  const landerXRef = useRef(0);
  const landerYRef = useRef(0);

  // Mirrored into state purely for data-attribute exposure
  const [exposed, setExposed] = useState({
    mode: "descending" as PlayerMode,
    px: CANVAS_WIDTH / 2,
    py: INITIAL_ALTITUDE,
    vy: 0,
    lx: 0,
    ly: 0,
  });

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let lastTime = performance.now();
    let rafId = 0;

    const gameLoop = (now: number) => {
      const dt = Math.min((now - lastTime) / 1000, 0.05); // Cap delta to avoid spiral
      lastTime = now;

      // --- Physics ---
      if (modeRef.current === "descending") {
        velYRef.current += GRAVITY * dt;
        posXRef.current += velXRef.current * dt;
        posYRef.current += velYRef.current * dt;
      }

      // --- Render ---
      ctx.fillStyle = "#0a0a0f";
      ctx.fillRect(0, 0, CANVAS_WIDTH, CANVAS_HEIGHT);

      rafId = requestAnimationFrame(gameLoop);
    };
    rafId = requestAnimationFrame(gameLoop);

    // Sync exposed state periodically so data attributes update
    const syncId = setInterval(() => {
      setExposed({
        mode: modeRef.current,
        px: posXRef.current,
        py: posYRef.current,
        vy: velYRef.current,
        lx: landerXRef.current,
        ly: landerYRef.current,
      });
    }, 50);

    return () => {
      cancelAnimationFrame(rafId);
      clearInterval(syncId);
    };
  }, []);

  return (
    <div
      data-testid="game-container"
      data-player-mode={exposed.mode}
      data-player-x={exposed.px}
      data-player-y={exposed.py}
      data-velocity-y={exposed.vy}
      data-lander-x={exposed.lx}
      data-lander-y={exposed.ly}
    >
      <canvas
        ref={canvasRef}
        data-testid="game-canvas"
        width={CANVAS_WIDTH}
        height={CANVAS_HEIGHT}
      />
    </div>
  );
}
