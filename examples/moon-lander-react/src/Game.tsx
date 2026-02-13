import { useRef, useEffect, useState } from "react";
import {
  CANVAS_WIDTH,
  CANVAS_HEIGHT,
  INITIAL_ALTITUDE,
  GRAVITY,
  GROUND_LEVEL,
  THRUST_POWER,
  THRUST_POWER_X,
  MAX_LANDING_VELOCITY,
  type PlayerMode,
} from "./game/constants.js";
import { drawBackground } from "./game/render.js";

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

  // Track which keys are currently held
  const keysRef = useRef(new Set<string>());

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => keysRef.current.add(e.code);
    const onKeyUp = (e: KeyboardEvent) => keysRef.current.delete(e.code);
    document.addEventListener("keydown", onKeyDown);
    document.addEventListener("keyup", onKeyUp);
    return () => {
      document.removeEventListener("keydown", onKeyDown);
      document.removeEventListener("keyup", onKeyUp);
    };
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    // Initial draw so pixels are available immediately (before first rAF)
    drawBackground(ctx, posXRef.current - CANVAS_WIDTH / 2);

    let lastTime = performance.now();
    let rafId = 0;

    const gameLoop = (now: number) => {
      const dt = Math.min((now - lastTime) / 1000, 0.05); // Cap delta to avoid spiral
      lastTime = now;
      const keys = keysRef.current;

      // --- Physics ---
      if (modeRef.current === "descending") {
        // Thrust
        if (keys.has("ArrowUp") || keys.has("KeyW")) {
          velYRef.current -= THRUST_POWER * dt;
        }
        if (keys.has("ArrowLeft") || keys.has("KeyA")) {
          velXRef.current -= THRUST_POWER_X * dt;
        }
        if (keys.has("ArrowRight") || keys.has("KeyD")) {
          velXRef.current += THRUST_POWER_X * dt;
        }

        velYRef.current += GRAVITY * dt;
        posXRef.current += velXRef.current * dt;
        posYRef.current += velYRef.current * dt;

        // Landing detection
        if (posYRef.current >= GROUND_LEVEL) {
          posYRef.current = GROUND_LEVEL;
          if (Math.abs(velYRef.current) <= MAX_LANDING_VELOCITY) {
            modeRef.current = "landed";
          }
          // TODO: crash if too fast
          velXRef.current = 0;
          velYRef.current = 0;
          landerXRef.current = posXRef.current;
          landerYRef.current = GROUND_LEVEL;
        }
      }

      // --- Render ---
      drawBackground(ctx, posXRef.current - CANVAS_WIDTH / 2);

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
