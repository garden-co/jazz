import { useRef, useEffect, useState, useCallback } from "react";
import {
  CANVAS_WIDTH,
  CANVAS_HEIGHT,
  INITIAL_ALTITUDE,
  GRAVITY,
  GROUND_LEVEL,
  THRUST_POWER,
  THRUST_POWER_X,
  MAX_LANDING_VELOCITY,
  WALK_SPEED,
  LANDER_INTERACT_RADIUS,
  INITIAL_FUEL,
  FUEL_BURN_Y,
  FUEL_BURN_X,
  type PlayerMode,
} from "./constants.js";
import { drawBackground, drawLander, drawAstronaut } from "./render.js";

// ---------------------------------------------------------------------------
// Engine state — the snapshot exposed to React each tick
// ---------------------------------------------------------------------------

export interface EngineState {
  mode: PlayerMode;
  positionX: number;
  positionY: number;
  velocityX: number;
  velocityY: number;
  landerX: number;
  landerY: number;
  fuel: number;
}

// ---------------------------------------------------------------------------
// useGameEngine — runs physics, input, camera, and rendering on a canvas
// ---------------------------------------------------------------------------

export function useGameEngine(
  canvasRef: React.RefObject<HTMLCanvasElement | null>,
  options?: { physicsSpeed?: number },
): EngineState {
  const physicsSpeed = options?.physicsSpeed ?? 1;

  const sizeRef = useRef({ w: CANVAS_WIDTH, h: CANVAS_HEIGHT });

  // Physics state lives in refs (60fps mutation, no re-renders)
  const posXRef = useRef(CANVAS_WIDTH / 2);
  const posYRef = useRef(INITIAL_ALTITUDE);
  const velXRef = useRef(0);
  const velYRef = useRef(0);
  const modeRef = useRef<PlayerMode>("descending");
  const landerXRef = useRef(0);
  const landerYRef = useRef(0);
  const fuelRef = useRef(INITIAL_FUEL);

  // Mirrored into state for external consumption (HUD, data attributes, Jazz)
  const [state, setState] = useState<EngineState>({
    mode: "descending",
    positionX: CANVAS_WIDTH / 2,
    positionY: INITIAL_ALTITUDE,
    velocityX: 0,
    velocityY: 0,
    landerX: 0,
    landerY: 0,
    fuel: INITIAL_FUEL,
  });

  // Track which keys are currently held + one-shot action queue
  const keysRef = useRef(new Set<string>());
  const actionsRef = useRef<string[]>([]);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      keysRef.current.add(e.code);
      if (e.code === "KeyE") actionsRef.current.push("interact");
    };
    const onKeyUp = (e: KeyboardEvent) => keysRef.current.delete(e.code);
    document.addEventListener("keydown", onKeyDown);
    document.addEventListener("keyup", onKeyUp);
    return () => {
      document.removeEventListener("keydown", onKeyDown);
      document.removeEventListener("keyup", onKeyUp);
    };
  }, []);

  // Resize canvas to fill viewport
  const resize = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    canvas.width = Math.max(window.innerWidth, CANVAS_WIDTH);
    canvas.height = Math.max(window.innerHeight, CANVAS_HEIGHT);
    sizeRef.current = { w: canvas.width, h: canvas.height };
  }, []);

  useEffect(() => {
    resize();
    window.addEventListener("resize", resize);
    return () => window.removeEventListener("resize", resize);
  }, [resize]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    // Initial draw
    const { w, h } = sizeRef.current;
    const initCamY = Math.floor(posYRef.current - h / 2);
    drawBackground(ctx, posXRef.current - w / 2, initCamY, w, h);

    let lastTime = performance.now();
    let rafId = 0;

    const gameLoop = (now: number) => {
      const rawDt = Math.min((now - lastTime) / 1000, 0.05);
      const dt = rawDt * physicsSpeed;
      lastTime = now;
      const keys = keysRef.current;
      const { w, h } = sizeRef.current;

      // --- Process one-shot actions ---
      const actions = actionsRef.current.splice(0);
      const wantsInteract = actions.includes("interact");

      const thrusting =
        modeRef.current === "descending" &&
        (keys.has("ArrowUp") || keys.has("KeyW"));

      // --- Physics ---
      if (modeRef.current === "descending") {
        const hasFuel = fuelRef.current > 0;
        if (hasFuel && (keys.has("ArrowUp") || keys.has("KeyW"))) {
          velYRef.current -= THRUST_POWER * dt;
          fuelRef.current = Math.max(0, fuelRef.current - FUEL_BURN_Y * dt);
        }
        if (hasFuel && (keys.has("ArrowLeft") || keys.has("KeyA"))) {
          velXRef.current -= THRUST_POWER_X * dt;
          fuelRef.current = Math.max(0, fuelRef.current - FUEL_BURN_X * dt);
        }
        if (hasFuel && (keys.has("ArrowRight") || keys.has("KeyD"))) {
          velXRef.current += THRUST_POWER_X * dt;
          fuelRef.current = Math.max(0, fuelRef.current - FUEL_BURN_X * dt);
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
          velXRef.current = 0;
          velYRef.current = 0;
          landerXRef.current = posXRef.current;
          landerYRef.current = GROUND_LEVEL;
        }
      } else if (modeRef.current === "landed" || modeRef.current === "in_lander") {
        if (wantsInteract) {
          modeRef.current = "walking";
        }
      } else if (modeRef.current === "walking") {
        if (keys.has("ArrowLeft") || keys.has("KeyA")) {
          posXRef.current -= WALK_SPEED * dt;
        }
        if (keys.has("ArrowRight") || keys.has("KeyD")) {
          posXRef.current += WALK_SPEED * dt;
        }

        if (wantsInteract) {
          const dist = Math.abs(posXRef.current - landerXRef.current);
          if (dist <= LANDER_INTERACT_RADIUS) {
            modeRef.current = "in_lander";
            posXRef.current = landerXRef.current;
          }
        }
      }

      // --- Camera ---
      const cameraX = Math.floor(posXRef.current - w / 2);
      // Vertical: follow player during descent, lock ground near bottom after landing
      const GROUND_MARGIN = 80; // pixels of ground visible below surface line
      let cameraY: number;
      if (modeRef.current === "descending") {
        // Centre on player vertically
        cameraY = Math.floor(posYRef.current - h / 2);
      } else {
        // Lock so ground is near the bottom
        cameraY = Math.floor(GROUND_LEVEL - h + GROUND_MARGIN);
      }

      // --- Render ---
      drawBackground(ctx, cameraX, cameraY, w, h);

      // Draw parked lander (if we've landed and are walking)
      if (
        modeRef.current === "walking" &&
        landerXRef.current !== 0
      ) {
        const landerScreenX = landerXRef.current - cameraX;
        const landerScreenY = GROUND_LEVEL - cameraY;
        drawLander(ctx, landerScreenX, landerScreenY, false);
      }

      // Draw player
      const screenX = posXRef.current - cameraX;
      if (modeRef.current === "descending") {
        const screenY = posYRef.current - cameraY;
        drawLander(ctx, screenX, screenY, thrusting);
      } else if (modeRef.current === "landed" || modeRef.current === "in_lander") {
        const screenY = GROUND_LEVEL - cameraY;
        drawLander(ctx, screenX, screenY, false);
      } else if (modeRef.current === "walking") {
        const screenY = GROUND_LEVEL - cameraY;
        drawAstronaut(ctx, screenX, screenY);
      }

      rafId = requestAnimationFrame(gameLoop);
    };
    rafId = requestAnimationFrame(gameLoop);

    // Sync exposed state periodically so data attributes + HUD update
    const syncId = setInterval(() => {
      setState({
        mode: modeRef.current,
        positionX: posXRef.current,
        positionY: posYRef.current,
        velocityX: velXRef.current,
        velocityY: velYRef.current,
        landerX: landerXRef.current,
        landerY: landerYRef.current,
        fuel: fuelRef.current,
      });
    }, 50);

    return () => {
      cancelAnimationFrame(rafId);
      clearInterval(syncId);
    };
  }, []);

  return state;
}
