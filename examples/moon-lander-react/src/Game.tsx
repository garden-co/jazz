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
  FUEL_TYPES,
  COLOURS,
  type PlayerMode,
  type FuelType,
} from "./game/constants.js";
import { drawBackground, drawLander, drawAstronaut } from "./game/render.js";

interface GameProps {
  physicsSpeed?: number;
}

/** Get or create a stable player ID persisted in localStorage. */
function getOrCreatePlayerId(): string {
  const KEY = "moon-lander-player-id";
  const existing = localStorage.getItem(KEY);
  if (existing) return existing;
  const id = crypto.randomUUID();
  localStorage.setItem(KEY, id);
  return id;
}

/** Simple hash of a string to a number (for deterministic derivation). */
function hashCode(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

const PLAYER_NAMES = [
  "Armstrong", "Aldrin", "Collins", "Shepard", "Glenn",
  "Ride", "Jemison", "Tereshkova", "Gagarin", "Leonov",
  "Bean", "Conrad", "Lovell", "Swigert", "Haise",
  "Cernan", "Schmitt", "Duke", "Young", "Scott",
];

const PLAYER_COLOURS = [
  "#ff00ff", "#00ffff", "#ff6600", "#00ff00",
  "#ff66ff", "#8b00ff", "#ffff00", "#ff3366",
];

/** Derive deterministic player properties from a player ID. */
function derivePlayerProps(id: string) {
  const h = hashCode(id);
  return {
    name: PLAYER_NAMES[h % PLAYER_NAMES.length],
    color: PLAYER_COLOURS[h % PLAYER_COLOURS.length],
    requiredFuelType: FUEL_TYPES[h % FUEL_TYPES.length] as FuelType,
  };
}

export function Game({ physicsSpeed = 1 }: GameProps) {
  const playerId = useRef(getOrCreatePlayerId()).current;
  const playerProps = useRef(derivePlayerProps(playerId)).current;

  const canvasRef = useRef<HTMLCanvasElement>(null);
  const sizeRef = useRef({ w: CANVAS_WIDTH, h: CANVAS_HEIGHT });

  // Physics state lives in refs (60fps, no re-renders)
  const posXRef = useRef(CANVAS_WIDTH / 2);
  const posYRef = useRef(INITIAL_ALTITUDE);
  const velXRef = useRef(0);
  const velYRef = useRef(0);
  const modeRef = useRef<PlayerMode>("descending");
  const landerXRef = useRef(0);
  const landerYRef = useRef(0);
  const fuelRef = useRef(INITIAL_FUEL);

  // Mirrored into state purely for data-attribute exposure + HUD
  const [exposed, setExposed] = useState({
    mode: "descending" as PlayerMode,
    px: CANVAS_WIDTH / 2,
    py: INITIAL_ALTITUDE,
    vx: 0,
    vy: 0,
    lx: 0,
    ly: 0,
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
      setExposed({
        mode: modeRef.current,
        px: posXRef.current,
        py: posYRef.current,
        vx: velXRef.current,
        vy: velYRef.current,
        lx: landerXRef.current,
        ly: landerYRef.current,
        fuel: fuelRef.current,
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
      data-player-id={playerId}
      data-player-name={playerProps.name}
      data-player-color={playerProps.color}
      data-required-fuel={playerProps.requiredFuelType}
      data-lander-fuel={exposed.fuel}
      data-player-mode={exposed.mode}
      data-player-x={exposed.px}
      data-player-y={exposed.py}
      data-velocity-y={exposed.vy}
      data-lander-x={exposed.lx}
      data-lander-y={exposed.ly}
      style={{ position: "relative", width: "100vw", height: "100vh" }}
    >
      <canvas
        ref={canvasRef}
        data-testid="game-canvas"
        style={{ display: "block" }}
      />

      {/* HUD */}
      <div
        style={{
          position: "absolute",
          top: 12,
          left: 12,
          fontFamily: "monospace",
          fontSize: 13,
          color: COLOURS.cyan,
          background: "rgba(10, 10, 15, 0.7)",
          padding: "8px 12px",
          borderRadius: 4,
          lineHeight: 1.6,
          pointerEvents: "none",
        }}
      >
        <div>
          mode: <span style={{ color: COLOURS.pink }}>{exposed.mode}</span>
        </div>
        <div>
          pos: {Math.floor(exposed.px)}, {Math.floor(exposed.py)}
        </div>
        <div>
          vel: {exposed.vx.toFixed(1)}, {exposed.vy.toFixed(1)}
        </div>
        <div>
          fuel: <span style={{ color: exposed.fuel > 10 ? COLOURS.green : COLOURS.orange }}>{exposed.fuel}</span>
        </div>
        {exposed.mode === "walking" && (
          <div>
            lander: {Math.floor(exposed.lx)} (dist:{" "}
            {Math.floor(Math.abs(exposed.px - exposed.lx))})
          </div>
        )}
      </div>

      {/* Controls hint */}
      <div
        style={{
          position: "absolute",
          bottom: 12,
          left: "50%",
          transform: "translateX(-50%)",
          fontFamily: "monospace",
          fontSize: 12,
          color: "rgba(255, 255, 255, 0.4)",
          pointerEvents: "none",
        }}
      >
        {exposed.mode === "descending" && "Arrow keys / WASD — thrust"}
        {exposed.mode === "landed" && "Press E to exit lander"}
        {exposed.mode === "in_lander" && "Press E to exit lander"}
        {exposed.mode === "walking" && "A/D — walk | E — enter lander (when near)"}
      </div>
    </div>
  );
}
