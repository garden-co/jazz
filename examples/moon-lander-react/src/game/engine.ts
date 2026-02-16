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
  MAX_FUEL,
  REFUEL_AMOUNT,
  MOON_SURFACE_WIDTH,
  FUEL_TYPES,
  ASTRONAUT_WIDTH,
  type PlayerMode,
  type FuelType,
} from "./constants.js";
import { drawBackground, drawLander, drawAstronaut, drawDeposit, drawArrow, drawSplash, DEPOSIT_COLOURS } from "./render.js";

// ---------------------------------------------------------------------------
// World wrapping — the moon is round
// ---------------------------------------------------------------------------

/** Wrap an x coordinate into [0, MOON_SURFACE_WIDTH). */
function wrapX(x: number): number {
  return ((x % MOON_SURFACE_WIDTH) + MOON_SURFACE_WIDTH) % MOON_SURFACE_WIDTH;
}

/** Shortest distance between two x positions on the wrapping surface. */
function wrapDistance(a: number, b: number): number {
  const direct = Math.abs(a - b);
  return Math.min(direct, MOON_SURFACE_WIDTH - direct);
}

/** Lerp an X position toward a target, taking the shortest wrapping path. */
function wrapLerp(current: number, target: number, t: number): number {
  let diff = target - current;
  if (diff > MOON_SURFACE_WIDTH / 2) diff -= MOON_SURFACE_WIDTH;
  if (diff < -MOON_SURFACE_WIDTH / 2) diff += MOON_SURFACE_WIDTH;
  return wrapX(current + diff * t);
}

/** Convert a world X to a screen X relative to the camera, with wrapping. */
function wrapScreenX(worldX: number, cameraX: number): number {
  let dx = worldX - cameraX;
  if (dx < -MOON_SURFACE_WIDTH / 2) dx += MOON_SURFACE_WIDTH;
  if (dx > MOON_SURFACE_WIDTH / 2) dx -= MOON_SURFACE_WIDTH;
  return dx;
}

// ---------------------------------------------------------------------------
// Fuel deposits — scattered across the moon surface
// ---------------------------------------------------------------------------

export interface Deposit {
  id: string;
  x: number;
  type: FuelType;
}

/** Deterministic pseudo-random (simple sine hash). */
function seededRand(seed: number): number {
  const x = Math.sin(seed * 127.1 + seed * 311.7) * 43758.5453;
  return x - Math.floor(x);
}

/**
 * Generate fuel deposits across the surface.
 * 3 of each fuel type spread evenly, plus 1 extra of the player's
 * required type placed 1/4–1/2 of the world away from the spawn point.
 * A no-spawn zone keeps deposits away from where the player lands.
 */
function generateDeposits(requiredFuelType: FuelType, spawnX: number): Deposit[] {
  const deposits: Deposit[] = [];
  const noSpawnRadius = 300;

  // 3 of each type, spread across the full surface
  for (let ti = 0; ti < FUEL_TYPES.length; ti++) {
    for (let i = 0; i < 3; i++) {
      const seed = ti * 100 + i;
      let x = seededRand(seed) * MOON_SURFACE_WIDTH;
      // Push deposits out of the landing zone
      if (wrapDistance(x, spawnX) < noSpawnRadius) {
        x = wrapX(spawnX + noSpawnRadius + seededRand(seed + 0.7) * 1000);
      }
      deposits.push({ id: String(deposits.length), x, type: FUEL_TYPES[ti] });
    }
  }

  // 1 extra of the required type, placed 1/4–1/2 world away
  const offset = MOON_SURFACE_WIDTH / 4 + seededRand(9999) * (MOON_SURFACE_WIDTH / 4);
  deposits.push({ id: String(deposits.length), x: wrapX(spawnX + offset), type: requiredFuelType });

  return deposits;
}

// ---------------------------------------------------------------------------
// Engine state — the snapshot exposed to React each tick
// ---------------------------------------------------------------------------

/** A remote player to render (already filtered for staleness). */
export interface RemotePlayerView {
  id: string;
  name: string;
  mode: string;
  positionX: number;
  positionY: number;
  velocityY: number;
  color: string;
  landerX?: number;
}

export interface EngineState {
  mode: PlayerMode;
  positionX: number;
  positionY: number;
  velocityX: number;
  velocityY: number;
  landerX: number;
  landerY: number;
  fuel: number;
  depositCount: number;
  inventory: string[];
  remotePlayerCount: number;
}

// ---------------------------------------------------------------------------
// useGameEngine — runs physics, input, camera, and rendering on a canvas
// ---------------------------------------------------------------------------

export function useGameEngine(
  canvasRef: React.RefObject<HTMLCanvasElement | null>,
  options?: {
    physicsSpeed?: number;
    requiredFuelType?: FuelType;
    remotePlayers?: RemotePlayerView[];
    deposits?: Deposit[];
    onCollectDeposit?: (id: string) => void;
  },
): EngineState {
  const physicsSpeed = options?.physicsSpeed ?? 1;
  const requiredFuelType = options?.requiredFuelType ?? "circle";

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

  // Camera smoothing
  const smoothCamYRef = useRef(NaN); // NaN = snap on first frame
  const launchElapsedRef = useRef(0);

  // Remote players (updated from props via ref so the game loop sees latest)
  const remotePlayersRef = useRef<RemotePlayerView[]>([]);
  const smoothedRemotesRef = useRef<Map<string, { x: number; y: number }>>(new Map());

  // Fuel deposits and inventory
  // Connected mode: external deposits via ref (updated from props)
  // Standalone mode: locally generated deposits
  const isConnected = options?.deposits !== undefined;
  const externalDepositsRef = useRef<Deposit[]>([]);
  const localDepositsRef = useRef<Deposit[]>(generateDeposits(requiredFuelType, CANVAS_WIDTH / 2));
  const onCollectDepositRef = useRef(options?.onCollectDeposit);
  onCollectDepositRef.current = options?.onCollectDeposit;

  // Keep external deposits ref in sync with latest props
  if (isConnected) {
    externalDepositsRef.current = options.deposits!;
  }

  // The active deposits list (connected uses external, standalone uses local)
  const depositsRef = isConnected ? externalDepositsRef : localDepositsRef;

  const inventoryRef = useRef<Set<FuelType>>(new Set());

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
    depositCount: depositsRef.current.length,
    inventory: [],
    remotePlayerCount: 0,
  });

  // Keep remote players ref in sync with latest props
  remotePlayersRef.current = options?.remotePlayers ?? [];

  // Track which keys are currently held + one-shot action queue
  const keysRef = useRef(new Set<string>());
  const actionsRef = useRef<string[]>([]);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      keysRef.current.add(e.code);
      if (e.code === "KeyE") actionsRef.current.push("interact");
      if (e.code === "Space") actionsRef.current.push("launch");
    };
    const onKeyUp = (e: KeyboardEvent) => keysRef.current.delete(e.code);
    const onBlur = () => keysRef.current.clear();
    document.addEventListener("keydown", onKeyDown);
    document.addEventListener("keyup", onKeyUp);
    window.addEventListener("blur", onBlur);
    return () => {
      document.removeEventListener("keydown", onKeyDown);
      document.removeEventListener("keyup", onKeyUp);
      window.removeEventListener("blur", onBlur);
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
      const wantsLaunch = actions.includes("launch");

      const thrusting =
        modeRef.current === "descending" &&
        fuelRef.current > 0 &&
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

        // Wrap horizontally
        posXRef.current = wrapX(posXRef.current);

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
        if (wantsLaunch && modeRef.current === "in_lander" && fuelRef.current >= MAX_FUEL) {
          modeRef.current = "launched";
        } else if (wantsInteract) {
          modeRef.current = "walking";
        }
      } else if (modeRef.current === "walking") {
        if (keys.has("ArrowLeft") || keys.has("KeyA")) {
          posXRef.current -= WALK_SPEED * dt;
        }
        if (keys.has("ArrowRight") || keys.has("KeyD")) {
          posXRef.current += WALK_SPEED * dt;
        }

        // Wrap horizontally
        posXRef.current = wrapX(posXRef.current);

        // Collect deposits the player walks over (skip types already owned)
        const pickupRange = ASTRONAUT_WIDTH;
        const collected: string[] = [];
        depositsRef.current = depositsRef.current.filter((d) => {
          if (wrapDistance(d.x, posXRef.current) < pickupRange && !inventoryRef.current.has(d.type)) {
            inventoryRef.current.add(d.type);
            collected.push(d.id);
            return false; // remove from surface
          }
          return true;
        });
        // Notify Jazz layer of collections (queued, not immediate)
        if (collected.length > 0 && onCollectDepositRef.current) {
          for (const id of collected) {
            onCollectDepositRef.current(id);
          }
        }

        if (wantsInteract) {
          if (wrapDistance(posXRef.current, landerXRef.current) <= LANDER_INTERACT_RADIUS) {
            modeRef.current = "in_lander";
            posXRef.current = landerXRef.current;

            // Refuel if carrying the correct fuel type
            if (inventoryRef.current.has(requiredFuelType)) {
              fuelRef.current = Math.min(MAX_FUEL, fuelRef.current + REFUEL_AMOUNT);
              inventoryRef.current.delete(requiredFuelType);
            }
          }
        }
      } else if (modeRef.current === "launched") {
        launchElapsedRef.current += dt;
        // Keep accelerating until well off-screen, then clamp position
        velYRef.current -= THRUST_POWER * 1.5 * dt;
        posYRef.current += velYRef.current * dt;
        fuelRef.current = Math.max(0, fuelRef.current - FUEL_BURN_Y * dt);
        // Clamp to prevent overflow (well above any camera position)
        if (posYRef.current < -100000) {
          posYRef.current = -100000;
          velYRef.current = 0;
        }
      }

      // --- Camera (smoothed) ---
      const cameraX = Math.floor(posXRef.current - w / 2);
      const GROUND_MARGIN = 80;

      let targetCamY: number;
      if (modeRef.current === "descending") {
        targetCamY = posYRef.current - h / 2;
      } else if (modeRef.current === "launched") {
        // Slow pan up to deep space — lander flies out of frame naturally
        targetCamY = INITIAL_ALTITUDE - h / 2;
      } else {
        // Ground modes — lock ground near bottom
        targetCamY = GROUND_LEVEL - h + GROUND_MARGIN;
      }

      // Lerp toward target (snap on first frame)
      if (isNaN(smoothCamYRef.current)) {
        smoothCamYRef.current = targetCamY;
      }
      const camLerp = modeRef.current === "launched" ? 1.5 : 5;
      smoothCamYRef.current += (targetCamY - smoothCamYRef.current) * Math.min(1, camLerp * dt);
      const cameraY = Math.floor(smoothCamYRef.current);

      // --- Render ---
      drawBackground(ctx, cameraX, cameraY, w, h);

      // Draw fuel deposits on the ground (with world wrapping)
      const groundScreenY = GROUND_LEVEL - cameraY;
      for (const dep of depositsRef.current) {
        const dx = wrapScreenX(dep.x, cameraX);
        if (dx > -20 && dx < w + 20) {
          drawDeposit(ctx, dx, groundScreenY, dep.type);
        }
      }

      // Draw parked lander (if we've landed and are walking)
      if (
        modeRef.current === "walking" &&
        landerXRef.current !== 0
      ) {
        const landerSX = wrapScreenX(landerXRef.current, cameraX);
        if (landerSX > -40 && landerSX < w + 40) {
          drawLander(ctx, landerSX, groundScreenY, false);
        }
      }

      // Smooth and draw remote players
      const smoothed = smoothedRemotesRef.current;
      const lerpT = Math.min(1, 8 * rawDt);
      const activeIds = new Set<string>();
      for (const rp of remotePlayersRef.current) {
        activeIds.add(rp.id);
        let s = smoothed.get(rp.id);
        if (!s) {
          s = { x: rp.positionX, y: rp.positionY };
          smoothed.set(rp.id, s);
        }
        s.x = wrapLerp(s.x, rp.positionX, lerpT);
        s.y += (rp.positionY - s.y) * lerpT;

        const rpSX = wrapScreenX(s.x, cameraX);
        if (rpSX < -60 || rpSX > w + 60) continue;

        if (rp.mode === "walking") {
          drawAstronaut(ctx, rpSX, groundScreenY, rp.color, rp.name);
          if (rp.landerX != null) {
            const rpLanderSX = wrapScreenX(rp.landerX, cameraX);
            if (rpLanderSX > -40 && rpLanderSX < w + 40) {
              drawLander(ctx, rpLanderSX, groundScreenY, false, rp.color);
            }
          }
        } else if (rp.mode === "descending") {
          const rpSY = s.y - cameraY;
          if (rpSY > -60 && rpSY < h + 60) {
            drawLander(ctx, rpSX, rpSY, rp.velocityY < 0, rp.color, rp.name);
          }
        } else {
          drawLander(ctx, rpSX, groundScreenY, false, rp.color, rp.name);
        }
      }
      // Clean up smoothed entries for players who left
      for (const id of smoothed.keys()) {
        if (!activeIds.has(id)) smoothed.delete(id);
      }

      // Draw player
      const screenX = posXRef.current - cameraX;
      if (modeRef.current === "descending") {
        const screenY = posYRef.current - cameraY;
        drawLander(ctx, screenX, screenY, thrusting);
      } else if (modeRef.current === "landed" || modeRef.current === "in_lander") {
        drawLander(ctx, screenX, groundScreenY, false);
      } else if (modeRef.current === "walking") {
        drawAstronaut(ctx, screenX, groundScreenY);
      } else if (modeRef.current === "launched") {
        const screenY = posYRef.current - cameraY;
        // Only draw lander while it's on-screen
        if (screenY > -60 && screenY < h + 60) {
          drawLander(ctx, screenX, screenY, launchElapsedRef.current < 3);
        }
      }

      // Success splash (fade in after 4s of launch)
      if (modeRef.current === "launched" && launchElapsedRef.current > 4) {
        const splashAlpha = Math.min(1, (launchElapsedRef.current - 4) * 0.8);
        drawSplash(ctx, w, h, splashAlpha);
      }

      // Arrows (only while walking)
      if (modeRef.current === "walking") {
        // Arrow to lander
        const landerSX = wrapScreenX(landerXRef.current, cameraX);
        const landerDist = Math.floor(wrapDistance(posXRef.current, landerXRef.current));
        drawArrow(ctx, landerSX, w, h, "#00ffff", `lander ${landerDist}`);

        // Arrow to nearest deposit of the required type
        let nearestDep: { sx: number; dist: number } | null = null;
        for (const dep of depositsRef.current) {
          if (dep.type !== requiredFuelType) continue;
          if (inventoryRef.current.has(dep.type)) continue;
          const dist = wrapDistance(posXRef.current, dep.x);
          if (!nearestDep || dist < nearestDep.dist) {
            nearestDep = { sx: wrapScreenX(dep.x, cameraX), dist };
          }
        }
        if (nearestDep) {
          drawArrow(ctx, nearestDep.sx, w, h, DEPOSIT_COLOURS[requiredFuelType], `fuel ${Math.floor(nearestDep.dist)}`);
        }
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
        depositCount: depositsRef.current.length,
        inventory: [...inventoryRef.current],
        remotePlayerCount: remotePlayersRef.current.length,
      });
    }, 50);

    return () => {
      cancelAnimationFrame(rafId);
      clearInterval(syncId);
    };
  }, []);

  return state;
}
