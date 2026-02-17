import { useRef, useEffect, useState, useCallback } from "react";
import {
  CANVAS_WIDTH,
  CANVAS_HEIGHT,
  INITIAL_ALTITUDE,
  GROUND_LEVEL,
  INITIAL_FUEL,
  type FuelType,
} from "./constants.js";
import { drawBackground } from "./render.js";
import { generateDeposits } from "./world.js";
import { mergeInventory } from "./inventory.js";
import { updatePhysics } from "./physics.js";
import { renderScene } from "./scene.js";
import { createParticlePool } from "./particles.js";
import type { ArcAnimation, Deposit, RemotePlayerView, EngineState, GameWorld } from "./types.js";

export type { Deposit, RemotePlayerView, EngineState } from "./types.js";

// ---------------------------------------------------------------------------
// useGameEngine — orchestrates physics, camera, and rendering on a canvas
// ---------------------------------------------------------------------------

export function useGameEngine(
  canvasRef: React.RefObject<HTMLCanvasElement | null>,
  options?: {
    physicsSpeed?: number;
    requiredFuelType?: FuelType;
    remotePlayers?: RemotePlayerView[];
    deposits?: Deposit[];
    inventory?: FuelType[];
    onCollectDeposit?: (id: string) => void;
    onRefuel?: (fuelType: FuelType) => void;
    onShareFuel?: (fuelType: string, receiverPlayerId: string) => void;
    onBurstDeposit?: (fuelType: string, newX: number) => void;
    chatMessages?: Array<{ id: string; playerId: string; message: string; createdAt: number }>;
    localPlayerId?: string;
    localPlayerName?: string;
    localPlayerColor?: string;
    chatOpenRef?: React.RefObject<boolean>;
  },
): EngineState {
  const physicsSpeed = options?.physicsSpeed ?? 1;
  const requiredFuelType = options?.requiredFuelType ?? "circle";

  const sizeRef = useRef({ w: CANVAS_WIDTH, h: CANVAS_HEIGHT });

  // Consolidated mutable world state (replaces 9 individual refs)
  const worldRef = useRef<GameWorld>({
    posX: CANVAS_WIDTH / 2,
    posY: INITIAL_ALTITUDE,
    velX: 0,
    velY: 0,
    mode: "descending",
    landerX: 0,
    landerY: 0,
    fuel: INITIAL_FUEL,
    launchElapsed: 0,
  });

  // Camera smoothing
  const smoothCamYRef = useRef(NaN); // NaN = snap on first frame

  // Remote players (updated from props via ref so the game loop sees latest)
  const remotePlayersRef = useRef<RemotePlayerView[]>([]);
  const smoothedRemotesRef = useRef<Map<string, { x: number; y: number }>>(new Map());

  // Fuel deposits and inventory
  const isConnected = options?.deposits !== undefined;
  const externalDepositsRef = useRef<Deposit[]>([]);
  const localDepositsRef = useRef<Deposit[]>(generateDeposits(requiredFuelType, CANVAS_WIDTH / 2));
  const onCollectDepositRef = useRef(options?.onCollectDeposit);
  onCollectDepositRef.current = options?.onCollectDeposit;
  const onRefuelRef = useRef(options?.onRefuel);
  onRefuelRef.current = options?.onRefuel;
  const onShareFuelRef = useRef(options?.onShareFuel);
  onShareFuelRef.current = options?.onShareFuel;
  const onBurstDepositRef = useRef(options?.onBurstDeposit);
  onBurstDepositRef.current = options?.onBurstDeposit;

  // Chat + local player identity
  const chatMessagesRef = useRef(options?.chatMessages ?? []);
  chatMessagesRef.current = options?.chatMessages ?? [];
  const localPlayerIdRef = useRef(options?.localPlayerId ?? "");
  localPlayerIdRef.current = options?.localPlayerId ?? "";
  const localPlayerNameRef = useRef(options?.localPlayerName ?? "");
  localPlayerNameRef.current = options?.localPlayerName ?? "";
  const localPlayerColorRef = useRef(options?.localPlayerColor ?? "");
  localPlayerColorRef.current = options?.localPlayerColor ?? "";
  const chatOpenRef = options?.chatOpenRef ?? useRef(false);

  // Keep external deposits ref in sync with latest props
  if (isConnected) {
    externalDepositsRef.current = options.deposits!;
  }
  const depositsRef = isConnected ? externalDepositsRef : localDepositsRef;

  // Inventory
  const inventoryRef = useRef<Set<FuelType>>(new Set());
  const optimisticInventoryRef = useRef<Set<FuelType>>(new Set());
  const collectedIdsRef = useRef<Set<string>>(new Set());
  const arcsRef = useRef<ArcAnimation[]>([]);
  const particlesRef = useRef(createParticlePool());
  const prevExternalInventoryRef = useRef<Set<FuelType>>(new Set());
  const sharedOutRef = useRef<Set<FuelType>>(new Set());
  const shareHintRef = useRef(false);

  // Merge Jazz inventory into the working set each render (connected mode)
  if (options?.inventory !== undefined) {
    const result = mergeInventory({
      jazzInventory: options.inventory,
      optimistic: optimisticInventoryRef.current,
      sharedOut: sharedOutRef.current,
      collectedIds: collectedIdsRef.current,
      prevJazzInventory: prevExternalInventoryRef.current,
      externalDeposits: externalDepositsRef.current,
      remotePlayers: remotePlayersRef.current,
      playerX: worldRef.current.posX,
    });

    inventoryRef.current = result.merged;
    sharedOutRef.current = result.sharedOut;
    prevExternalInventoryRef.current = result.prevJazzInventory;
    for (const arc of result.newArcs) arcsRef.current.push(arc);
    for (const id of result.collectedIdsToRemove) collectedIdsRef.current.delete(id);
  }

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
    shareHint: false,
  });

  // Keep remote players ref in sync with latest props
  remotePlayersRef.current = options?.remotePlayers ?? [];

  // Track which keys are currently held + one-shot action queue
  const keysRef = useRef(new Set<string>());
  const actionsRef = useRef<string[]>([]);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (chatOpenRef.current) return; // suppress game keys while chatting
      const fresh = !keysRef.current.has(e.code);
      keysRef.current.add(e.code);
      if (e.code === "KeyE") actionsRef.current.push("interact");
      if (e.code === "Space") actionsRef.current.push("launch");
      if (fresh && (e.code === "Space" || e.code === "KeyW")) actionsRef.current.push("jump");
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
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
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
    const world = worldRef.current;
    const initCamY = Math.floor(world.posY - h / 2);
    drawBackground(ctx, world.posX - w / 2, initCamY, w, h);

    let lastTime = performance.now();
    let rafId = 0;

    const gameLoop = (now: number) => {
      const rawDt = Math.min((now - lastTime) / 1000, 0.05);
      const dt = rawDt * physicsSpeed;
      lastTime = now;
      const keys = keysRef.current;
      const { w, h } = sizeRef.current;
      const world = worldRef.current;

      // --- Read input ---
      const actions = actionsRef.current.splice(0);
      const input = {
        left: keys.has("ArrowLeft") || keys.has("KeyA"),
        right: keys.has("ArrowRight") || keys.has("KeyD"),
        up: keys.has("ArrowUp") || keys.has("KeyW"),
        interact: actions.includes("interact"),
        launch: actions.includes("launch"),
        jump: actions.includes("jump"),
      };

      // --- Physics ---
      const collectEffects: Array<{ x: number; fuelType: import("./constants.js").FuelType; isRequired: boolean }> = [];
      const { thrusting, thrustLeft, thrustRight } = updatePhysics(world, input, {
        dt,
        requiredFuelType,
        deposits: depositsRef.current,
        collectedIds: collectedIdsRef.current,
        inventory: inventoryRef.current,
        optimisticInventory: optimisticInventoryRef.current,
        sharedOut: sharedOutRef.current,
        remotePlayers: remotePlayersRef.current,
        arcs: arcsRef.current,
        collectEffects,
        callbacks: {
          onCollectDeposit: (id) => onCollectDepositRef.current?.(id),
          onRefuel: (ft) => onRefuelRef.current?.(ft),
          onShareFuel: (ft, rpId) => onShareFuelRef.current?.(ft, rpId),
          onBurstDeposit: (ft, x) => onBurstDepositRef.current?.(ft, x),
        },
      });

      // --- Camera (smoothed) ---
      const cameraX = Math.floor(world.posX - w / 2);
      const GROUND_MARGIN = 80;

      let targetCamY: number;
      if (world.mode === "launched") {
        // Bias: lander starts in lower third, gradually centres over 3s
        const launchBias = Math.max(0, 1 - world.launchElapsed / 3) * (h / 3);
        targetCamY = world.posY - h / 2 + launchBias;
      } else if (world.mode === "descending") {
        targetCamY = world.posY - h / 2;
      } else {
        // Ground modes — lock ground near bottom
        targetCamY = GROUND_LEVEL - h + GROUND_MARGIN;
      }

      if (isNaN(smoothCamYRef.current)) {
        smoothCamYRef.current = targetCamY;
      }
      const camLerp = world.mode === "launched" ? 0.7 : 5;
      smoothCamYRef.current += (targetCamY - smoothCamYRef.current) * Math.min(1, camLerp * dt);
      // Never dip downward during launch — camera may only move up
      if (world.mode === "launched") {
        smoothCamYRef.current = Math.min(smoothCamYRef.current, targetCamY);
      }
      const cameraY = Math.floor(smoothCamYRef.current);

      // --- Render ---
      const sceneResult = renderScene({
        ctx,
        w,
        h,
        cameraX,
        cameraY,
        groundScreenY: GROUND_LEVEL - cameraY,
        dt: rawDt,
        now: now / 1000, // monotonic seconds
        world,
        thrusting,
        localPlayerName: localPlayerNameRef.current,
        localPlayerColor: localPlayerColorRef.current,
        deposits: depositsRef.current,
        collectedIds: collectedIdsRef.current,
        requiredFuelType,
        inventory: inventoryRef.current,
        arcs: arcsRef.current,
        remotePlayers: remotePlayersRef.current,
        smoothedRemotes: smoothedRemotesRef.current,
        chatMessages: chatMessagesRef.current,
        localPlayerId: localPlayerIdRef.current,
        particles: particlesRef.current,
        thrustLeft,
        thrustRight,
        collectEffects,
        walkingInput: world.mode === "walking" && (input.left || input.right),
      });
      shareHintRef.current = sceneResult.shareHint;

      rafId = requestAnimationFrame(gameLoop);
    };
    rafId = requestAnimationFrame(gameLoop);

    // Sync exposed state periodically so data attributes + HUD update.
    const prevRef = {
      mode: "" as string,
      positionX: NaN,
      positionY: NaN,
      velocityX: NaN,
      velocityY: NaN,
      landerX: NaN,
      landerY: NaN,
      fuel: NaN,
      depositCount: NaN,
      inventoryKey: "",
      remotePlayerCount: NaN,
      shareHint: false,
    };
    const syncId = setInterval(() => {
      const world = worldRef.current;
      const mode = world.mode;
      const positionX = world.posX;
      const positionY = world.posY;
      const velocityX = world.velX;
      const velocityY = world.velY;
      const landerX = world.landerX;
      const landerY = world.landerY;
      const fuel = world.fuel;
      const remotePlayerCount = remotePlayersRef.current.length;
      const shareHint = shareHintRef.current;

      let depositCount = 0;
      for (const d of depositsRef.current) {
        if (!collectedIdsRef.current.has(d.id)) depositCount++;
      }

      const inventoryArr = [...inventoryRef.current];
      const inventoryKey = inventoryArr.join(",");

      // Skip setState if nothing changed
      if (
        mode === prevRef.mode &&
        positionX === prevRef.positionX &&
        positionY === prevRef.positionY &&
        velocityX === prevRef.velocityX &&
        velocityY === prevRef.velocityY &&
        landerX === prevRef.landerX &&
        landerY === prevRef.landerY &&
        fuel === prevRef.fuel &&
        depositCount === prevRef.depositCount &&
        inventoryKey === prevRef.inventoryKey &&
        remotePlayerCount === prevRef.remotePlayerCount &&
        shareHint === prevRef.shareHint
      ) {
        return;
      }

      prevRef.mode = mode;
      prevRef.positionX = positionX;
      prevRef.positionY = positionY;
      prevRef.velocityX = velocityX;
      prevRef.velocityY = velocityY;
      prevRef.landerX = landerX;
      prevRef.landerY = landerY;
      prevRef.fuel = fuel;
      prevRef.depositCount = depositCount;
      prevRef.inventoryKey = inventoryKey;
      prevRef.remotePlayerCount = remotePlayerCount;
      prevRef.shareHint = shareHint;

      setState({
        mode,
        positionX,
        positionY,
        velocityX,
        velocityY,
        landerX,
        landerY,
        fuel,
        depositCount,
        inventory: inventoryArr,
        remotePlayerCount,
        shareHint,
      });
    }, 50);

    return () => {
      cancelAnimationFrame(rafId);
      clearInterval(syncId);
    };
  }, []);

  return state;
}
