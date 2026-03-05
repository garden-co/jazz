import { useEffect, useRef, useState } from "react";
import type { Player } from "../../schema/app";
import {
  type FuelType,
  GROUND_LEVEL,
  INITIAL_ALTITUDE,
  INITIAL_FUEL,
  MOON_SURFACE_WIDTH,
} from "./constants";
import { mergeInventory } from "./inventory";
import { type Particle, createParticlePool } from "./particles";
import { updatePhysics } from "./physics";
import { renderScene } from "./scene";
import { createSpriteAnimationState, type SpriteAnimationState } from "./sprites";
import { createCameraTrackingState, type CameraTrackingState, drawBackground } from "./terrain";
import type { ArcAnimation, Deposit, EngineProps, EngineState, GameWorld } from "./types";
import { generateDeposits } from "./world";

export type { Deposit, EngineState } from "./types";

// ---------------------------------------------------------------------------
// GameEngine — owns all mutable game state outside React
// ---------------------------------------------------------------------------

export class GameEngine {
  // --- Props (pushed from React each render) ---
  props: EngineProps;

  // --- Canvas ---
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private size: { w: number; h: number };

  // --- World ---
  world: GameWorld;
  private smoothCamY = NaN; // NaN = snap on first frame

  // --- Remote players ---
  private smoothedRemotes = new Map<string, { x: number; y: number }>();

  // --- Deposits ---
  private externalDeposits: Deposit[] = [];
  private localDeposits: Deposit[];

  // --- Inventory ---
  private inventory = new Set<FuelType>();
  private collectedIds = new Set<string>();
  private arcs: ArcAnimation[] = [];
  private particles: Particle[];
  private sharedOut = new Set<FuelType>();
  shareHint = false;
  thrusting = false;

  // --- Per-instance animation/rendering state ---
  private spriteAnim: SpriteAnimationState;
  private cameraTracking: CameraTrackingState;

  // --- Input ---
  private keys = new Set<string>();
  private actions: string[] = [];

  // --- RAF ---
  private rafId = 0;
  private lastTime = performance.now();

  // --- Event listener refs (for cleanup) ---
  private onKeyDown: (e: KeyboardEvent) => void;
  private onKeyUp: (e: KeyboardEvent) => void;
  private onBlur: () => void;
  private onResize: () => void;

  constructor(canvas: HTMLCanvasElement, initialProps: EngineProps) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d")!;
    this.props = initialProps;

    // Resize to fill viewport
    canvas.width = window.innerWidth;
    canvas.height = window.innerHeight;
    this.size = { w: canvas.width, h: canvas.height };

    // Initialise world
    const startX = initialProps.spawnX ?? Math.floor(Math.random() * MOON_SURFACE_WIDTH);
    const grounded =
      initialProps.initialMode === "landed" ||
      initialProps.initialMode === "walking" ||
      initialProps.initialMode === "in_lander";
    this.world = {
      posX: startX,
      posY: grounded ? GROUND_LEVEL : INITIAL_ALTITUDE,
      velX: 0,
      velY: 0,
      mode: initialProps.initialMode,
      landerX: grounded ? startX : 0,
      fuel: INITIAL_FUEL,
      launchElapsed: 0,
      crashElapsed: 0,
    };

    // Initialise deposits, particles, and per-instance rendering state
    this.localDeposits = generateDeposits(initialProps.requiredFuelType, startX);
    this.particles = createParticlePool();
    this.spriteAnim = createSpriteAnimationState();
    this.cameraTracking = createCameraTrackingState();

    // Initial draw
    const { w, h } = this.size;
    const initCamY = Math.floor(this.world.posY - h / 2);
    drawBackground(this.ctx, this.world.posX - w / 2, initCamY, w, h);

    // Event listeners
    this.onKeyDown = (e: KeyboardEvent) => {
      if (e.code === "Enter") {
        if (this.props.chatOpen) {
          this.props.onChatSend?.();
        } else {
          e.preventDefault();
          this.props.onChatOpen?.();
        }
        return;
      }
      if (e.code === "Escape" && this.props.chatOpen) {
        this.props.onChatCancel?.();
        return;
      }
      if (this.props.chatOpen) return;
      const fresh = !this.keys.has(e.code);
      this.keys.add(e.code);
      if (e.code === "KeyE") this.actions.push("interact");
      if (e.code === "Space") this.actions.push("launch");
      if (fresh && (e.code === "Space" || e.code === "KeyW")) this.actions.push("jump");
    };
    this.onKeyUp = (e: KeyboardEvent) => this.keys.delete(e.code);
    this.onBlur = () => this.keys.clear();
    this.onResize = () => {
      this.canvas.width = window.innerWidth;
      this.canvas.height = window.innerHeight;
      this.size = { w: this.canvas.width, h: this.canvas.height };
    };

    document.addEventListener("keydown", this.onKeyDown);
    document.addEventListener("keyup", this.onKeyUp);
    window.addEventListener("blur", this.onBlur);
    window.addEventListener("resize", this.onResize);

    // Start RAF
    this.rafId = requestAnimationFrame(this.gameLoop);
  }

  setProps(props: EngineProps): void {
    this.props = props;
    if (props.deposits !== undefined) {
      this.externalDeposits = props.deposits;
    }
  }

  snapshot(): EngineState {
    const deposits = this.activeDeposits();
    let depositCount = 0;
    for (const d of deposits) {
      if (!this.collectedIds.has(d.id)) depositCount++;
    }
    return {
      mode: this.world.mode,
      positionX: this.world.posX,
      positionY: this.world.posY,
      velocityX: this.world.velX,
      velocityY: this.world.velY,
      landerX: this.world.landerX,
      fuel: this.world.fuel,
      thrusting: this.thrusting,
      depositCount,
      inventory: [...this.inventory],
      remotePlayerCount: this.props.remotePlayers.length,
      shareHint: this.shareHint,
    };
  }

  destroy(): void {
    cancelAnimationFrame(this.rafId);
    document.removeEventListener("keydown", this.onKeyDown);
    document.removeEventListener("keyup", this.onKeyUp);
    window.removeEventListener("blur", this.onBlur);
    window.removeEventListener("resize", this.onResize);
  }

  // --- Private ---

  private activeDeposits(): Deposit[] {
    return this.props.deposits !== undefined ? this.externalDeposits : this.localDeposits;
  }

  private mergeExternalInventory(): void {
    const { props, world } = this;
    if (props.inventory === undefined) return;
    const canHold =
      world.mode === "walking" ||
      world.mode === "landed" ||
      world.mode === "in_lander" ||
      world.mode === "launched";
    if (!canHold) return;

    const result = mergeInventory({
      jazzInventory: props.inventory,
      localInventory: this.inventory,
      sharedOut: this.sharedOut,
      collectedIds: this.collectedIds,
      externalDeposits: this.externalDeposits,
      remotePlayers: props.remotePlayers,
      playerX: world.posX,
    });

    this.sharedOut = result.sharedOut;
    for (const arc of result.newArcs) this.arcs.push(arc);
    for (const id of result.collectedIdsToRemove) this.collectedIds.delete(id);
  }

  private gameLoop = (now: number): void => {
    const rawDt = Math.min((now - this.lastTime) / 1000, 0.05);
    const dt = rawDt * this.props.physicsSpeed;
    this.lastTime = now;
    const { w, h } = this.size;
    const world = this.world;

    // --- Prune stale remote-player smoothing entries ---
    if (this.smoothedRemotes.size > 0) {
      const activeIds = new Set(this.props.remotePlayers.map((p) => p.playerId));
      for (const id of this.smoothedRemotes.keys()) {
        if (!activeIds.has(id)) this.smoothedRemotes.delete(id);
      }
    }

    // --- Inventory merge (idempotent, runs each frame) ---
    this.mergeExternalInventory();

    // --- Read input ---
    const actions = this.actions.splice(0);
    const input = {
      left: this.keys.has("ArrowLeft") || this.keys.has("KeyA"),
      right: this.keys.has("ArrowRight") || this.keys.has("KeyD"),
      up: this.keys.has("ArrowUp") || this.keys.has("KeyW"),
      interact: actions.includes("interact"),
      launch: actions.includes("launch"),
      jump: actions.includes("jump"),
    };

    // --- Physics ---
    const deposits = this.activeDeposits();
    const collectEffects: Array<{
      x: number;
      fuelType: FuelType;
      isRequired: boolean;
    }> = [];
    const { thrusting, thrustLeft, thrustRight } = updatePhysics(world, input, {
      dt,
      requiredFuelType: this.props.requiredFuelType,
      deposits,
      collectedIds: this.collectedIds,
      inventory: this.inventory,
      sharedOut: this.sharedOut,
      remotePlayers: this.props.remotePlayers,
      arcs: this.arcs,
      collectEffects,
      callbacks: {
        onCollectDeposit: (id) => this.props.onCollectDeposit?.(id),
        onRefuel: (ft) => this.props.onRefuel?.(ft),
        onShareFuel: (ft, rpId) => this.props.onShareFuel?.(ft, rpId),
        onBurstDeposit: (ft) => this.props.onBurstDeposit?.(ft),
      },
    });

    this.thrusting = thrusting;

    // --- Camera (smoothed) ---
    const cameraX = Math.floor(world.posX - w / 2);
    const GROUND_MARGIN = 80;

    const groundCamY = GROUND_LEVEL - h + GROUND_MARGIN;
    let targetCamY: number;
    if (world.mode === "launched") {
      targetCamY = world.posY - h / 2;
      targetCamY = Math.min(targetCamY, this.smoothCamY);
    } else if (world.mode === "descending" || world.mode === "start") {
      targetCamY = world.posY - h / 2;
    } else {
      targetCamY = groundCamY;
    }
    targetCamY = Math.min(targetCamY, groundCamY);

    if (isNaN(this.smoothCamY)) {
      this.smoothCamY = targetCamY;
    }
    const camLerp = world.mode === "launched" ? 3 * Math.max(0, 1 - world.launchElapsed / 1.5) : 5;
    this.smoothCamY += (targetCamY - this.smoothCamY) * Math.min(1, camLerp * dt);
    const cameraY = Math.floor(this.smoothCamY);

    // --- Render ---
    const sceneResult = renderScene({
      ctx: this.ctx,
      w,
      h,
      cameraX,
      cameraY,
      groundScreenY: GROUND_LEVEL - cameraY,
      dt: rawDt,
      now: now / 1000,
      world,
      thrusting,
      localPlayerName: this.props.localPlayerName,
      localPlayerColor: this.props.localPlayerColor,
      deposits,
      collectedIds: this.collectedIds,
      requiredFuelType: this.props.requiredFuelType,
      inventory: this.inventory,
      arcs: this.arcs,
      remotePlayers: this.props.remotePlayers,
      smoothedRemotes: this.smoothedRemotes,
      chatMessages: this.props.chatMessages,
      localPlayerId: this.props.localPlayerId,
      particles: this.particles,
      thrustLeft,
      thrustRight,
      collectEffects,
      walkingInput: world.mode === "walking" && (input.left || input.right),
      spriteAnim: this.spriteAnim,
      cameraTracking: this.cameraTracking,
    });
    this.shareHint = sceneResult.shareHint;

    this.rafId = requestAnimationFrame(this.gameLoop);
  };
}

// ---------------------------------------------------------------------------
// useGameEngine — thin React lifecycle wrapper around GameEngine
// ---------------------------------------------------------------------------

export function useGameEngine(
  canvasRef: React.RefObject<HTMLCanvasElement | null>,
  options?: {
    physicsSpeed?: number;
    initialMode?: import("./constants").PlayerMode;
    /** Override the random spawn X position (for tests). */
    spawnX?: number;
    requiredFuelType?: FuelType;
    remotePlayers?: Player[];
    deposits?: Deposit[];
    inventory?: FuelType[];
    onCollectDeposit?: (id: string) => void;
    onRefuel?: (fuelType: FuelType) => void;
    onShareFuel?: (fuelType: string, receiverPlayerId: string) => void;
    onBurstDeposit?: (fuelType: string) => void;
    chatMessages?: Array<{
      id: string;
      playerId: string;
      message: string;
      createdAt: number;
    }>;
    localPlayerId?: string;
    localPlayerName?: string;
    localPlayerColor?: string;
    chatOpen?: boolean;
    onChatOpen?: () => void;
    onChatSend?: () => void;
    onChatCancel?: () => void;
  },
): EngineState {
  const engineRef = useRef<GameEngine | null>(null);

  const initialMode = options?.initialMode ?? "start";
  const grounded =
    initialMode === "landed" || initialMode === "walking" || initialMode === "in_lander";

  const [state, setState] = useState<EngineState>({
    mode: initialMode,
    positionX: options?.spawnX ?? 0,
    positionY: grounded ? GROUND_LEVEL : INITIAL_ALTITUDE,
    velocityX: 0,
    velocityY: 0,
    landerX: 0,
    fuel: INITIAL_FUEL,
    thrusting: false,
    depositCount: 0,
    inventory: [],
    remotePlayerCount: 0,
    shareHint: false,
  });

  // Build props from options
  const props: EngineProps = {
    physicsSpeed: options?.physicsSpeed ?? 1,
    initialMode,
    spawnX: options?.spawnX,
    requiredFuelType: options?.requiredFuelType ?? "circle",
    remotePlayers: options?.remotePlayers ?? [],
    deposits: options?.deposits,
    inventory: options?.inventory,
    chatMessages: options?.chatMessages ?? [],
    localPlayerId: options?.localPlayerId ?? "",
    localPlayerName: options?.localPlayerName ?? "",
    localPlayerColor: options?.localPlayerColor ?? "",
    chatOpen: options?.chatOpen ?? false,
    onCollectDeposit: options?.onCollectDeposit,
    onRefuel: options?.onRefuel,
    onShareFuel: options?.onShareFuel,
    onBurstDeposit: options?.onBurstDeposit,
    onChatOpen: options?.onChatOpen,
    onChatSend: options?.onChatSend,
    onChatCancel: options?.onChatCancel,
  };

  // Push latest props into the engine each render
  if (engineRef.current) {
    engineRef.current.setProps(props);
  }

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const engine = new GameEngine(canvas, props);
    engineRef.current = engine;

    // Sync exposed state periodically so data attributes + HUD update
    let prevKey = "";
    const syncId = setInterval(() => {
      const next = engine.snapshot();
      const inventoryKey = next.inventory.join(",");
      const key = `${next.mode}|${next.positionX}|${next.positionY}|${next.velocityX}|${next.velocityY}|${next.landerX}|${next.fuel}|${next.thrusting}|${next.depositCount}|${inventoryKey}|${next.remotePlayerCount}|${next.shareHint}`;
      if (key === prevKey) return;
      prevKey = key;
      setState(next);
    }, 50);

    return () => {
      engine.destroy();
      clearInterval(syncId);
      engineRef.current = null;
    };
  }, []);

  return state;
}
