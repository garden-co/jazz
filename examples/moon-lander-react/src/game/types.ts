import type { Player } from "../../schema/app";
import type { FuelType, PlayerMode } from "./constants";

// ---------------------------------------------------------------------------
// Engine types — the snapshot exposed to React each tick
// ---------------------------------------------------------------------------

export interface ArcAnimation {
  fuelType: FuelType;
  startX: number; // world X
  endX: number; // world X
  peakHeight: number; // pixels above ground level
  duration: number; // seconds (game time)
  elapsed: number;
  onComplete?: () => void;
  rotation: number; // current rotation angle (radians)
  glowPhase: number; // offset for pulsing glow (radians)
  targetPlayerId?: string; // if set, arc tracks this player's position
}

export interface Deposit {
  id: string;
  x: number;
  type: FuelType;
  /** Monotonic time (seconds) when this deposit was first seen. Used for fade-in. */
  spawnTime: number;
}

export interface EngineState {
  mode: PlayerMode;
  positionX: number;
  positionY: number;
  velocityX: number;
  velocityY: number;
  landerX: number;
  fuel: number;
  thrusting: boolean;
  depositCount: number;
  inventory: string[];
  remotePlayerCount: number;
  shareHint: boolean;
}

// ---------------------------------------------------------------------------
// EngineProps — everything the engine receives from React each render
// ---------------------------------------------------------------------------

export interface EngineProps {
  physicsSpeed: number;
  initialMode: PlayerMode;
  /** Override the random spawn X position (for tests). */
  spawnX?: number;
  requiredFuelType: FuelType;
  remotePlayers: Player[];
  deposits?: Deposit[];
  inventory?: FuelType[];
  chatMessages: Array<{
    id: string;
    playerId: string;
    message: string;
    createdAt: number;
  }>;
  localPlayerId: string;
  localPlayerName: string;
  localPlayerColor: string;
  chatOpen: boolean;
  onCollectDeposit?: (id: string) => void;
  onRefuel?: (fuelType: FuelType) => void;
  onShareFuel?: (fuelType: string, receiverPlayerId: string) => void;
  onBurstDeposit?: (fuelType: string) => void;
}

// ---------------------------------------------------------------------------
// Mutable world state — replaces individual physics refs in the game loop
// ---------------------------------------------------------------------------

export interface GameWorld {
  posX: number;
  posY: number;
  velX: number;
  velY: number;
  mode: PlayerMode;
  landerX: number;
  fuel: number;
  launchElapsed: number;
  crashElapsed: number;
}

// ---------------------------------------------------------------------------
// Input — a snapshot of the keyboard state for one frame
// ---------------------------------------------------------------------------

export interface InputSnapshot {
  left: boolean;
  right: boolean;
  up: boolean;
  interact: boolean;
  launch: boolean;
  jump: boolean;
}

// ---------------------------------------------------------------------------
// Physics callbacks — side-effects the engine fires during simulation
// ---------------------------------------------------------------------------

export interface PhysicsCallbacks {
  onCollectDeposit?: (id: string) => void;
  onRefuel?: (fuelType: FuelType) => void;
  onShareFuel?: (fuelType: string, receiverPlayerId: string) => void;
  onBurstDeposit?: (fuelType: string) => void;
}

// ---------------------------------------------------------------------------
// Scene context — everything the renderer needs for one frame
// ---------------------------------------------------------------------------

export interface SceneContext {
  ctx: CanvasRenderingContext2D;
  w: number;
  h: number;
  cameraX: number;
  cameraY: number;
  groundScreenY: number;
  dt: number;
  now: number; // monotonic time in seconds (for animation timing)
  world: GameWorld;
  thrusting: boolean;
  localPlayerName: string;
  localPlayerColor: string;
  deposits: Deposit[];
  collectedIds: Set<string>;
  requiredFuelType: FuelType;
  inventory: Set<FuelType>;
  arcs: ArcAnimation[];
  remotePlayers: Player[];
  smoothedRemotes: Map<string, { x: number; y: number }>;
  chatMessages: Array<{
    id: string;
    playerId: string;
    message: string;
    createdAt: number;
  }>;
  localPlayerId: string;
  particles: import("./particles").Particle[];
  thrustLeft: boolean;
  thrustRight: boolean;
  /** Deposit pickups this frame: [{x, fuelType, isRequired, burst?}] for sparkle/burst effects */
  collectEffects: Array<{
    x: number;
    fuelType: FuelType;
    isRequired: boolean;
    burst?: boolean;
  }>;
  walkingInput: boolean; // true if left/right keys held during walking mode
}
