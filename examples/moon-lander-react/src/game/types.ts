import type { PlayerMode, FuelType } from "./constants.js";

// ---------------------------------------------------------------------------
// Types — the contract between Jazz and the game engine
// ---------------------------------------------------------------------------

/** State pushed to Jazz on each sync tick. */
export interface GameState {
  mode: PlayerMode;
  positionX: number;
  positionY: number;
  velocityX: number;
  velocityY: number;
  fuel: number;
  landerSpawnX: number;
  playerName: string;
  playerColor: string;
  requiredFuelType: FuelType;
}

/** A remote player received from Jazz and rendered in the game world. */
export interface RemotePlayer {
  id: string;
  name: string;
  mode: PlayerMode;
  positionX: number;
  positionY: number;
  velocityX: number;
  velocityY: number;
  color: string;
  requiredFuelType: string;
  lastSeen: number;
  landerFuelLevel: number;
  playerId?: string;
  landerX?: number;
  hasRequiredFuel?: boolean;
}

/** A chat message from Jazz. */
export interface ChatMessage {
  id: string;
  playerId: string;
  message: string;
  createdAt: number;
}

// ---------------------------------------------------------------------------
// Engine types — the snapshot exposed to React each tick
// ---------------------------------------------------------------------------

export interface ArcAnimation {
  fuelType: FuelType;
  startX: number;   // world X
  endX: number;     // world X
  peakHeight: number; // pixels above ground level
  duration: number; // seconds (game time)
  elapsed: number;
  onComplete?: () => void;
  rotation: number;     // current rotation angle (radians)
  glowPhase: number;    // offset for pulsing glow (radians)
  targetPlayerId?: string; // if set, arc tracks this player's position
}

export interface Deposit {
  id: string;
  x: number;
  type: FuelType;
}

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
  requiredFuelType?: string;
  playerId?: string;
  hasRequiredFuel?: boolean;
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
  shareHint: boolean;
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
  landerY: number;
  fuel: number;
  launchElapsed: number;
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
  onBurstDeposit?: (fuelType: string, newX: number) => void;
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
  remotePlayers: RemotePlayerView[];
  smoothedRemotes: Map<string, { x: number; y: number }>;
  chatMessages: Array<{ id: string; playerId: string; message: string; createdAt: number }>;
  localPlayerId: string;
  particles: import("./particles.js").Particle[];
  thrustLeft: boolean;
  thrustRight: boolean;
  /** Deposit pickups this frame: [{x, fuelType, isRequired}] for sparkle effects */
  collectEffects: Array<{ x: number; fuelType: FuelType; isRequired: boolean }>;
  walkingInput: boolean; // true if left/right keys held during walking mode
}
