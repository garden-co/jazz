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
