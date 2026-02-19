// ---------------------------------------------------------------------------
// Game constants
// ---------------------------------------------------------------------------

// Canvas
export const CANVAS_WIDTH = 960;
export const CANVAS_HEIGHT = 640;

// World
export const MOON_SURFACE_WIDTH = 9600; // Pixels — ~5 screens wide
export const GROUND_LEVEL = 560; // Y coordinate of the moon surface (from top)

// Physics
export const GRAVITY = 40; // Pixels/sec² (lunar — gentle)
export const THRUST_POWER = 120; // Pixels/sec² applied upward when thrusting
export const THRUST_POWER_X = 60; // Pixels/sec² applied horizontally when thrusting
export const MAX_LANDING_VELOCITY = 80; // Max safe landing speed (pixels/sec)
export const WALK_SPEED = 120; // Pixels/sec

// Fuel
export const FUEL_BURN_Y = 8; // Fuel units/sec for vertical thrust
export const FUEL_BURN_X = 4; // Fuel units/sec for horizontal thrust
export const INITIAL_FUEL = 40; // Enough to land, not enough to launch
export const MAX_FUEL = 100;
export const REFUEL_AMOUNT = 100; // Per correct fuel unit collected

// Lander interaction
export const LANDER_INTERACT_RADIUS = 40; // Pixels — how close to walk to enter lander
export const SHARE_PROXIMITY_RADIUS = 80; // Pixels — how close for automatic fuel sharing

// Walking jump
export const JUMP_VELOCITY = -140; // Pixels/sec upward (negative = up)
export const JUMP_GRAVITY = 200; // Pixels/sec² — snappier than descent gravity for a short, floaty hop

// Spawn
export const INITIAL_ALTITUDE = -400; // Y position at spawn (high above the moon surface)

// Player
export const ASTRONAUT_WIDTH = 16;
export const ASTRONAUT_HEIGHT = 24;
export const LANDER_WIDTH = 24;
export const LANDER_HEIGHT = 32;

// Fuel types
export const FUEL_TYPES = [
  "circle",
  "triangle",
  "square",
  "pentagon",
  "hexagon",
  "diamond",
  "octagon",
] as const;

export type FuelType = (typeof FUEL_TYPES)[number];

// Player modes
export type PlayerMode =
  | "start"
  | "descending"
  | "landed"
  | "walking"
  | "in_lander"
  | "launched"
  | "crashed";

// Colours — synthwave palette
export const COLOURS = {
  background: "#0a0a0f",
  ground: "#2a1a3a",
  pink: "#ff00ff",
  cyan: "#00ffff",
  purple: "#8b00ff",
  yellow: "#ffff00",
  green: "#00ff00",
  orange: "#ff6600",
  softPink: "#ff66ff",
} as const;

// Visual curvature — derived from world circumference via π
export const MOON_RADIUS = MOON_SURFACE_WIDTH / (2 * Math.PI);

/** Vertical drop (px) at screenX due to spherical curvature. */
export function curveOffset(screenX: number, screenW: number): number {
  const dx = screenX - screenW / 2;
  return MOON_RADIUS * (1 - Math.cos(dx / MOON_RADIUS));
}

/** Radial lean angle (radians) at screenX — for tilting entities on the sphere. */
export function leanAngle(screenX: number, screenW: number): number {
  return (screenX - screenW / 2) / MOON_RADIUS;
}

// Deposit colours — shared by render and sprite systems
export const DEPOSIT_COLOURS: Record<FuelType, string> = {
  circle: COLOURS.cyan,
  triangle: COLOURS.pink,
  square: COLOURS.yellow,
  pentagon: COLOURS.green,
  hexagon: COLOURS.orange,
  diamond: COLOURS.softPink,
  octagon: COLOURS.purple,
};

// DB sync
export const DB_SYNC_INTERVAL_MS = 200;
