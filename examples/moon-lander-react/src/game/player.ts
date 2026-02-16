import { FUEL_TYPES, type FuelType } from "./constants.js";

// ---------------------------------------------------------------------------
// Deterministic player identity — derived from a stable localStorage UUID
// ---------------------------------------------------------------------------

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

/** Simple hash of a string to a number (for deterministic derivation). */
function hashCode(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

export interface PlayerProps {
  name: string;
  color: string;
  requiredFuelType: FuelType;
}

/** Get or create a stable player ID persisted in localStorage. */
export function getOrCreatePlayerId(): string {
  const KEY = "moon-lander-player-id";
  const existing = localStorage.getItem(KEY);
  if (existing) return existing;
  const id = crypto.randomUUID();
  localStorage.setItem(KEY, id);
  return id;
}

/** Derive player properties from a player ID.
 *  Name and colour are stable (identity). Fuel type varies each session. */
export function derivePlayerProps(id: string): PlayerProps {
  const h = hashCode(id);
  return {
    name: PLAYER_NAMES[h % PLAYER_NAMES.length],
    color: PLAYER_COLOURS[h % PLAYER_COLOURS.length],
    requiredFuelType: FUEL_TYPES[Math.floor(Math.random() * FUEL_TYPES.length)],
  };
}
