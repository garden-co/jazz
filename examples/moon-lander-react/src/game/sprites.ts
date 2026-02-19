import { COLOURS, DEPOSIT_COLOURS, type FuelType } from "./constants";

// ---------------------------------------------------------------------------
// Pixel-art sprite system — OffscreenCanvas cache with palette colouring
// ---------------------------------------------------------------------------

// Palette keys used in sprite grids:
//   .  transparent
//
// Lander palette:
//   O  black outline (#000000)
//   X  dark steel, main hull (#2e2e3e)
//   Z  medium steel, cockpit frame (#484860)
//   K  light steel, worn metal highlight (#5c5c74)
//   V  panel seam shadow (#1a1a28)
//   T  intake grille, near-black (#101018)
//   B  player body colour (trim / accents)
//   H  highlight (lighter body)
//   N  bright beacon glow (very light body)
//   W  hot pink neon (#ff00ff)
//   R  white reflection (#ffffff)
//   L  cyan thruster glow (#00ffff)
//   U  magenta underlight (#cc00cc)
//
// Astronaut palette:
//   O  black outline (#000000)
//   H  bright edge highlight (lighten player colour)
//   B  player colour (plate highlights / accents)
//   D  dark player colour (armour base, 45% darken)
//   S  bodysuit, visible at joints (#303048)
//   W  neon visor glow (#ff00ff)
//   R  visor reflection (#ffffff)
//
// Deposit palette:
//   C  deposit body colour
//   H  highlight
//   I  inner detail (lighter fill)
//   D  dark shade

type Palette = Record<string, string>;

// ---------------------------------------------------------------------------
// Colour helpers
// ---------------------------------------------------------------------------

function adjustColour(hex: string, fn: (ch: number) => number): string {
  const r = fn(parseInt(hex.slice(1, 3), 16));
  const g = fn(parseInt(hex.slice(3, 5), 16));
  const b = fn(parseInt(hex.slice(5, 7), 16));
  return `#${r.toString(16).padStart(2, "0")}${g.toString(16).padStart(2, "0")}${b.toString(16).padStart(2, "0")}`;
}

function darken(hex: string, amount = 0.3): string {
  return adjustColour(hex, (ch) => Math.round(ch * (1 - amount)));
}

function lighten(hex: string, amount = 0.4): string {
  return adjustColour(hex, (ch) =>
    Math.min(255, Math.round(ch + (255 - ch) * amount)),
  );
}

// ---------------------------------------------------------------------------
// Grid helpers
// ---------------------------------------------------------------------------

/** Pad/clip every row to `width` characters, filling with '.' */
function padGrid(rows: string[], width: number): string[] {
  return rows.map((r) => r.padEnd(width, ".").slice(0, width));
}

// ---------------------------------------------------------------------------
// Stamp a sprite grid onto an OffscreenCanvas
// ---------------------------------------------------------------------------

function stampSprite(grid: string[], palette: Palette): OffscreenCanvas {
  const h = grid.length;
  const w = grid[0].length;
  const canvas = new OffscreenCanvas(w, h);
  const ctx = canvas.getContext("2d")!;
  for (let y = 0; y < h; y++) {
    const row = grid[y];
    for (let x = 0; x < w; x++) {
      const ch = row[x];
      if (ch === ".") continue;
      const colour = palette[ch];
      if (!colour) continue;
      ctx.fillStyle = colour;
      ctx.fillRect(x, y, 1, 1);
    }
  }
  return canvas;
}

// ---------------------------------------------------------------------------
// Lander sprite grid (24×32)
// Cyberpunk city lander — angular cockpit, panel lines, neon underlighting,
// splayed landing skids, central thruster glow
// ---------------------------------------------------------------------------

const LANDER_BODY = padGrid(
  [
    // antenna / sensor mast (rows 0–2)
    "...........OO",
    "..........ONNO",
    ".........OWBBWO",
    // cockpit (rows 3–8)
    "........OXXXXXXO",
    ".......OXXZZZZXXO",
    "......OXXXZZZZXXXO",
    ".....OXZZWWWWWWZZXO",
    ".....OXZWWRWWWRWZXO",
    ".....OXXZZZZZZZZXXO",
    // hull (rows 9–17, 16 px wide)
    "....OBXXXXXXXXXXXXBO",
    "....OXXXXXXVVXXXXXXO",
    "....OXXVXXXXXXXXVXXO",
    "....OXXXXXXVVXXXXXXO",
    "....OBBBBBBBBBBBBBBO",
    "....OXXXXXXVVXXXXXXO",
    "....OXXVXXXXXXXXVXXO",
    "....OXXXXXXVVXXXXXXO",
    "....OXXKKKKKKKKKKXXO",
    // engine section (rows 18–23)
    ".....OXXXXXXXXXXXXO",
    ".....OXTTTTTTTTTTXO",
    ".....OXXXXXXXXXXXXO",
    "....OWXXXXXXXXXXXXWO",
    "....OXXXXXXUUXXXXXXO",
    "....OXXXXUUUUUUXXXXO",
    // landing gear + nozzle (rows 24–31)
    "..OXO.OXXUUUUUUXXO.OXO",
    ".OXO..OXXUUUUUUXXO..OXO",
    "OXO....OWWUUUUWWO....OXO",
    "OXO.....OLLLLLLO.....OXO",
    "OXO.....OLLLLLLO.....OXO",
    ".........OLLLLO",
    "..........OLLO",
    "...........OO",
  ],
  24,
);

// ---------------------------------------------------------------------------
// Astronaut sprite grid (16×24)
// Cyberpunk armoured suit — plated bodysuit, neon visor, idle stance facing right
// ---------------------------------------------------------------------------

const ASTRO_FRAME_0 = padGrid(
  [
    // helmet (rows 0–6) — angular, top-lit player colour, visor on right
    ".....OOOO",
    "....OHHHHO",
    "...OHBDDBHO",
    "...OBDWWWWO",
    "...OBDWWRWO",
    "...OBDDDDBO",
    "....OBDDBO",
    // neck (row 7)
    ".....OSSO",
    // shoulders + torso (rows 8–15) — pauldrons, bodysuit at joints
    "....OBDSSDBO",
    "...OBHDSSDHBO",
    "...OBDDSSDDBO",
    "...ODDDBBBDDO",
    "...ODDDSSSDDO",
    "....ODDSSSDO",
    "....ODDSSDDO",
    "....ODDDDDDO",
    // legs + boots (rows 16–23) — left foot forward, right foot behind
    "...ODDO.ODDO",
    "...ODDO.ODDO",
    "...ODDO.ODDO",
    "...ODDO.ODDO",
    ".OBDDDO.ODDO",
    ".OBDDDO.ODDO",
    "........ODDBO",
    "........ODDBO",
  ],
  16,
);

const ASTRO_FRAME_1 = padGrid(
  [
    // helmet (same)
    ".....OOOO",
    "....OHHHHO",
    "...OHBDDBHO",
    "...OBDWWWWO",
    "...OBDWWRWO",
    "...OBDDDDBO",
    "....OBDDBO",
    // neck (same)
    ".....OSSO",
    // shoulders + torso (same)
    "....OBDSSDBO",
    "...OBHDSSDHBO",
    "...OBDDSSDDBO",
    "...ODDDBBBDDO",
    "...ODDDSSSDDO",
    "....ODDSSSDO",
    "....ODDSSDDO",
    "....ODDDDDDO",
    // legs + boots (mirrored stride — right foot forward, left foot behind)
    "...ODDO.ODDO",
    "...ODDO.ODDO",
    "...ODDO.ODDO",
    "...ODDO.ODDO",
    "...ODDO.ODDDBO",
    "...ODDO.ODDDBO",
    ".OBDDDO",
    ".OBDDDO",
  ],
  16,
);

// ---------------------------------------------------------------------------
// Deposit sprite grids (16×16)
// Each polygon shape with highlight border
// ---------------------------------------------------------------------------

const DEPOSIT_GRIDS: Record<FuelType, string[]> = {
  // Circle: beveled gem with inner facet ring
  circle: [
    "................",
    ".....DDDDDD.....",
    "....DIIIIIID....",
    "...DIHHCCCCID...",
    "..DIHCCCCCCCID..",
    ".DICCCCCCCCCCID.",
    ".DICCCCCCCCCCID.",
    ".DICCCCCCCCCCID.",
    ".DICCCCCCCCCCID.",
    ".DICCCCCCCCCCID.",
    ".DICCCCCCCCCCID.",
    "..DICCCCCCCCID..",
    "...DICCCCCCID...",
    "....DIIIIIID....",
    ".....DDDDDD.....",
    "................",
  ],
  // Triangle: upward-pointing with facet edges
  triangle: [
    ".......DD.......",
    "......DHHD......",
    ".....DIHHID.....",
    ".....DICCID.....",
    "....DICCCCID....",
    "....DICCCCID....",
    "...DICCCCCCID...",
    "...DICCCCCCID...",
    "..DICCCCCCCCID..",
    "..DICCCCCCCCID..",
    ".DICCCCCCCCCCID.",
    ".DICCCCCCCCCCID.",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DIIIIIIIIIIIIIID",
    "DDDDDDDDDDDDDDDD",
  ],
  // Square: beveled block with facet border
  square: [
    "DDDDDDDDDDDDDDDD",
    "DIIIIIIIIIIIIIID",
    "DIHHCCCCCCCCCCID",
    "DIHCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DIIIIIIIIIIIIIID",
    "DDDDDDDDDDDDDDDD",
  ],
  // Pentagon: pointed top, wide shoulders, narrower flat base
  pentagon: [
    ".......DD.......",
    "......DHHD......",
    ".....DIHHID.....",
    "....DICCCCID....",
    "...DICCCCCCID...",
    "..DICCCCCCCCID..",
    ".DICCCCCCCCCCID.",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    ".DICCCCCCCCCCID.",
    ".DICCCCCCCCCCID.",
    "..DICCCCCCCCID..",
    "..DICCCCCCCCID..",
    "..DICCCCCCCCID..",
    "..DIIIIIIIIIID..",
    "..DDDDDDDDDDDD..",
  ],
  // Hexagon: flat top and bottom, bulges in middle
  hexagon: [
    "....DDDDDDDD....",
    "...DIIIIIIIID...",
    "..DIHCCCCCCCID..",
    ".DICCCCCCCCCCID.",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    ".DICCCCCCCCCCID.",
    "..DICCCCCCCCID..",
    "...DIIIIIIIID...",
    "....DDDDDDDD....",
  ],
  // Diamond: pointed top and bottom, widest at centre
  diamond: [
    ".......DD.......",
    "......DIID......",
    ".....DIHHID.....",
    "....DIHCCCID....",
    "...DICCCCCCID...",
    "..DICCCCCCCCID..",
    ".DICCCCCCCCCCID.",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    ".DICCCCCCCCCCID.",
    "..DICCCCCCCCID..",
    "...DICCCCCCID...",
    "....DICCCCID....",
    ".....DICCID.....",
    "......DIID......",
    ".......DD.......",
  ],
  // Octagon: stop-sign with clipped corners
  octagon: [
    "...DDDDDDDDDD...",
    "..DIIIIIIIIIID..",
    ".DIHCCCCCCCCCID.",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    "DICCCCCCCCCCCCID",
    ".DICCCCCCCCCCID.",
    "..DIIIIIIIIIID..",
    "...DDDDDDDDDD...",
  ],
};

// ---------------------------------------------------------------------------
// Sprite cache — keyed by sprite type + colour
// ---------------------------------------------------------------------------

const cache = new Map<string, OffscreenCanvas>();

function makeLanderPalette(colour: string): Palette {
  return {
    O: "#000000", // hard black outline
    X: "#8080a0", // mid steel (main hull)
    Z: "#a0a0bc", // light steel (cockpit frame)
    K: "#c0c0d8", // bright steel (worn metal highlight)
    V: "#606080", // panel seam shadow
    T: "#505068", // intake grille
    B: colour, // player body colour (trim / accents)
    H: lighten(colour), // highlight (lighter body)
    N: lighten(colour, 0.7), // bright beacon glow
    W: COLOURS.pink, // hot pink neon
    R: "#ffffff", // white reflection
    L: "#00ffff", // cyan thruster glow
    U: "#cc00cc", // magenta underlight
  };
}

function makeAstroPalette(colour: string): Palette {
  return {
    O: "#000000", // hard black outline
    H: lighten(colour), // bright edge highlight (top-lit)
    B: colour, // player colour (plate highlights)
    D: darken(colour, 0.45), // dark player colour (armour base)
    S: "#303048", // bodysuit (visible at joints)
    W: COLOURS.pink, // neon visor glow
    R: "#ffffff", // visor reflection
  };
}

function makeDepositPalette(fuelType: FuelType): Palette {
  const colour = DEPOSIT_COLOURS[fuelType];
  return {
    C: colour,
    H: lighten(colour, 0.5),
    I: lighten(colour, 0.25),
    D: darken(colour, 0.5),
  };
}

// ---------------------------------------------------------------------------
// Public API — get cached sprites
// ---------------------------------------------------------------------------

export function getLanderSprite(colour?: string): OffscreenCanvas {
  const c = colour ?? COLOURS.cyan;
  const key = `lander:${c}`;
  let s = cache.get(key);
  if (!s) {
    s = stampSprite(LANDER_BODY, makeLanderPalette(c));
    cache.set(key, s);
  }
  return s;
}

export function getAstronautSprite(
  colour: string | undefined,
  frame: number,
): OffscreenCanvas {
  const c = colour ?? COLOURS.cyan;
  const f = frame % 2;
  const key = `astro:${c}:${f}`;
  let s = cache.get(key);
  if (!s) {
    const grid = f === 0 ? ASTRO_FRAME_0 : ASTRO_FRAME_1;
    s = stampSprite(grid, makeAstroPalette(c));
    cache.set(key, s);
  }
  return s;
}

export function getDepositSprite(fuelType: FuelType): OffscreenCanvas {
  const key = `deposit:${fuelType}`;
  let s = cache.get(key);
  if (!s) {
    const grid = DEPOSIT_GRIDS[fuelType];
    s = stampSprite(grid, makeDepositPalette(fuelType));
    cache.set(key, s);
  }
  return s;
}

// ---------------------------------------------------------------------------
// Animation clock — global frame counters
// ---------------------------------------------------------------------------

let thrustPhase = 0;
let walkPhase = 0;
let walkTimer = 0;
const WALK_FRAME_INTERVAL = 0.2; // seconds per walk frame

export function tickSpriteAnimation(dt: number): void {
  thrustPhase += dt * 12; // fast cycling for thrust flicker
  walkTimer += dt;
  if (walkTimer >= WALK_FRAME_INTERVAL) {
    walkTimer -= WALK_FRAME_INTERVAL;
    walkPhase = (walkPhase + 1) % 2;
  }
}

export function getThrustFrame(): number {
  return Math.floor(thrustPhase) % 3;
}

export function getWalkFrame(): number {
  return walkPhase;
}

export function resetWalkFrame(): void {
  walkPhase = 0;
  walkTimer = 0;
}
