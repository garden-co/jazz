import {
  COLOURS,
  type FuelType,
} from "./constants.js";

// ---------------------------------------------------------------------------
// Pixel-art sprite system — OffscreenCanvas cache with palette colouring
// ---------------------------------------------------------------------------

// Palette keys used in sprite grids:
//   .  transparent
//   B  body colour (player colour, default cyan)
//   D  dark shade
//   W  window / visor (pink)
//   H  highlight (lighter body)
//   S  suit grey
//   A  accent (boots — same as body colour)
//   F  flame orange
//   G  flame yellow
//   P  flame pink
//   C  deposit body colour
//   E  white (suit detail)

type Palette = Record<string, string>;

// ---------------------------------------------------------------------------
// Colour helpers
// ---------------------------------------------------------------------------

function darken(hex: string, amount = 0.3): string {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  const f = 1 - amount;
  const tr = Math.round(r * f).toString(16).padStart(2, "0");
  const tg = Math.round(g * f).toString(16).padStart(2, "0");
  const tb = Math.round(b * f).toString(16).padStart(2, "0");
  return `#${tr}${tg}${tb}`;
}

function lighten(hex: string, amount = 0.4): string {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  const tr = Math.min(255, Math.round(r + (255 - r) * amount)).toString(16).padStart(2, "0");
  const tg = Math.min(255, Math.round(g + (255 - g) * amount)).toString(16).padStart(2, "0");
  const tb = Math.min(255, Math.round(b + (255 - b) * amount)).toString(16).padStart(2, "0");
  return `#${tr}${tg}${tb}`;
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
// Tapered nose, boxy hull, pink window, splayed legs
// ---------------------------------------------------------------------------

const LANDER_BODY = padGrid([
  // nose (rows 0-4)
  "..........HH",
  ".........HBBH",
  "........HBBBBH",
  ".......HBBBBBBH",
  "......HBBBBBBBBH",
  // hull (rows 5-8)
  ".....HBBBBBBBBBBH",
  ".....BBBBBBBBBBBB",
  "....HBBBBBBBBBBBBH",
  "....BBBBBBBBBBBBBB",
  // window (rows 9-11)
  "....BBBWWWWWWBBBBB",
  "....BBBWWWWWWBBBBB",
  "....BBBWWWWWWBBBBB",
  // hull body (rows 12-18)
  "....BBBBBBBBBBBBBB",
  "....BBBBBBBBBBBBBB",
  "....BBBHHHHHHBBBBB",
  "....BBBBBBBBBBBBBB",
  "....BBBBBBBBBBBBBB",
  "....BBBBBBBBBBBBBB",
  "....BBBBBBBBBBBBBB",
  // hull bottom (rows 19-22)
  ".....BBBBBBBBBBBB",
  ".....DDDDDDDDDDDD",
  ".....BBBBBBBBBBBB",
  ".....BBBBBBBBBBBB",
  // legs (rows 23-28)
  "...BB...BBBBBB...BB",
  "..BB....BBBBBB....BB",
  ".BB.....BBBBBB.....BB",
  "BB......BBBBBB......BB",
  "BB......BBBBBB......BB",
  "BBB.....BBBBBB.....BBB",
  // nozzle (rows 29-31)
  "........BBBBBB",
  "........BBBBBB",
  ".........BBBB",
], 24);

// ---------------------------------------------------------------------------
// Astronaut sprite grid (16×24)
// Rounded helmet, bulky suit, pink visor, boots
// ---------------------------------------------------------------------------

const ASTRO_FRAME_0 = padGrid([
  // helmet (rows 0-7)
  "...BBBBBB",
  "..BBBBBBBB",
  ".BBBBBBBBBB",
  ".BBWWWWBBBB",
  ".BBWWWWBBBB",
  ".BBBBBBBBBB",
  "..BBBBBBBB",
  "...BBBBBB",
  // neck + suit body (rows 8-15)
  "...ESSSE",
  "..ESSSSSSE",
  "..SSSSSSSS",
  ".SSSSSSSSSS",
  ".SSSSSSSSSS",
  "..SSSSSSSS",
  "..SSSSSSSS",
  "...SSSSSS",
  // legs + boots (rows 16-23)
  "...SS..SS",
  "...SS..SS",
  "...SS..SS",
  "...SS..SS",
  "..AAA..SS",
  "..AAA..SS",
  ".......AAA",
  ".......AAA",
], 16);

const ASTRO_FRAME_1 = padGrid([
  // helmet (same)
  "...BBBBBB",
  "..BBBBBBBB",
  ".BBBBBBBBBB",
  ".BBWWWWBBBB",
  ".BBWWWWBBBB",
  ".BBBBBBBBBB",
  "..BBBBBBBB",
  "...BBBBBB",
  // suit body (same)
  "...ESSSE",
  "..ESSSSSSE",
  "..SSSSSSSS",
  ".SSSSSSSSSS",
  ".SSSSSSSSSS",
  "..SSSSSSSS",
  "..SSSSSSSS",
  "...SSSSSS",
  // legs + boots (mirrored stride)
  "...SS..SS",
  "...SS..SS",
  "...SS..SS",
  "...SS..SS",
  "...SS..AAA",
  "...SS..AAA",
  "..AAA",
  "..AAA",
], 16);

// ---------------------------------------------------------------------------
// Deposit sprite grids (16×16)
// Each polygon shape with highlight border
// ---------------------------------------------------------------------------

const DEPOSIT_GRIDS: Record<FuelType, string[]> = {
  circle: [
    "......CCCC......",
    "....CHHHHHCC....",
    "...CHCCCCCHC....",
    "..CH.CCCCCC.CC..",
    "..CC.CCCCCC.CC..",
    ".CC..CCCCCC..CC.",
    ".CC..CCCCCC..CC.",
    ".CC..CCCCCC..CC.",
    ".CC..CCCCCC..CC.",
    ".CC..CCCCCC..CC.",
    ".CC..CCCCCC..CC.",
    "..CC.CCCCCC.CC..",
    "..CC.CCCCCC.CC..",
    "...CCCCCCCCCC...",
    "....CCCCCCCC....",
    "......CCCC......",
  ],
  triangle: [
    ".......HH.......",
    "......HCCH......",
    "......HCCH......",
    ".....HCCCCH.....",
    ".....HCCCCH.....",
    "....HCCCCCCH....",
    "....HCCCCCCH....",
    "...HCCCCCCCCH...",
    "...HCCCCCCCCH...",
    "..HCCCCCCCCCCH..",
    "..HCCCCCCCCCCH..",
    ".HCCCCCCCCCCCCH.",
    ".HCCCCCCCCCCCCH.",
    "HCCCCCCCCCCCCCCH",
    "CCCCCCCCCCCCCCCC",
    "CCCCCCCCCCCCCCCC",
  ],
  square: [
    "HHHHHHHHHHHHHHHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HCCCCCCCCCCCCCHH",
    "HHCCCCCCCCCCCCHH",
    "HHHHHHHHHHHHHHHH",
  ],
  pentagon: [
    "......HHHH......",
    ".....HCCCCH.....",
    "....HCCCCCCH....",
    "...HCCCCCCCCH...",
    "..HCCCCCCCCCCH..",
    ".HCCCCCCCCCCCCH.",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    ".HCCCCCCCCCCCCH.",
    "..HCCCCCCCCCCCH.",
    "...HCCCCCCCCH...",
    "....HCCCCCCH....",
    ".....HCCCCH.....",
    "......HCCH......",
    ".......HH.......",
  ],
  hexagon: [
    "....HHHHHHHH....",
    "...HCCCCCCCCH...",
    "..HCCCCCCCCCCH..",
    ".HCCCCCCCCCCCCH.",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    ".HCCCCCCCCCCCCH.",
    "..HCCCCCCCCCCH..",
    "...HCCCCCCCCH...",
    "....HHHHHHHH....",
  ],
  heptagon: [
    "......HHHH......",
    ".....HCCCCH.....",
    "....HCCCCCCH....",
    "...HCCCCCCCCH...",
    "..HCCCCCCCCCCH..",
    ".HCCCCCCCCCCCCH.",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    ".HCCCCCCCCCCCCH.",
    "..HCCCCCCCCCCH..",
    "..HCCCCCCCCCCH..",
    "...HCCCCCCCCH...",
    "....HCCCCCCH....",
    ".....HHHHHH.....",
  ],
  octagon: [
    "....HHHHHHHH....",
    "...HCCCCCCCCH...",
    "..HCCCCCCCCCCH..",
    ".HCCCCCCCCCCCCH.",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    "HCCCCCCCCCCCCCCH",
    ".HCCCCCCCCCCCCH.",
    "..HCCCCCCCCCCH..",
    "...HCCCCCCCCH...",
    "....HHHHHHHH....",
  ],
};

// ---------------------------------------------------------------------------
// Sprite cache — keyed by sprite type + colour
// ---------------------------------------------------------------------------

const cache = new Map<string, OffscreenCanvas>();

function makeLanderPalette(colour: string): Palette {
  return {
    B: colour,
    D: darken(colour),
    H: lighten(colour),
    W: COLOURS.pink,
    F: COLOURS.orange,
    G: COLOURS.yellow,
    P: COLOURS.pink,
  };
}

function makeAstroPalette(colour: string): Palette {
  return {
    B: colour,
    H: lighten(colour),
    W: COLOURS.pink,
    S: "#cccccc",
    E: "#eeeeee",
    A: colour,
  };
}

// Deposit colours duplicated here to avoid circular import with render.ts
const SPRITE_DEPOSIT_COLOURS: Record<FuelType, string> = {
  circle: COLOURS.cyan,
  triangle: COLOURS.pink,
  square: COLOURS.yellow,
  pentagon: COLOURS.green,
  hexagon: COLOURS.orange,
  heptagon: COLOURS.softPink,
  octagon: COLOURS.purple,
};

function makeDepositPalette(fuelType: FuelType): Palette {
  const colour = SPRITE_DEPOSIT_COLOURS[fuelType];
  return {
    C: colour,
    H: lighten(colour, 0.5),
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


export function getAstronautSprite(colour: string | undefined, frame: number): OffscreenCanvas {
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
