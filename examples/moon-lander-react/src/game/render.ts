import {
  GROUND_LEVEL,
  MOON_SURFACE_WIDTH,
  LANDER_WIDTH,
  LANDER_HEIGHT,
  ASTRONAUT_WIDTH,
  ASTRONAUT_HEIGHT,
  COLOURS,
  curveOffset,
  type FuelType,
} from "./constants.js";
import {
  getLanderSprite,
  getAstronautSprite,
  getDepositSprite,
  getThrustFrame,
  getWalkFrame,
} from "./sprites.js";

// ---------------------------------------------------------------------------
// Glow helper — apply and restore shadowBlur in one place
// ---------------------------------------------------------------------------

function withGlow(
  ctx: CanvasRenderingContext2D,
  colour: string,
  blur: number,
  fn: () => void,
): void {
  const prevBlur = ctx.shadowBlur;
  const prevColour = ctx.shadowColor;
  ctx.shadowColor = colour;
  ctx.shadowBlur = blur;
  fn();
  ctx.shadowBlur = prevBlur;
  ctx.shadowColor = prevColour;
}

// ---------------------------------------------------------------------------
// Starfield — 3 parallax layers with deterministic pseudo-random positions
// ---------------------------------------------------------------------------

interface Star {
  x: number;
  y: number;
  size: number;
  brightness: number;
  layer: number;
  colour: string;
  rgb: [number, number, number]; // pre-parsed for rgba fillStyle
  twinklePhase: number;
}

const PARALLAX_SPEEDS_X = [0.05, 0.15, 0.3];
const PARALLAX_SPEEDS_Y = [0.02, 0.06, 0.12];
const STARS_PER_LAYER = 70;

const STAR_COLOURS = ["#ffffff", "#ffffff", "#ffffff", "#aaddff", "#ffaadd", "#ddaaff", "#aaffee"];

function hexToRgb(hex: string): [number, number, number] {
  return [
    parseInt(hex.slice(1, 3), 16),
    parseInt(hex.slice(3, 5), 16),
    parseInt(hex.slice(5, 7), 16),
  ];
}

function seededRandom(seed: number): number {
  const x = Math.sin(seed * 127.1 + seed * 311.7) * 43758.5453;
  return x - Math.floor(x);
}

const STAR_FIELD_WIDTH = 4000;
const STAR_FIELD_HEIGHT = 2000;
const stars: Star[] = [];
for (let layer = 0; layer < 3; layer++) {
  for (let i = 0; i < STARS_PER_LAYER; i++) {
    const seed = layer * 1000 + i;
    const starColour = STAR_COLOURS[Math.floor(seededRandom(seed + 0.9) * STAR_COLOURS.length)];
    stars.push({
      x: seededRandom(seed) * STAR_FIELD_WIDTH,
      y: seededRandom(seed + 0.5) * STAR_FIELD_HEIGHT,
      size: layer === 2 ? 2 : 1,
      brightness: 0.4 + seededRandom(seed + 0.3) * 0.6,
      layer,
      colour: starColour,
      rgb: hexToRgb(starColour),
      twinklePhase: seededRandom(seed + 0.7) * Math.PI * 2,
    });
  }
}

export function drawStarfield(
  ctx: CanvasRenderingContext2D,
  cameraX: number,
  cameraY: number,
  w: number,
  h: number,
  now: number,
) {
  for (const star of stars) {
    const px = PARALLAX_SPEEDS_X[star.layer];
    const py = PARALLAX_SPEEDS_Y[star.layer];
    let sx = star.x - cameraX * px;
    let sy = star.y - cameraY * py;
    sx = ((sx % STAR_FIELD_WIDTH) + STAR_FIELD_WIDTH) % STAR_FIELD_WIDTH;
    sy = ((sy % STAR_FIELD_HEIGHT) + STAR_FIELD_HEIGHT) % STAR_FIELD_HEIGHT;
    if (sx > w + 2 || sy > h + 2) continue;

    // Near-layer stars twinkle via sine wave
    let b = star.brightness;
    if (star.layer === 2) {
      b *= 0.7 + 0.3 * Math.sin(now * 2.5 + star.twinklePhase);
    }

    // Use rgba in fillStyle (not globalAlpha) so it doesn't leak to other draws
    const [sr, sg, sb] = star.rgb;
    ctx.fillStyle = `rgba(${sr},${sg},${sb},${b})`;
    ctx.fillRect(Math.floor(sx), Math.floor(sy), star.size, star.size);
  }
}

// ---------------------------------------------------------------------------
// Moon surface — with horizon accent line and subtle grid
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Terrain — 16-bit Mega Drive / Street Fighter stage floor aesthetic
// Bold colour bands, chunky rock silhouettes, hard pixel outlines
// ---------------------------------------------------------------------------

function terrainHash(seed: number): number {
  const x = Math.sin(seed * 127.1 + seed * 311.7) * 43758.5453;
  return x - Math.floor(x);
}

// Ground colour bands — darkens with depth, Street Fighter stage floor style
const BAND_COLOURS = [
  "#3d1f54", // surface — lighter purple
  "#321848", // mid
  "#28123c", // deeper
  "#1e0c30", // dark
  "#150826", // darkest
];
const BAND_HEIGHT = 6; // pixels per horizontal stripe

// ---------------------------------------------------------------------------
// Cliff terrain — hash-based heights with linear interpolation
// Linear interp between random heights at fixed intervals creates angular,
// cliff-like silhouettes. Tile sizes divide W exactly, so wrapping is seamless.
// ---------------------------------------------------------------------------

const TWO_PI = Math.PI * 2;
const W = MOON_SURFACE_WIDTH;

// --- Cliff system (mountains / cliffs) ---

interface CliffScale {
  tileSize: number; // must divide W evenly
  amp: number;
  seed: number;
}

interface CliffLayerConfig {
  parallax: number;
  maxHeight: number;
  colour: string;
  scales: CliffScale[];
  step?: number;
  // Height fraction below which terrain is clamped to zero.
  // Creates flat stretches punctuated by sharp cliff faces.
  threshold?: number;
  // Ridgeline outline
  outlineColour?: string;
  outlineWidth?: number;
  // Horizontal rock strata lines within cliff faces
  strataColour?: string;
  strataSpacing?: number;
  // Vertical crevice lines dropping from the ridgeline
  creviceColour?: string;
  creviceSeed?: number;
  // Darker band near the base of cliff faces
  shadowColour?: string;
  shadowHeight?: number; // fraction of cliff height (default 0.35)
}

/** Multi-octave cliff height via linear interpolation of hashed tile heights. */
function evalCliffHeight(worldX: number, scales: CliffScale[]): number {
  let total = 0;
  let ampSum = 0;
  for (const s of scales) {
    const tilesPerWorld = W / s.tileSize;
    const pos = worldX / s.tileSize;
    const tileIdx = Math.floor(pos);
    const frac = pos - tileIdx;
    const idx0 =
      ((tileIdx % tilesPerWorld) + tilesPerWorld) % tilesPerWorld;
    const idx1 = (idx0 + 1) % tilesPerWorld;
    const h0 = terrainHash(idx0 * 127 + s.seed);
    const h1 = terrainHash(idx1 * 127 + s.seed);
    total += (h0 + (h1 - h0) * frac) * s.amp;
    ampSum += s.amp;
  }
  return total / ampSum;
}

const CLIFF_LAYERS: CliffLayerConfig[] = [
  // Far — broad mountain range silhouette (minimal detail, just outline)
  {
    parallax: 0.15,
    maxHeight: 100,
    colour: "#100c16",
    scales: [
      { tileSize: 960, amp: 0.5, seed: 1 },
      { tileSize: 480, amp: 0.3, seed: 17 },
      { tileSize: 192, amp: 0.15, seed: 41 },
      { tileSize: 96, amp: 0.05, seed: 73 },
    ],
    threshold: 0.15,
    outlineColour: "#1a1520",
  },
  // Mid — angular ridges (outline + strata)
  {
    parallax: 0.35,
    maxHeight: 70,
    colour: "#1a1522",
    scales: [
      { tileSize: 320, amp: 0.35, seed: 103 },
      { tileSize: 96, amp: 0.35, seed: 137 },
      { tileSize: 48, amp: 0.2, seed: 167 },
      { tileSize: 32, amp: 0.1, seed: 199 },
    ],
    threshold: 0.25,
    outlineColour: "#2e2438",
    strataColour: "#211a2c",
    strataSpacing: 11,
    shadowColour: "#120e18",
  },
  // Near — jagged cliff faces (full detail)
  {
    parallax: 0.6,
    maxHeight: 55,
    colour: "#261e30",
    scales: [
      { tileSize: 128, amp: 0.25, seed: 229 },
      { tileSize: 48, amp: 0.3, seed: 263 },
      { tileSize: 32, amp: 0.25, seed: 293 },
      { tileSize: 16, amp: 0.2, seed: 331 },
    ],
    threshold: 0.2,
    step: 1,
    outlineColour: "#3a2d48",
    outlineWidth: 2,
    strataColour: "#2e2538",
    strataSpacing: 7,
    creviceColour: "#1a1428",
    creviceSeed: 500,
    shadowColour: "#1a1428",
    shadowHeight: 0.35,
  },
];

function drawCliffLayer(
  ctx: CanvasRenderingContext2D,
  cameraX: number,
  screenGroundY: number,
  w: number,
  config: CliffLayerConfig,
) {
  const scrollX = cameraX * config.parallax;
  const step = config.step ?? 2;
  const threshold = config.threshold ?? 0;

  const evalH = (sx: number): number => {
    let h = evalCliffHeight(sx + scrollX, config.scales);
    if (threshold > 0) {
      h = Math.max(0, (h - threshold) / (1 - threshold));
    }
    return h * config.maxHeight;
  };

  // Ground Y at a given screen X, including sphere curvature
  const baseY = (sx: number) =>
    Math.round(screenGroundY + curveOffset(sx, w));

  // --- Base fill ---
  ctx.fillStyle = config.colour;
  ctx.beginPath();
  ctx.moveTo(0, baseY(0));
  for (let sx = 0; sx <= w; sx += step) {
    ctx.lineTo(sx, Math.round(baseY(sx) - evalH(sx)));
  }
  ctx.lineTo(w, baseY(w));
  ctx.closePath();
  ctx.fill();

  // --- Shadow band near base of cliff faces ---
  if (config.shadowColour) {
    const shadowFrac = config.shadowHeight ?? 0.35;
    ctx.fillStyle = config.shadowColour;
    ctx.beginPath();
    ctx.moveTo(0, baseY(0));
    for (let sx = 0; sx <= w; sx += step) {
      const h = evalH(sx);
      ctx.lineTo(sx, Math.round(baseY(sx) - h * shadowFrac));
    }
    ctx.lineTo(w, baseY(w));
    ctx.closePath();
    ctx.fill();
  }

  // --- Horizontal strata lines (rock layers) ---
  if (config.strataColour) {
    const spacing = config.strataSpacing ?? 8;
    ctx.strokeStyle = config.strataColour;
    ctx.lineWidth = 1;
    for (let dy = spacing; dy < config.maxHeight; dy += spacing) {
      ctx.beginPath();
      let drawing = false;
      for (let sx = 0; sx <= w; sx += step) {
        const by = baseY(sx);
        const peakY = Math.round(by - evalH(sx));
        const strataY = Math.round(by - dy);
        if (peakY <= strataY - 2) {
          if (!drawing) {
            ctx.moveTo(sx, strataY);
            drawing = true;
          } else {
            ctx.lineTo(sx, strataY);
          }
        } else {
          drawing = false;
        }
      }
      ctx.stroke();
    }
  }

  // --- Vertical crevices ---
  if (config.creviceColour && config.creviceSeed != null) {
    const CREVICE_TILE = 20;
    ctx.strokeStyle = config.creviceColour;
    ctx.lineWidth = 1;
    for (let sx = 0; sx <= w; sx += CREVICE_TILE) {
      const worldCrevX = sx + scrollX;
      const crevIdx = Math.floor(worldCrevX / CREVICE_TILE);
      if (terrainHash(crevIdx * 157 + config.creviceSeed) > 0.4) continue;

      const h = evalH(sx);
      if (h < 6) continue;

      const by = baseY(sx);
      const peakY = Math.round(by - h);
      const crevLen = h * (0.25 + terrainHash(crevIdx * 163 + 7) * 0.5);
      const xOff = Math.floor(
        (terrainHash(crevIdx * 173 + 13) - 0.5) * 4,
      );

      ctx.beginPath();
      ctx.moveTo(sx, peakY + 1);
      ctx.lineTo(sx + xOff, Math.round(peakY + crevLen));
      ctx.stroke();
    }
  }

  // --- Solid ridgeline outline ---
  if (config.outlineColour) {
    ctx.strokeStyle = config.outlineColour;
    ctx.lineWidth = config.outlineWidth ?? 1;
    ctx.beginPath();
    for (let sx = 0; sx <= w; sx += step) {
      const peakY = Math.round(baseY(sx) - evalH(sx));
      if (sx === 0) ctx.moveTo(sx, peakY);
      else ctx.lineTo(sx, peakY);
    }
    ctx.stroke();
  }
}

// Unwrapped camera X for parallax continuity across the world meridian.
// Raw cameraX wraps at MOON_SURFACE_WIDTH, but parallax * wrappedX jumps
// discontinuously because parallax * W is not a multiple of the terrain period.
// We track cumulative deltas to get a smooth, monotonic scroll value.
let prevRawCameraX = NaN;
let unwrappedCameraX = 0;

function trackUnwrappedCamera(cameraX: number): void {
  if (isNaN(prevRawCameraX)) {
    prevRawCameraX = cameraX;
    unwrappedCameraX = cameraX;
    return;
  }
  let delta = cameraX - prevRawCameraX;
  // Detect wrap: if delta jumps by more than half the world, correct it
  if (delta > W / 2) delta -= W;
  if (delta < -W / 2) delta += W;
  unwrappedCameraX += delta;
  prevRawCameraX = cameraX;
}

function drawMountainLayers(
  ctx: CanvasRenderingContext2D,
  cameraX: number,
  w: number,
  screenGroundY: number,
) {
  trackUnwrappedCamera(cameraX);
  for (const layer of CLIFF_LAYERS) {
    drawCliffLayer(ctx, unwrappedCameraX, screenGroundY, w, layer);
  }
}

// --- Sine system (surface rocks only) ---

interface SineTerm {
  freq: number;
  amp: number;
  phase: number;
  fn: "sin" | "abs";
}

/** Evaluate sum-of-sines height, normalised to 0..1. */
function evalSineHeight(worldX: number, sines: SineTerm[]): number {
  let total = 0;
  let ampSum = 0;
  for (const s of sines) {
    const angle = (worldX * s.freq * TWO_PI) / W + s.phase;
    const val =
      s.fn === "abs"
        ? Math.abs(Math.sin(angle))
        : (Math.sin(angle) + 1) * 0.5;
    total += val * s.amp;
    ampSum += s.amp;
  }
  return total / ampSum;
}

// Surface rocks — high-frequency rocky profile at the horizon
const SURFACE_ROCK_SINES: SineTerm[] = [
  { freq: 31, amp: 0.3, phase: 0.4, fn: "abs" },
  { freq: 47, amp: 0.25, phase: 1.7, fn: "abs" },
  { freq: 67, amp: 0.2, phase: 2.9, fn: "abs" },
  { freq: 97, amp: 0.15, phase: 0.8, fn: "sin" },
  { freq: 19, amp: 0.1, phase: 3.3, fn: "abs" },
];

const SURFACE_ROCK_MAX_HEIGHT = 8;
const SURFACE_ROCK_STEP = 2;

function drawSurfaceRocks(
  ctx: CanvasRenderingContext2D,
  cameraX: number,
  screenGroundY: number,
  w: number,
) {
  ctx.fillStyle = "#2e1844";
  ctx.beginPath();
  const by0 = Math.round(screenGroundY + curveOffset(0, w));
  ctx.moveTo(0, by0);

  for (let sx = 0; sx <= w; sx += SURFACE_ROCK_STEP) {
    const by = Math.round(screenGroundY + curveOffset(sx, w));
    const worldX = sx + cameraX;
    const h = evalSineHeight(worldX, SURFACE_ROCK_SINES);
    ctx.lineTo(sx, Math.round(by - h * SURFACE_ROCK_MAX_HEIGHT));
  }

  const byW = Math.round(screenGroundY + curveOffset(w, w));
  ctx.lineTo(w, byW);
  ctx.closePath();
  ctx.fill();

  // Top-edge highlight for definition
  ctx.strokeStyle = "#4a2868";
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let sx = 0; sx <= w; sx += SURFACE_ROCK_STEP) {
    const by = Math.round(screenGroundY + curveOffset(sx, w));
    const worldX = sx + cameraX;
    const h = evalSineHeight(worldX, SURFACE_ROCK_SINES);
    const ry = Math.round(by - h * SURFACE_ROCK_MAX_HEIGHT);
    if (sx === 0) ctx.moveTo(sx, ry);
    else ctx.lineTo(sx, ry);
  }
  ctx.stroke();
}

/** Draw scattered pixel-scale surface marks (cracks, scuffs) and craters. */
function drawSurfaceMarks(
  ctx: CanvasRenderingContext2D,
  cameraX: number,
  screenGroundY: number,
  w: number,
  h: number,
) {
  const groundH = h - screenGroundY;
  if (groundH <= 0) return;

  const camWrapped = ((cameraX % MOON_SURFACE_WIDTH) + MOON_SURFACE_WIDTH) % MOON_SURFACE_WIDTH;

  // --- Craters ---
  const CRATER_TILE = 40;
  const CRATER_TILES_TOTAL = MOON_SURFACE_WIDTH / CRATER_TILE;
  const craterFirst = Math.floor(camWrapped / CRATER_TILE) - 1;
  const craterCount = Math.ceil(w / CRATER_TILE) + 2;

  for (let i = 0; i < craterCount; i++) {
    const tile =
      ((craterFirst + i) % CRATER_TILES_TOTAL + CRATER_TILES_TOTAL) %
      CRATER_TILES_TOTAL;
    const h0 = terrainHash(tile * 83 + 53);
    if (h0 > 0.45) continue;

    const worldX = tile * CRATER_TILE + terrainHash(tile * 89 + 59) * CRATER_TILE;
    let sx = worldX - camWrapped;
    if (sx < -CRATER_TILE) sx += MOON_SURFACE_WIDTH;
    if (sx > w + CRATER_TILE) continue;

    const cx = Math.floor(sx);
    const cy = screenGroundY + curveOffset(cx, w) + 3 + Math.floor(terrainHash(tile * 97 + 61) * Math.min(groundH - 14, 18));
    const radius = 5 + Math.floor(terrainHash(tile * 101 + 67) * 10);
    const rimW = radius * 1.3;
    const rimH = radius * 0.55;

    // Outer rim — slightly lighter ring
    ctx.fillStyle = "#3d2850";
    ctx.beginPath();
    ctx.ellipse(cx, cy, rimW, rimH, 0, 0, TWO_PI);
    ctx.fill();

    // Dark crater bowl
    ctx.fillStyle = "#08030e";
    ctx.beginPath();
    ctx.ellipse(cx, cy + 1, rimW - 2, rimH - 1, 0, 0, TWO_PI);
    ctx.fill();

    // Bright upper rim highlight (top arc only)
    ctx.strokeStyle = "#6a4090";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.ellipse(cx, cy, rimW, rimH, 0, Math.PI, TWO_PI);
    ctx.stroke();

    // Inner shadow offset for depth
    ctx.fillStyle = "#0a0414";
    ctx.beginPath();
    ctx.ellipse(cx + 1, cy + 2, rimW * 0.6, rimH * 0.5, 0, 0, TWO_PI);
    ctx.fill();

    // Ejecta debris around larger craters
    if (radius >= 9) {
      const debrisCount = 3 + Math.floor(terrainHash(tile * 109 + 71) * 5);
      for (let d = 0; d < debrisCount; d++) {
        const angle = terrainHash(tile * 113 + d * 7) * TWO_PI;
        const dist = rimW + 2 + terrainHash(tile * 119 + d * 11) * 10;
        const dx = cx + Math.floor(Math.cos(angle) * dist);
        const dy = cy + Math.floor(Math.sin(angle) * dist * 0.4);
        ctx.fillStyle = "#2a1840";
        ctx.fillRect(dx, dy, 2, 1);
      }
    }
  }

  // --- Cracks / scuffs ---
  const TILE = 16;
  const CRACK_TILES_TOTAL = MOON_SURFACE_WIDTH / TILE;
  const firstTile = Math.floor(camWrapped / TILE) - 1;
  const tileCount = Math.ceil(w / TILE) + 2;

  for (let i = 0; i < tileCount; i++) {
    const tile =
      ((firstTile + i) % CRACK_TILES_TOTAL + CRACK_TILES_TOTAL) %
      CRACK_TILES_TOTAL;
    const h0 = terrainHash(tile * 59 + 29);
    if (h0 > 0.35) continue;

    const worldX = tile * TILE + terrainHash(tile * 61 + 31) * TILE;
    let sx = worldX - camWrapped;
    if (sx < -TILE) sx += MOON_SURFACE_WIDTH;

    const markSx = Math.floor(sx);
    const markY = screenGroundY + curveOffset(markSx, w) + 3 + Math.floor(terrainHash(tile * 67 + 37) * Math.min(groundH - 4, 20));
    const markLen = 2 + Math.floor(terrainHash(tile * 71 + 41) * 6);

    // Horizontal crack / scuff (single pixel height)
    ctx.fillStyle = "#4a2868";
    ctx.fillRect(markSx, markY, markLen, 1);
    // Shadow below the crack
    ctx.fillStyle = "#0e0618";
    ctx.fillRect(markSx, markY + 1, markLen, 1);
  }
}

export function drawGround(
  ctx: CanvasRenderingContext2D,
  cameraX: number,
  cameraY: number,
  w: number,
  h: number,
) {
  const screenGroundY = GROUND_LEVEL - cameraY;
  const groundH = h - screenGroundY;
  if (groundH <= 0) return;

  const CURVE_STEP = 4;

  // Mountain silhouettes drawn above the horizon (into the sky area)
  drawMountainLayers(ctx, cameraX, w, screenGroundY);

  // Curved colour bands — each fills from its curved top edge to canvas bottom.
  // Later (darker) bands paint over earlier ones.
  const bandCount = Math.ceil(groundH / BAND_HEIGHT);
  for (let band = 0; band < bandCount; band++) {
    const colourIdx = Math.min(band, BAND_COLOURS.length - 1);
    ctx.fillStyle = BAND_COLOURS[colourIdx];
    ctx.beginPath();
    ctx.moveTo(0, h);
    for (let sx = 0; sx <= w; sx += CURVE_STEP) {
      const by = Math.round(screenGroundY + curveOffset(sx, w) + band * BAND_HEIGHT);
      ctx.lineTo(sx, by);
    }
    ctx.lineTo(w, h);
    ctx.closePath();
    ctx.fill();
  }

  // Surface detail — rocky profile at horizon, then cracks/scuffs
  drawSurfaceRocks(ctx, cameraX, screenGroundY, w);
  drawSurfaceMarks(ctx, cameraX, screenGroundY, w, h);

  // Bright horizon accent line (magenta glow — curved)
  withGlow(ctx, COLOURS.pink, 8, () => {
    ctx.strokeStyle = COLOURS.pink;
    ctx.lineWidth = 2;
    ctx.beginPath();
    for (let sx = 0; sx <= w; sx += CURVE_STEP) {
      const by = Math.round(screenGroundY + curveOffset(sx, w));
      if (sx === 0) ctx.moveTo(sx, by);
      else ctx.lineTo(sx, by);
    }
    ctx.stroke();
  });

  // Secondary accent line below the horizon (softer, curved)
  ctx.strokeStyle = "rgba(255, 0, 255, 0.2)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let sx = 0; sx <= w; sx += CURVE_STEP) {
    const by = Math.round(screenGroundY + curveOffset(sx, w) + 3);
    if (sx === 0) ctx.moveTo(sx, by);
    else ctx.lineTo(sx, by);
  }
  ctx.stroke();
}

// ---------------------------------------------------------------------------
// Background (clear + starfield + ground)
// ---------------------------------------------------------------------------

export function drawBackground(
  ctx: CanvasRenderingContext2D,
  cameraX: number,
  cameraY: number,
  w: number,
  h: number,
  now?: number,
) {
  ctx.fillStyle = COLOURS.background;
  ctx.fillRect(0, 0, w, h);
  drawStarfield(ctx, cameraX, cameraY, w, h, now ?? 0);
  drawGround(ctx, cameraX, cameraY, w, h);
}

// ---------------------------------------------------------------------------
// Lander — pixel-art sprite with glow
// ---------------------------------------------------------------------------

export function drawLander(
  ctx: CanvasRenderingContext2D,
  screenX: number,
  screenY: number,
  thrusting: boolean,
  colour?: string,
  name?: string,
  thrustLeft?: boolean,
  thrustRight?: boolean,
) {
  const bodyColour = colour ?? COLOURS.cyan;
  const x = Math.floor(screenX - LANDER_WIDTH / 2);
  const y = Math.floor(screenY - LANDER_HEIGHT);

  // Name label above lander
  if (name) {
    ctx.font = "10px monospace";
    ctx.fillStyle = bodyColour;
    ctx.textAlign = "center";
    ctx.fillText(name, Math.floor(screenX), y - 6);
    ctx.textAlign = "start";
  }

  // Sprite blit with glow
  ctx.imageSmoothingEnabled = false;
  withGlow(ctx, bodyColour, 5, () => {
    const sprite = getLanderSprite(colour);
    ctx.drawImage(sprite, x, y, LANDER_WIDTH, LANDER_HEIGHT);
  });

  // Main downward thrust flame (animated, procedural)
  if (thrusting) {
    const frame = getThrustFrame();
    const flameY = y + LANDER_HEIGHT - 2;
    const cx = x + LANDER_WIDTH / 2;
    // Flame height varies by frame
    const flameH = [8, 12, 16][frame % 3];
    withGlow(ctx, COLOURS.orange, 16, () => {
      // Twin side flames
      ctx.fillStyle = COLOURS.pink;
      ctx.fillRect(cx - 6, flameY, 4, flameH - 2);
      ctx.fillRect(cx + 2, flameY, 4, flameH - 2);
      // Central flame
      ctx.fillStyle = COLOURS.yellow;
      ctx.fillRect(cx - 4, flameY + 2, 8, flameH - 4);
      // Hot core
      ctx.fillStyle = COLOURS.orange;
      ctx.fillRect(cx - 3, flameY + 4, 6, flameH - 2);
      // Tip
      ctx.fillStyle = COLOURS.pink;
      ctx.fillRect(cx - 2, flameY + flameH - 4, 4, 4);
    });
  }

  // Lateral thrust jets (small sideways flames on the hull)
  if (thrustLeft || thrustRight) {
    const jetY = y + Math.floor(LANDER_HEIGHT * 0.35);
    withGlow(ctx, COLOURS.pink, 8, () => {
      if (thrustRight) {
        // Jet on left side pushing right
        ctx.fillStyle = COLOURS.pink;
        ctx.fillRect(x - 5, jetY, 5, 3);
        ctx.fillStyle = COLOURS.orange;
        ctx.fillRect(x - 9, jetY, 4, 3);
      }
      if (thrustLeft) {
        // Jet on right side pushing left
        ctx.fillStyle = COLOURS.pink;
        ctx.fillRect(x + LANDER_WIDTH, jetY, 5, 3);
        ctx.fillStyle = COLOURS.orange;
        ctx.fillRect(x + LANDER_WIDTH + 5, jetY, 4, 3);
      }
    });
  }
}

// ---------------------------------------------------------------------------
// Astronaut — pixel-art sprite with glow
// ---------------------------------------------------------------------------

export function drawAstronaut(
  ctx: CanvasRenderingContext2D,
  screenX: number,
  screenY: number,
  colour?: string,
  name?: string,
  moving?: boolean,
) {
  const helmetColour = colour ?? COLOURS.cyan;
  const x = Math.floor(screenX - ASTRONAUT_WIDTH / 2);
  const y = Math.floor(screenY - ASTRONAUT_HEIGHT);

  // Name label
  if (name) {
    ctx.font = "10px monospace";
    ctx.fillStyle = helmetColour;
    ctx.textAlign = "center";
    ctx.fillText(name, Math.floor(screenX), y - 6);
    ctx.textAlign = "start";
  }

  // Sprite blit with glow
  ctx.imageSmoothingEnabled = false;
  const frame = moving ? getWalkFrame() : 0;
  withGlow(ctx, helmetColour, 4, () => {
    const sprite = getAstronautSprite(colour, frame);
    ctx.drawImage(sprite, x, y, ASTRONAUT_WIDTH, ASTRONAUT_HEIGHT);
  });
}

// ---------------------------------------------------------------------------
// Fuel deposit — pixel-art sprite with glow
// ---------------------------------------------------------------------------

export const DEPOSIT_COLOURS: Record<FuelType, string> = {
  circle: COLOURS.cyan,
  triangle: COLOURS.pink,
  square: COLOURS.yellow,
  pentagon: COLOURS.green,
  hexagon: COLOURS.orange,
  heptagon: COLOURS.softPink,
  octagon: COLOURS.purple,
};

const DEPOSIT_SIZE = 16;

export function drawDeposit(
  ctx: CanvasRenderingContext2D,
  screenX: number,
  screenY: number,
  type: FuelType,
  alpha?: number,
) {
  const colour = DEPOSIT_COLOURS[type] ?? COLOURS.cyan;
  const cx = Math.floor(screenX - DEPOSIT_SIZE / 2);
  const cy = Math.floor(screenY - DEPOSIT_SIZE - 2);

  const a = alpha ?? 1;
  const prevAlpha = ctx.globalAlpha;
  ctx.globalAlpha = prevAlpha * a;
  ctx.imageSmoothingEnabled = false;
  withGlow(ctx, colour, 8 * a, () => {
    const sprite = getDepositSprite(type);
    ctx.drawImage(sprite, cx, cy, DEPOSIT_SIZE, DEPOSIT_SIZE);
  });
  ctx.globalAlpha = prevAlpha;
}

// ---------------------------------------------------------------------------
// Edge-of-screen arrow
// ---------------------------------------------------------------------------

const ARROW_SIZE = 10;
const ARROW_MARGIN = 16;

export function drawArrow(
  ctx: CanvasRenderingContext2D,
  targetScreenX: number,
  screenW: number,
  screenH: number,
  colour: string,
  label?: string,
) {
  if (targetScreenX >= -20 && targetScreenX <= screenW + 20) return;

  const pointsLeft = targetScreenX < 0;
  const arrowX = pointsLeft ? ARROW_MARGIN : screenW - ARROW_MARGIN;
  const arrowY = screenH - 60;

  withGlow(ctx, colour, 6, () => {
    ctx.fillStyle = colour;
    ctx.beginPath();
    if (pointsLeft) {
      ctx.moveTo(arrowX, arrowY);
      ctx.lineTo(arrowX + ARROW_SIZE, arrowY - ARROW_SIZE);
      ctx.lineTo(arrowX + ARROW_SIZE, arrowY + ARROW_SIZE);
    } else {
      ctx.moveTo(arrowX, arrowY);
      ctx.lineTo(arrowX - ARROW_SIZE, arrowY - ARROW_SIZE);
      ctx.lineTo(arrowX - ARROW_SIZE, arrowY + ARROW_SIZE);
    }
    ctx.closePath();
    ctx.fill();
  });

  if (label) {
    ctx.font = "10px monospace";
    ctx.fillStyle = colour;
    ctx.textAlign = pointsLeft ? "left" : "right";
    const labelX = pointsLeft ? arrowX + ARROW_SIZE + 4 : arrowX - ARROW_SIZE - 4;
    ctx.fillText(label, labelX, arrowY + 4);
    ctx.textAlign = "start";
  }
}

// ---------------------------------------------------------------------------
// Speech bubbles
// ---------------------------------------------------------------------------

const BUBBLE_MAX = 4;
const BUBBLE_LINE_HEIGHT = 18;
const BUBBLE_PADDING_X = 8;
const BUBBLE_PADDING_Y = 4;
const BUBBLE_OFFSET_Y = 8;

export function drawBubbles(
  ctx: CanvasRenderingContext2D,
  screenX: number,
  screenY: number,
  messages: string[],
) {
  if (messages.length === 0) return;
  const recent = messages.slice(-BUBBLE_MAX);

  ctx.font = "11px monospace";
  ctx.textAlign = "center";

  for (let i = 0; i < recent.length; i++) {
    const text = recent[i];
    const slot = recent.length - 1 - i;
    const alpha = 1 - slot * 0.25;
    const y = screenY - BUBBLE_OFFSET_Y - slot * BUBBLE_LINE_HEIGHT;

    const metrics = ctx.measureText(text);
    const bw = metrics.width + BUBBLE_PADDING_X * 2;
    const bh = BUBBLE_LINE_HEIGHT;
    const bx = Math.floor(screenX - bw / 2);
    const by = Math.floor(y - bh);

    // Background
    ctx.fillStyle = `rgba(10, 10, 15, ${0.75 * alpha})`;
    ctx.beginPath();
    ctx.roundRect(bx, by, bw, bh, 4);
    ctx.fill();

    // Border with glow
    ctx.shadowColor = COLOURS.pink;
    ctx.shadowBlur = 4;
    ctx.strokeStyle = `rgba(255, 0, 255, ${0.5 * alpha})`;
    ctx.lineWidth = 1;
    ctx.stroke();
    ctx.shadowBlur = 0;

    // Text
    ctx.fillStyle = `rgba(0, 255, 255, ${alpha})`;
    ctx.fillText(text, Math.floor(screenX), Math.floor(y - BUBBLE_PADDING_Y));
  }

  ctx.textAlign = "start";
}

// ---------------------------------------------------------------------------
// Success splash — neon glow text
// ---------------------------------------------------------------------------

export function drawSplash(
  ctx: CanvasRenderingContext2D,
  w: number,
  h: number,
  alpha: number,
  elapsed: number,
) {
  const a = Math.min(1, Math.max(0, alpha));
  const t = elapsed; // seconds since splash started

  // Overlay — deepens over time
  ctx.fillStyle = `rgba(10, 10, 15, ${a * 0.7})`;
  ctx.fillRect(0, 0, w, h);

  // Radiating starburst lines from centre
  const cx = w / 2;
  const cy = h / 2;
  const burstCount = 24;
  const burstRadius = Math.min(w, h) * 0.6;
  ctx.save();
  ctx.globalAlpha = a * 0.15;
  for (let i = 0; i < burstCount; i++) {
    const angle = (i / burstCount) * Math.PI * 2 + t * 0.3;
    ctx.strokeStyle = i % 2 === 0 ? COLOURS.pink : COLOURS.cyan;
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.lineTo(cx + Math.cos(angle) * burstRadius, cy + Math.sin(angle) * burstRadius);
    ctx.stroke();
  }
  ctx.restore();

  // Pulsing ring
  const ringPulse = 1 + 0.15 * Math.sin(t * 4);
  const ringRadius = 120 * ringPulse * Math.min(1, t * 0.5);
  ctx.save();
  ctx.globalAlpha = a * 0.4;
  ctx.strokeStyle = COLOURS.pink;
  ctx.lineWidth = 3;
  ctx.shadowColor = COLOURS.pink;
  ctx.shadowBlur = 20;
  ctx.beginPath();
  ctx.arc(cx, cy, ringRadius, 0, Math.PI * 2);
  ctx.stroke();
  ctx.strokeStyle = COLOURS.cyan;
  ctx.shadowColor = COLOURS.cyan;
  ctx.beginPath();
  ctx.arc(cx, cy, ringRadius * 0.6, 0, Math.PI * 2);
  ctx.stroke();
  ctx.restore();
  ctx.shadowBlur = 0;

  ctx.textAlign = "center";

  // Title — scale bounce on entry
  const titleScale = Math.min(1, t * 2) * (1 + 0.03 * Math.sin(t * 6));
  ctx.save();
  ctx.translate(cx, cy - 30);
  ctx.scale(titleScale, titleScale);
  withGlow(ctx, COLOURS.pink, 25 + 10 * Math.sin(t * 3), () => {
    ctx.font = "bold 48px monospace";
    ctx.fillStyle = `rgba(255, 0, 255, ${a})`;
    ctx.fillText("MISSION COMPLETE", 0, 0);
  });
  ctx.restore();

  // Subtitle — fades in after title
  const subAlpha = Math.max(0, Math.min(1, (t - 0.5) * 2));
  withGlow(ctx, COLOURS.cyan, 12, () => {
    ctx.font = "16px monospace";
    ctx.fillStyle = `rgba(0, 255, 255, ${a * subAlpha * 0.9})`;
    ctx.fillText("you escaped the moon", cx, cy + 20);
  });

  // Floating sparkle dots
  const sparkleCount = 16;
  ctx.save();
  for (let i = 0; i < sparkleCount; i++) {
    const seed = i * 7919;
    const angle = ((seed % 360) / 360) * Math.PI * 2 + t * (0.2 + (i % 3) * 0.1);
    const dist = 60 + (seed % 100) + 20 * Math.sin(t * 2 + i);
    const sx = cx + Math.cos(angle) * dist;
    const sy = cy + Math.sin(angle) * dist;
    const sparkleAlpha = a * (0.4 + 0.6 * Math.abs(Math.sin(t * 3 + i * 1.7)));
    ctx.globalAlpha = sparkleAlpha;
    ctx.fillStyle = i % 3 === 0 ? COLOURS.yellow : i % 3 === 1 ? COLOURS.pink : COLOURS.cyan;
    ctx.fillRect(Math.floor(sx) - 1, Math.floor(sy) - 1, 3, 3);
  }
  ctx.restore();

  // Scanline overlay for retro feel
  ctx.save();
  ctx.globalAlpha = a * 0.04;
  ctx.fillStyle = "#000";
  for (let y = 0; y < h; y += 4) {
    ctx.fillRect(0, y, w, 2);
  }
  ctx.restore();

  ctx.textAlign = "start";
}

// ---------------------------------------------------------------------------
// Crash splash — red-toned failure screen
// ---------------------------------------------------------------------------

export function drawCrashSplash(
  ctx: CanvasRenderingContext2D,
  w: number,
  h: number,
  alpha: number,
  elapsed: number,
) {
  const a = Math.min(1, Math.max(0, alpha));
  const t = elapsed;

  // Dark overlay
  ctx.fillStyle = `rgba(15, 5, 5, ${a * 0.75})`;
  ctx.fillRect(0, 0, w, h);

  const cx = w / 2;
  const cy = h / 2;

  // Flickering static lines (interference / damage effect)
  ctx.save();
  ctx.globalAlpha = a * 0.08;
  const lineCount = 12;
  for (let i = 0; i < lineCount; i++) {
    const ly = (Math.sin(t * 11 + i * 2.7) * 0.5 + 0.5) * h;
    ctx.fillStyle = i % 2 === 0 ? "#ff3333" : "#ff00ff";
    ctx.fillRect(0, Math.floor(ly), w, 1);
  }
  ctx.restore();

  // Pulsing warning ring
  const ringPulse = 1 + 0.2 * Math.sin(t * 6);
  const ringRadius = 100 * ringPulse * Math.min(1, t * 0.6);
  ctx.save();
  ctx.globalAlpha = a * 0.4;
  ctx.strokeStyle = "#ff3333";
  ctx.lineWidth = 3;
  ctx.shadowColor = "#ff3333";
  ctx.shadowBlur = 20;
  ctx.beginPath();
  ctx.arc(cx, cy, ringRadius, 0, Math.PI * 2);
  ctx.stroke();
  ctx.restore();
  ctx.shadowBlur = 0;

  ctx.textAlign = "center";

  // Title — screen shake on entry
  const shakeX = t < 0.5 ? (Math.random() - 0.5) * 8 * (1 - t * 2) : 0;
  const shakeY = t < 0.5 ? (Math.random() - 0.5) * 8 * (1 - t * 2) : 0;
  const titleScale = Math.min(1, t * 3);
  ctx.save();
  ctx.translate(cx + shakeX, cy - 30 + shakeY);
  ctx.scale(titleScale, titleScale);
  withGlow(ctx, "#ff3333", 25 + 10 * Math.sin(t * 5), () => {
    ctx.font = "bold 48px monospace";
    ctx.fillStyle = `rgba(255, 50, 50, ${a})`;
    ctx.fillText("YOU CRASHED", 0, 0);
  });
  ctx.restore();

  // Subtitle
  const subAlpha = Math.max(0, Math.min(1, (t - 0.5) * 2));
  withGlow(ctx, COLOURS.pink, 10, () => {
    ctx.font = "16px monospace";
    ctx.fillStyle = `rgba(255, 100, 100, ${a * subAlpha * 0.9})`;
    ctx.fillText("the lander couldn't handle the impact", cx, cy + 20);
  });

  // Falling debris dots
  const debrisCount = 12;
  ctx.save();
  for (let i = 0; i < debrisCount; i++) {
    const seed = i * 3571;
    const dx = ((seed % 300) - 150);
    const dy = t * (40 + (seed % 60)) - 20;
    const sx = cx + dx + Math.sin(t * 2 + i) * 10;
    const sy = cy + dy;
    if (sy > h + 10) continue;
    const debrisAlpha = a * Math.max(0, 1 - dy / 200);
    ctx.globalAlpha = debrisAlpha;
    ctx.fillStyle = i % 3 === 0 ? "#ff3333" : i % 3 === 1 ? "#ff8844" : "#ffaa00";
    ctx.fillRect(Math.floor(sx) - 1, Math.floor(sy) - 1, 3, 2);
  }
  ctx.restore();

  // Scanlines
  ctx.save();
  ctx.globalAlpha = a * 0.06;
  ctx.fillStyle = "#000";
  for (let y = 0; y < h; y += 4) {
    ctx.fillRect(0, y, w, 2);
  }
  ctx.restore();

  ctx.textAlign = "start";
}
