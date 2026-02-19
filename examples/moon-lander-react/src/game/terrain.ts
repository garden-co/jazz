import {
  COLOURS,
  curveOffset,
  GROUND_LEVEL,
  MOON_SURFACE_WIDTH,
} from "./constants";
import { withGlow } from "./render";
import { seededRand } from "./world";

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

const STAR_COLOURS = [
  "#ffffff",
  "#ffffff",
  "#ffffff",
  "#aaddff",
  "#ffaadd",
  "#ddaaff",
  "#aaffee",
];

function hexToRgb(hex: string): [number, number, number] {
  return [
    parseInt(hex.slice(1, 3), 16),
    parseInt(hex.slice(3, 5), 16),
    parseInt(hex.slice(5, 7), 16),
  ];
}

const STAR_FIELD_WIDTH = 4000;
const STAR_FIELD_HEIGHT = 2000;
const stars: Star[] = [];
for (let layer = 0; layer < 3; layer++) {
  for (let i = 0; i < STARS_PER_LAYER; i++) {
    const seed = layer * 1000 + i;
    const starColour =
      STAR_COLOURS[Math.floor(seededRand(seed + 0.9) * STAR_COLOURS.length)];
    stars.push({
      x: seededRand(seed) * STAR_FIELD_WIDTH,
      y: seededRand(seed + 0.5) * STAR_FIELD_HEIGHT,
      size: layer === 2 ? 2 : 1,
      brightness: 0.4 + seededRand(seed + 0.3) * 0.6,
      layer,
      colour: starColour,
      rgb: hexToRgb(starColour),
      twinklePhase: seededRand(seed + 0.7) * Math.PI * 2,
    });
  }
}

function drawStarfield(
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
// Terrain — 16-bit Mega Drive / Street Fighter stage floor aesthetic
// Bold colour bands, chunky rock silhouettes, hard pixel outlines
// ---------------------------------------------------------------------------

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
    const idx0 = ((tileIdx % tilesPerWorld) + tilesPerWorld) % tilesPerWorld;
    const idx1 = (idx0 + 1) % tilesPerWorld;
    const h0 = seededRand(idx0 * 127 + s.seed);
    const h1 = seededRand(idx1 * 127 + s.seed);
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
  // Mid — angular ridges
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
    shadowColour: "#120e18",
  },
  // Near — jagged cliff faces
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
  const baseY = (sx: number) => Math.round(screenGroundY + curveOffset(sx, w));

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
  if (Number.isNaN(prevRawCameraX)) {
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
  w: number,
  screenGroundY: number,
) {
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
    const angle = (worldX * s.freq * Math.PI * 2) / W + s.phase;
    const val =
      s.fn === "abs" ? Math.abs(Math.sin(angle)) : (Math.sin(angle) + 1) * 0.5;
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

  const camWrapped =
    ((cameraX % MOON_SURFACE_WIDTH) + MOON_SURFACE_WIDTH) % MOON_SURFACE_WIDTH;

  // --- Craters ---
  const CRATER_TILE = 40;
  const CRATER_TILES_TOTAL = MOON_SURFACE_WIDTH / CRATER_TILE;
  const craterFirst = Math.floor(camWrapped / CRATER_TILE) - 1;
  const craterCount = Math.ceil(w / CRATER_TILE) + 2;

  for (let i = 0; i < craterCount; i++) {
    const tile =
      (((craterFirst + i) % CRATER_TILES_TOTAL) + CRATER_TILES_TOTAL) %
      CRATER_TILES_TOTAL;
    const h0 = seededRand(tile * 83 + 53);
    if (h0 > 0.45) continue;

    const worldX =
      tile * CRATER_TILE + seededRand(tile * 89 + 59) * CRATER_TILE;
    let sx = worldX - camWrapped;
    if (sx < -CRATER_TILE) sx += MOON_SURFACE_WIDTH;
    if (sx > w + CRATER_TILE) continue;

    const cx = Math.floor(sx);
    const cy =
      screenGroundY +
      curveOffset(cx, w) +
      3 +
      Math.floor(seededRand(tile * 97 + 61) * Math.min(groundH - 14, 18));
    const radius = 5 + Math.floor(seededRand(tile * 101 + 67) * 10);
    const rimW = radius * 1.3;
    const rimH = radius * 0.55;

    // Outer rim — slightly lighter ring
    ctx.fillStyle = "#3d2850";
    ctx.beginPath();
    ctx.ellipse(cx, cy, rimW, rimH, 0, 0, Math.PI * 2);
    ctx.fill();

    // Dark crater bowl
    ctx.fillStyle = "#08030e";
    ctx.beginPath();
    ctx.ellipse(cx, cy + 1, rimW - 2, rimH - 1, 0, 0, Math.PI * 2);
    ctx.fill();

    // Bright upper rim highlight (top arc only)
    ctx.strokeStyle = "#6a4090";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.ellipse(cx, cy, rimW, rimH, 0, Math.PI, Math.PI * 2);
    ctx.stroke();

    // Inner shadow offset for depth
    ctx.fillStyle = "#0a0414";
    ctx.beginPath();
    ctx.ellipse(cx + 1, cy + 2, rimW * 0.6, rimH * 0.5, 0, 0, Math.PI * 2);
    ctx.fill();

    // Ejecta debris around larger craters
    if (radius >= 9) {
      const debrisCount = 3 + Math.floor(seededRand(tile * 109 + 71) * 5);
      for (let d = 0; d < debrisCount; d++) {
        const angle = seededRand(tile * 113 + d * 7) * Math.PI * 2;
        const dist = rimW + 2 + seededRand(tile * 119 + d * 11) * 10;
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
      (((firstTile + i) % CRACK_TILES_TOTAL) + CRACK_TILES_TOTAL) %
      CRACK_TILES_TOTAL;
    const h0 = seededRand(tile * 59 + 29);
    if (h0 > 0.35) continue;

    const worldX = tile * TILE + seededRand(tile * 61 + 31) * TILE;
    let sx = worldX - camWrapped;
    if (sx < -TILE) sx += MOON_SURFACE_WIDTH;

    const markSx = Math.floor(sx);
    const markY =
      screenGroundY +
      curveOffset(markSx, w) +
      3 +
      Math.floor(seededRand(tile * 67 + 37) * Math.min(groundH - 4, 20));
    const markLen = 2 + Math.floor(seededRand(tile * 71 + 41) * 6);

    // Horizontal crack / scuff (single pixel height)
    ctx.fillStyle = "#4a2868";
    ctx.fillRect(markSx, markY, markLen, 1);
    // Shadow below the crack
    ctx.fillStyle = "#0e0618";
    ctx.fillRect(markSx, markY + 1, markLen, 1);
  }
}

function drawGround(
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
  drawMountainLayers(ctx, w, screenGroundY);

  // Curved colour bands — each fills from its curved top edge to canvas bottom.
  // Later (darker) bands paint over earlier ones.
  const bandCount = Math.ceil(groundH / BAND_HEIGHT);
  for (let band = 0; band < bandCount; band++) {
    const colourIdx = Math.min(band, BAND_COLOURS.length - 1);
    ctx.fillStyle = BAND_COLOURS[colourIdx];
    ctx.beginPath();
    ctx.moveTo(0, h);
    for (let sx = 0; sx <= w; sx += CURVE_STEP) {
      const by = Math.round(
        screenGroundY + curveOffset(sx, w) + band * BAND_HEIGHT,
      );
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
  // Track unwrapped camera before both starfield and ground so parallax
  // doesn't jump when cameraX wraps at the world meridian.
  trackUnwrappedCamera(cameraX);
  drawStarfield(ctx, unwrappedCameraX, cameraY, w, h, now ?? 0);
  drawGround(ctx, cameraX, cameraY, w, h);
}
