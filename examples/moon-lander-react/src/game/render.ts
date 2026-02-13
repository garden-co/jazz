import { CANVAS_WIDTH, CANVAS_HEIGHT, GROUND_LEVEL, COLOURS } from "./constants.js";

// ---------------------------------------------------------------------------
// Starfield — 3 parallax layers with deterministic pseudo-random positions
// ---------------------------------------------------------------------------

interface Star {
  x: number;
  y: number;
  size: number;
  brightness: number;
  layer: number; // 0 = far, 1 = mid, 2 = near
}

const PARALLAX_SPEEDS = [0.05, 0.15, 0.3];
const STARS_PER_LAYER = 70;

/** Seeded pseudo-random (simple LCG). Deterministic per index. */
function seededRandom(seed: number): number {
  const x = Math.sin(seed * 127.1 + seed * 311.7) * 43758.5453;
  return x - Math.floor(x);
}

// Pre-generate stars once
const stars: Star[] = [];
for (let layer = 0; layer < 3; layer++) {
  for (let i = 0; i < STARS_PER_LAYER; i++) {
    const seed = layer * 1000 + i;
    stars.push({
      x: seededRandom(seed) * CANVAS_WIDTH * 3, // Wider than screen for scrolling
      y: seededRandom(seed + 0.5) * GROUND_LEVEL, // Only above ground
      size: layer === 2 ? 2 : 1,
      brightness: 0.4 + seededRandom(seed + 0.3) * 0.6,
      layer,
    });
  }
}

export function drawStarfield(ctx: CanvasRenderingContext2D, cameraX: number) {
  for (const star of stars) {
    const parallax = PARALLAX_SPEEDS[star.layer];
    let sx = star.x - cameraX * parallax;
    // Wrap around screen
    sx = ((sx % (CANVAS_WIDTH * 3)) + CANVAS_WIDTH * 3) % (CANVAS_WIDTH * 3) - CANVAS_WIDTH;
    if (sx < -2 || sx > CANVAS_WIDTH + 2) continue;

    const alpha = star.brightness;
    ctx.fillStyle = `rgba(255, 255, 255, ${alpha})`;
    ctx.fillRect(Math.floor(sx), Math.floor(star.y), star.size, star.size);
  }
}

// ---------------------------------------------------------------------------
// Moon surface
// ---------------------------------------------------------------------------

export function drawGround(ctx: CanvasRenderingContext2D) {
  ctx.fillStyle = COLOURS.ground;
  ctx.fillRect(0, GROUND_LEVEL, CANVAS_WIDTH, CANVAS_HEIGHT - GROUND_LEVEL);
}

// ---------------------------------------------------------------------------
// Background (clear + starfield + ground)
// ---------------------------------------------------------------------------

export function drawBackground(ctx: CanvasRenderingContext2D, cameraX: number) {
  ctx.fillStyle = COLOURS.background;
  ctx.fillRect(0, 0, CANVAS_WIDTH, CANVAS_HEIGHT);
  drawStarfield(ctx, cameraX);
  drawGround(ctx);
}
