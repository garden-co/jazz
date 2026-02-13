import {
  GROUND_LEVEL,
  LANDER_WIDTH,
  LANDER_HEIGHT,
  ASTRONAUT_WIDTH,
  ASTRONAUT_HEIGHT,
  COLOURS,
} from "./constants.js";

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

const PARALLAX_SPEEDS_X = [0.05, 0.15, 0.3];
const PARALLAX_SPEEDS_Y = [0.02, 0.06, 0.12];
const STARS_PER_LAYER = 70;

/** Seeded pseudo-random (simple LCG). Deterministic per index. */
function seededRandom(seed: number): number {
  const x = Math.sin(seed * 127.1 + seed * 311.7) * 43758.5453;
  return x - Math.floor(x);
}

// Pre-generate stars across a virtual strip larger than any viewport
const STAR_FIELD_WIDTH = 4000;
const STAR_FIELD_HEIGHT = 2000;
const stars: Star[] = [];
for (let layer = 0; layer < 3; layer++) {
  for (let i = 0; i < STARS_PER_LAYER; i++) {
    const seed = layer * 1000 + i;
    stars.push({
      x: seededRandom(seed) * STAR_FIELD_WIDTH,
      y: seededRandom(seed + 0.5) * STAR_FIELD_HEIGHT,
      size: layer === 2 ? 2 : 1,
      brightness: 0.4 + seededRandom(seed + 0.3) * 0.6,
      layer,
    });
  }
}

export function drawStarfield(
  ctx: CanvasRenderingContext2D,
  cameraX: number,
  cameraY: number,
  w: number,
  h: number,
) {
  for (const star of stars) {
    const px = PARALLAX_SPEEDS_X[star.layer];
    const py = PARALLAX_SPEEDS_Y[star.layer];
    let sx = star.x - cameraX * px;
    let sy = star.y - cameraY * py;
    // Wrap horizontally
    sx = ((sx % STAR_FIELD_WIDTH) + STAR_FIELD_WIDTH) % STAR_FIELD_WIDTH;
    // Wrap vertically
    sy = ((sy % STAR_FIELD_HEIGHT) + STAR_FIELD_HEIGHT) % STAR_FIELD_HEIGHT;
    if (sx > w + 2 || sy > h + 2) continue;

    ctx.fillStyle = `rgba(255, 255, 255, ${star.brightness})`;
    ctx.fillRect(Math.floor(sx), Math.floor(sy), star.size, star.size);
  }
}

// ---------------------------------------------------------------------------
// Moon surface
// ---------------------------------------------------------------------------

export function drawGround(
  ctx: CanvasRenderingContext2D,
  cameraY: number,
  w: number,
  h: number,
) {
  const screenGroundY = GROUND_LEVEL - cameraY;
  ctx.fillStyle = COLOURS.ground;
  ctx.fillRect(0, screenGroundY, w, h - screenGroundY);
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
) {
  ctx.fillStyle = COLOURS.background;
  ctx.fillRect(0, 0, w, h);
  drawStarfield(ctx, cameraX, cameraY, w, h);
  drawGround(ctx, cameraY, w, h);
}

// ---------------------------------------------------------------------------
// Lander — simple placeholder (coloured rectangle with legs)
// ---------------------------------------------------------------------------

export function drawLander(
  ctx: CanvasRenderingContext2D,
  screenX: number,
  screenY: number,
  thrusting: boolean,
  colour?: string,
  name?: string,
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
    ctx.textAlign = "start"; // reset
  }

  // Body
  ctx.fillStyle = bodyColour;
  ctx.fillRect(x + 4, y, LANDER_WIDTH - 8, LANDER_HEIGHT - 6);

  // Legs
  ctx.fillStyle = bodyColour;
  ctx.fillRect(x, y + LANDER_HEIGHT - 6, 4, 6);
  ctx.fillRect(x + LANDER_WIDTH - 4, y + LANDER_HEIGHT - 6, 4, 6);

  // Window
  ctx.fillStyle = COLOURS.pink;
  ctx.fillRect(x + 8, y + 4, 8, 6);

  // Thrust flame
  if (thrusting) {
    ctx.fillStyle = COLOURS.pink;
    ctx.fillRect(x + 6, y + LANDER_HEIGHT, 4, 8);
    ctx.fillStyle = COLOURS.yellow;
    ctx.fillRect(x + LANDER_WIDTH - 10, y + LANDER_HEIGHT, 4, 8);
    ctx.fillStyle = COLOURS.orange;
    ctx.fillRect(x + 8, y + LANDER_HEIGHT + 4, 8, 6);
  }
}

// ---------------------------------------------------------------------------
// Astronaut — simple placeholder (coloured rectangle with helmet)
// ---------------------------------------------------------------------------

export function drawAstronaut(
  ctx: CanvasRenderingContext2D,
  screenX: number,
  screenY: number,
  colour?: string,
  name?: string,
) {
  const helmetColour = colour ?? COLOURS.cyan;
  const x = Math.floor(screenX - ASTRONAUT_WIDTH / 2);
  const y = Math.floor(screenY - ASTRONAUT_HEIGHT);

  // Name label above astronaut
  if (name) {
    ctx.font = "10px monospace";
    ctx.fillStyle = helmetColour;
    ctx.textAlign = "center";
    ctx.fillText(name, Math.floor(screenX), y - 6);
    ctx.textAlign = "start"; // reset
  }

  // Body
  ctx.fillStyle = "#cccccc";
  ctx.fillRect(x + 2, y + 8, ASTRONAUT_WIDTH - 4, ASTRONAUT_HEIGHT - 8);

  // Helmet
  ctx.fillStyle = helmetColour;
  ctx.fillRect(x + 3, y, ASTRONAUT_WIDTH - 6, 10);

  // Visor
  ctx.fillStyle = COLOURS.pink;
  ctx.fillRect(x + 5, y + 2, 6, 5);
}
