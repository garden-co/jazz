import {
  GROUND_LEVEL,
  LANDER_WIDTH,
  LANDER_HEIGHT,
  ASTRONAUT_WIDTH,
  ASTRONAUT_HEIGHT,
  COLOURS,
  type FuelType,
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

// ---------------------------------------------------------------------------
// Fuel deposit — shape drawn on the ground, colour indicates fuel type
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Edge-of-screen arrow — points toward an off-screen target
// ---------------------------------------------------------------------------

const ARROW_SIZE = 10;
const ARROW_MARGIN = 16;

/** Draw an arrow at the edge of the screen pointing toward a target. */
export function drawArrow(
  ctx: CanvasRenderingContext2D,
  targetScreenX: number,
  screenW: number,
  screenH: number,
  colour: string,
  label?: string,
) {
  // Only draw if target is off-screen horizontally
  if (targetScreenX >= -20 && targetScreenX <= screenW + 20) return;

  const pointsLeft = targetScreenX < 0;
  const arrowX = pointsLeft ? ARROW_MARGIN : screenW - ARROW_MARGIN;
  const arrowY = screenH - 60; // near bottom, above controls hint

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
// Fuel deposit — shape drawn on the ground, colour indicates fuel type
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

const DEPOSIT_RADIUS = 8;

/** Draw a fuel deposit at the given screen position. */
export function drawDeposit(
  ctx: CanvasRenderingContext2D,
  screenX: number,
  screenY: number,
  type: FuelType,
) {
  const colour = DEPOSIT_COLOURS[type] ?? COLOURS.cyan;
  const cx = Math.floor(screenX);
  const cy = Math.floor(screenY - DEPOSIT_RADIUS - 2);
  const r = DEPOSIT_RADIUS;

  ctx.fillStyle = colour;
  ctx.beginPath();

  if (type === "circle") {
    ctx.arc(cx, cy, r, 0, Math.PI * 2);
  } else {
    const sides =
      type === "triangle" ? 3
      : type === "square" ? 4
      : type === "pentagon" ? 5
      : type === "hexagon" ? 6
      : type === "heptagon" ? 7
      : 8; // octagon
    const angleStep = (Math.PI * 2) / sides;
    const startAngle = -Math.PI / 2; // point upward
    for (let i = 0; i < sides; i++) {
      const a = startAngle + i * angleStep;
      const px = cx + r * Math.cos(a);
      const py = cy + r * Math.sin(a);
      if (i === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    }
    ctx.closePath();
  }

  ctx.fill();
}

// ---------------------------------------------------------------------------
// Success splash — shown after launch
// ---------------------------------------------------------------------------

/** Draw a "MISSION COMPLETE" splash with fade-in alpha (0–1). */
export function drawSplash(
  ctx: CanvasRenderingContext2D,
  w: number,
  h: number,
  alpha: number,
) {
  const a = Math.min(1, Math.max(0, alpha));

  // Overlay
  ctx.fillStyle = `rgba(10, 10, 15, ${a * 0.6})`;
  ctx.fillRect(0, 0, w, h);

  ctx.textAlign = "center";

  // Title
  ctx.font = "bold 48px monospace";
  ctx.fillStyle = `rgba(255, 0, 255, ${a})`;
  ctx.fillText("MISSION COMPLETE", w / 2, h / 2 - 20);

  // Subtitle
  ctx.font = "16px monospace";
  ctx.fillStyle = `rgba(0, 255, 255, ${a * 0.8})`;
  ctx.fillText("you escaped the moon", w / 2, h / 2 + 20);

  ctx.textAlign = "start";
}
