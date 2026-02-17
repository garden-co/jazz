import {
  GROUND_LEVEL,
  LANDER_WIDTH,
  LANDER_HEIGHT,
  ASTRONAUT_WIDTH,
  ASTRONAUT_HEIGHT,
  COLOURS,
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

export function drawGround(
  ctx: CanvasRenderingContext2D,
  cameraX: number,
  cameraY: number,
  w: number,
  h: number,
) {
  const screenGroundY = GROUND_LEVEL - cameraY;
  ctx.fillStyle = COLOURS.ground;
  ctx.fillRect(0, screenGroundY, w, h - screenGroundY);

  // Subtle grid lines
  const gridSpacing = 10;
  ctx.strokeStyle = "rgba(138, 43, 226, 0.08)";
  ctx.lineWidth = 1;
  // Vertical grid lines (parallax-shifted)
  const startGridX = -(cameraX % gridSpacing);
  for (let x = startGridX; x < w; x += gridSpacing) {
    ctx.beginPath();
    ctx.moveTo(Math.floor(x) + 0.5, screenGroundY);
    ctx.lineTo(Math.floor(x) + 0.5, h);
    ctx.stroke();
  }
  // Horizontal grid lines
  for (let y = screenGroundY + gridSpacing; y < h; y += gridSpacing) {
    ctx.beginPath();
    ctx.moveTo(0, Math.floor(y) + 0.5);
    ctx.lineTo(w, Math.floor(y) + 0.5);
    ctx.stroke();
  }

  // Bright horizon accent line (1-2px magenta)
  withGlow(ctx, COLOURS.pink, 8, () => {
    ctx.strokeStyle = COLOURS.pink;
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(0, screenGroundY);
    ctx.lineTo(w, screenGroundY);
    ctx.stroke();
  });
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
) {
  const colour = DEPOSIT_COLOURS[type] ?? COLOURS.cyan;
  const cx = Math.floor(screenX - DEPOSIT_SIZE / 2);
  const cy = Math.floor(screenY - DEPOSIT_SIZE - 2);

  ctx.imageSmoothingEnabled = false;
  withGlow(ctx, colour, 8, () => {
    const sprite = getDepositSprite(type);
    ctx.drawImage(sprite, cx, cy, DEPOSIT_SIZE, DEPOSIT_SIZE);
  });
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
) {
  const a = Math.min(1, Math.max(0, alpha));

  // Overlay
  ctx.fillStyle = `rgba(10, 10, 15, ${a * 0.6})`;
  ctx.fillRect(0, 0, w, h);

  ctx.textAlign = "center";

  // Title with neon glow
  withGlow(ctx, COLOURS.pink, 20, () => {
    ctx.font = "bold 48px monospace";
    ctx.fillStyle = `rgba(255, 0, 255, ${a})`;
    ctx.fillText("MISSION COMPLETE", w / 2, h / 2 - 20);
  });

  // Subtitle with glow
  withGlow(ctx, COLOURS.cyan, 12, () => {
    ctx.font = "16px monospace";
    ctx.fillStyle = `rgba(0, 255, 255, ${a * 0.8})`;
    ctx.fillText("you escaped the moon", w / 2, h / 2 + 20);
  });

  ctx.textAlign = "start";
}
