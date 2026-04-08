import {
  ASTRONAUT_HEIGHT,
  ASTRONAUT_WIDTH,
  COLOURS,
  DEPOSIT_COLOURS,
  FUEL_TYPES,
  type FuelType,
  LANDER_HEIGHT,
  LANDER_WIDTH,
} from "./constants";
import {
  type SpriteAnimationState,
  getAstronautSprite,
  getDepositSprite,
  getLanderSprite,
  getThrustFrame,
  getWalkFrame,
} from "./sprites";

// ---------------------------------------------------------------------------
// Glow helper — apply and restore shadowBlur in one place
// ---------------------------------------------------------------------------

export function withGlow(
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
  anim?: SpriteAnimationState,
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
  if (thrusting && anim) {
    const frame = getThrustFrame(anim);
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
  anim?: SpriteAnimationState,
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
  const frame = moving && anim ? getWalkFrame(anim) : 0;
  withGlow(ctx, helmetColour, 4, () => {
    const sprite = getAstronautSprite(colour, frame);
    ctx.drawImage(sprite, x, y, ASTRONAUT_WIDTH, ASTRONAUT_HEIGHT);
  });
}

// ---------------------------------------------------------------------------
// Fuel deposit — pixel-art sprite with glow
// ---------------------------------------------------------------------------

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
// Inventory bar — small deposit icons above the player's head
// ---------------------------------------------------------------------------

const INV_ICON_SIZE = 8;
const INV_ICON_GAP = 3;

export function drawInventoryBar(
  ctx: CanvasRenderingContext2D,
  screenX: number,
  screenY: number,
  inventory: Set<FuelType>,
  requiredFuelType: FuelType,
) {
  // Show the required type first, then any other collected types
  const types: FuelType[] = [requiredFuelType];
  for (const ft of FUEL_TYPES) {
    if (ft === requiredFuelType) continue;
    if (inventory.has(ft)) types.push(ft);
  }
  const totalWidth = types.length * INV_ICON_SIZE + (types.length - 1) * INV_ICON_GAP;
  const startX = Math.floor(screenX - totalWidth / 2);
  const y = Math.floor(screenY);

  for (let i = 0; i < types.length; i++) {
    const ft = types[i];
    const collected = inventory.has(ft);
    const colour = DEPOSIT_COLOURS[ft] ?? COLOURS.cyan;
    const ix = startX + i * (INV_ICON_SIZE + INV_ICON_GAP);

    ctx.save();
    if (collected) {
      ctx.globalAlpha = 1;
      ctx.shadowColor = colour;
      ctx.shadowBlur = 4;
    } else {
      ctx.globalAlpha = 0.25;
    }

    const sprite = getDepositSprite(ft);
    ctx.imageSmoothingEnabled = false;
    ctx.drawImage(sprite, ix, y, INV_ICON_SIZE, INV_ICON_SIZE);

    ctx.restore();
  }
}
