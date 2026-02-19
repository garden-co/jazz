import { COLOURS } from "./constants";
import { withGlow } from "./render";
import { seededRand } from "./world";

// ---------------------------------------------------------------------------
// Full-screen overlays (arcade style)
// ---------------------------------------------------------------------------

/** Scanline overlay across the full screen. */
function drawScanlines(
  ctx: CanvasRenderingContext2D,
  w: number,
  h: number,
  alpha: number,
) {
  ctx.save();
  ctx.globalAlpha = alpha;
  ctx.fillStyle = "#000";
  for (let sy = 0; sy < h; sy += 4) {
    ctx.fillRect(0, sy, w, 2);
  }
  ctx.restore();
}

/** Radiating starburst lines from a centre point. */
function drawStarburst(
  ctx: CanvasRenderingContext2D,
  cx: number,
  cy: number,
  radius: number,
  lineCount: number,
  colour: string,
  alpha: number,
  rotation: number,
) {
  ctx.save();
  ctx.globalAlpha = alpha;
  ctx.strokeStyle = colour;
  ctx.lineWidth = 1;
  for (let i = 0; i < lineCount; i++) {
    const angle = rotation + (i / lineCount) * Math.PI * 2;
    const r0 = radius * 0.3;
    const r1 = radius;
    ctx.beginPath();
    ctx.moveTo(cx + Math.cos(angle) * r0, cy + Math.sin(angle) * r0);
    ctx.lineTo(cx + Math.cos(angle) * r1, cy + Math.sin(angle) * r1);
    ctx.stroke();
  }
  ctx.restore();
}

// ---------------------------------------------------------------------------
// Success splash — full-screen celebration
// ---------------------------------------------------------------------------

export function drawSplash(
  ctx: CanvasRenderingContext2D,
  w: number,
  h: number,
  alpha: number,
  elapsed: number,
) {
  const a = Math.min(1, Math.max(0, alpha));
  const t = elapsed;
  const cx = w / 2;
  const cy = h / 2 - 30;

  // Dark backdrop
  ctx.fillStyle = `rgba(5, 2, 10, ${a * 0.7})`;
  ctx.fillRect(0, 0, w, h);

  // Expanding concentric rings
  for (let i = 0; i < 3; i++) {
    const ringT = (t * 0.4 + i * 0.33) % 1;
    const ringR = 40 + ringT * 300;
    const ringA = a * (1 - ringT) * 0.25;
    ctx.strokeStyle = i % 2 === 0 ? COLOURS.pink : COLOURS.cyan;
    ctx.globalAlpha = ringA;
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.arc(cx, cy, ringR, 0, Math.PI * 2);
    ctx.stroke();
  }
  ctx.globalAlpha = 1;

  // Starburst lines rotating slowly
  drawStarburst(ctx, cx, cy, 280, 16, COLOURS.pink, a * 0.12, t * 0.3);
  drawStarburst(ctx, cx, cy, 240, 12, COLOURS.cyan, a * 0.08, -t * 0.2);

  // Sparkles scattered across the screen
  ctx.save();
  for (let i = 0; i < 20; i++) {
    const sx = seededRand(i * 73 + 11) * w;
    const sy = seededRand(i * 97 + 23) * h;
    const sparkA = a * (0.2 + 0.6 * Math.abs(Math.sin(t * 3 + i * 1.7)));
    ctx.globalAlpha = sparkA;
    ctx.fillStyle =
      i % 3 === 0 ? COLOURS.yellow : i % 3 === 1 ? COLOURS.pink : COLOURS.cyan;
    const sz = i % 4 === 0 ? 3 : 2;
    ctx.fillRect(Math.floor(sx), Math.floor(sy), sz, sz);
  }
  ctx.restore();

  // Scanlines
  drawScanlines(ctx, w, h, a * 0.03);

  ctx.textAlign = "center";

  // Title with glow pulse
  const titlePulse = 1 + 0.03 * Math.sin(t * 5);
  ctx.save();
  ctx.translate(cx, cy);
  ctx.scale(titlePulse, titlePulse);
  withGlow(ctx, COLOURS.pink, 20 + 8 * Math.sin(t * 3), () => {
    ctx.font = "bold 42px monospace";
    ctx.fillStyle = `rgba(255, 0, 255, ${a})`;
    ctx.fillText("MISSION COMPLETE", 0, 0);
  });
  ctx.restore();

  // Subtitle fades in
  const subAlpha = Math.max(0, Math.min(1, (t - 0.3) * 2));
  withGlow(ctx, COLOURS.cyan, 8, () => {
    ctx.font = "16px monospace";
    ctx.fillStyle = `rgba(0, 255, 255, ${a * subAlpha * 0.9})`;
    ctx.fillText("you escaped the moon", cx, cy + 50);
  });

  // Prompt at bottom
  const promptAlpha = Math.max(0, Math.min(1, (t - 1) * 1.5));
  const blinkAlpha = 0.4 + 0.6 * Math.abs(Math.sin(t * 2.5));
  withGlow(ctx, COLOURS.pink, 8, () => {
    ctx.font = "14px monospace";
    ctx.fillStyle = `rgba(255, 0, 255, ${a * promptAlpha * blinkAlpha})`;
    ctx.fillText("PRESS SPACE TO PLAY AGAIN", cx, h - 80);
  });

  ctx.textAlign = "start";
}

// ---------------------------------------------------------------------------
// Crash splash — full-screen red-tinted with glitch
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
  const cx = w / 2;
  const cy = h / 2 - 30;

  // Red-tinted dark backdrop
  ctx.fillStyle = `rgba(20, 2, 2, ${a * 0.7})`;
  ctx.fillRect(0, 0, w, h);

  // VHS glitch: horizontal static lines flickering across the screen
  ctx.save();
  for (let i = 0; i < 12; i++) {
    const ly = (Math.sin(t * 13 + i * 2.7) * 0.5 + 0.5) * h;
    const lw = 50 + seededRand(i * 41 + Math.floor(t * 7)) * (w - 100);
    const lx = seededRand(i * 53 + Math.floor(t * 11)) * (w - lw);
    ctx.globalAlpha = a * (0.03 + 0.04 * Math.abs(Math.sin(t * 17 + i)));
    ctx.fillStyle =
      i % 3 === 0 ? "#ff3333" : i % 3 === 1 ? "#ff00ff" : "#ff6600";
    ctx.fillRect(Math.floor(lx), Math.floor(ly), Math.floor(lw), 1);
  }
  ctx.restore();

  // Red warning ring pulsing
  const ringPulse = 60 + 20 * Math.sin(t * 4);
  ctx.strokeStyle = "#ff3333";
  ctx.globalAlpha = a * (0.15 + 0.1 * Math.sin(t * 6));
  ctx.lineWidth = 3;
  ctx.beginPath();
  ctx.arc(cx, cy, ringPulse, 0, Math.PI * 2);
  ctx.stroke();
  ctx.globalAlpha = 1;

  // Scanlines
  drawScanlines(ctx, w, h, a * 0.04);

  ctx.textAlign = "center";

  // Title with screen shake on entry
  const shakeFade = Math.max(0, 1 - t * 2);
  const shakeX =
    shakeFade > 0
      ? (seededRand(Math.floor(t * 60) * 7) - 0.5) * 8 * shakeFade
      : 0;
  const shakeY =
    shakeFade > 0
      ? (seededRand(Math.floor(t * 60) * 13) - 0.5) * 8 * shakeFade
      : 0;
  const titleScale = Math.min(1, t * 3);
  ctx.save();
  ctx.translate(cx + shakeX, cy + shakeY);
  ctx.scale(titleScale, titleScale);
  withGlow(ctx, "#ff3333", 16 + 8 * Math.sin(t * 5), () => {
    ctx.font = "bold 42px monospace";
    ctx.fillStyle = `rgba(255, 50, 50, ${a})`;
    ctx.fillText("CRASH LANDING", 0, 0);
  });
  ctx.restore();

  // Subtitle
  const subAlpha = Math.max(0, Math.min(1, (t - 0.4) * 2));
  withGlow(ctx, "#ff6644", 6, () => {
    ctx.font = "16px monospace";
    ctx.fillStyle = `rgba(255, 100, 100, ${a * subAlpha * 0.85})`;
    ctx.fillText("the lander couldn't take it", cx, cy + 50);
  });

  // Debris pixels drifting downward
  ctx.save();
  for (let i = 0; i < 10; i++) {
    const baseX = seededRand(i * 71 + 3) * w;
    const baseY = seededRand(i * 83 + 7) * h;
    const driftY = (baseY + t * (30 + i * 10)) % h;
    ctx.globalAlpha = a * 0.4;
    ctx.fillStyle =
      i % 3 === 0 ? "#ff3333" : i % 3 === 1 ? "#ff8844" : "#ffaa00";
    ctx.fillRect(Math.floor(baseX), Math.floor(driftY), 2, 2);
  }
  ctx.restore();

  // Prompt
  const promptAlpha = Math.max(0, Math.min(1, (t - 0.8) * 1.5));
  const blinkAlpha = 0.4 + 0.6 * Math.abs(Math.sin(t * 2.5));
  withGlow(ctx, "#ff3333", 8, () => {
    ctx.font = "14px monospace";
    ctx.fillStyle = `rgba(255, 80, 80, ${a * promptAlpha * blinkAlpha})`;
    ctx.fillText("PRESS SPACE TO TRY AGAIN", cx, h - 80);
  });

  ctx.textAlign = "start";
}

// ---------------------------------------------------------------------------
// Start screen — full-screen title overlay
// ---------------------------------------------------------------------------

export function drawStartScreen(
  ctx: CanvasRenderingContext2D,
  w: number,
  h: number,
  elapsed: number,
) {
  const t = elapsed;
  const cx = w / 2;
  const cy = h / 2 - 50;

  // Dim overlay
  ctx.fillStyle = "rgba(5, 2, 10, 0.6)";
  ctx.fillRect(0, 0, w, h);

  // Slow-rotating starburst behind the title
  drawStarburst(ctx, cx, cy, 350, 24, COLOURS.pink, 0.08, t * 0.15);
  drawStarburst(ctx, cx, cy, 300, 18, COLOURS.cyan, 0.05, -t * 0.1);

  // Pulsing ring
  const ringR = 80 + 15 * Math.sin(t * 2);
  ctx.strokeStyle = COLOURS.pink;
  ctx.globalAlpha = 0.15 + 0.08 * Math.sin(t * 3);
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.arc(cx, cy, ringR, 0, Math.PI * 2);
  ctx.stroke();
  ctx.globalAlpha = 1;

  // Second pulsing ring (offset phase)
  const ringR2 = 120 + 20 * Math.sin(t * 1.7 + 1);
  ctx.strokeStyle = COLOURS.cyan;
  ctx.globalAlpha = 0.08 + 0.05 * Math.sin(t * 2.3 + 1);
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.arc(cx, cy, ringR2, 0, Math.PI * 2);
  ctx.stroke();
  ctx.globalAlpha = 1;

  // Scanlines
  drawScanlines(ctx, w, h, 0.025);

  ctx.textAlign = "center";

  // Title with glow pulse
  const titlePulse = 1 + 0.02 * Math.sin(t * 4);
  ctx.save();
  ctx.translate(cx, cy);
  ctx.scale(titlePulse, titlePulse);
  withGlow(ctx, COLOURS.pink, 18 + 6 * Math.sin(t * 2.5), () => {
    ctx.font = "bold 48px monospace";
    ctx.fillStyle = COLOURS.pink;
    ctx.fillText("MOON LANDER", 0, 0);
  });
  ctx.restore();

  // How-to-play lines
  ctx.font = "14px monospace";
  withGlow(ctx, COLOURS.cyan, 4, () => {
    ctx.fillStyle = "rgba(0, 255, 255, 0.85)";
    ctx.fillText("arrow keys / WASD \u2014 thrust", cx, cy + 60);
    ctx.fillText(
      "land gently \u2014 collect fuel \u2014 launch home",
      cx,
      cy + 84,
    );
  });

  // Decorative dot row
  ctx.save();
  for (let i = 0; i < 7; i++) {
    const dx = cx - 60 + i * 20;
    const dy = cy + 115;
    ctx.globalAlpha = 0.25 + 0.35 * Math.abs(Math.sin(t * 2 + i * 0.9));
    ctx.fillStyle = i % 2 === 0 ? COLOURS.pink : COLOURS.cyan;
    ctx.fillRect(Math.floor(dx), dy, 2, 2);
  }
  ctx.restore();

  // Blinking prompt at bottom
  const blinkAlpha = 0.4 + 0.6 * Math.abs(Math.sin(t * 2.5));
  withGlow(ctx, COLOURS.pink, 10, () => {
    ctx.font = "16px monospace";
    ctx.fillStyle = `rgba(255, 0, 255, ${blinkAlpha})`;
    ctx.fillText("PRESS SPACE TO START", cx, h - 80);
  });

  ctx.textAlign = "start";
}

// ---------------------------------------------------------------------------
// Velocity warning — shown during descent when approaching crash thresholds
// ---------------------------------------------------------------------------

const CRASH_VEL_Y = 50;
const CRASH_VEL_X = 30;

function velColour(ratio: number): string {
  if (ratio < 0.5) return COLOURS.green;
  if (ratio < 0.8) return COLOURS.orange;
  return "#ff3333";
}

function drawGauge(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  barX: number,
  label: string,
  value: number,
  maxValue: number,
) {
  const ratio = Math.abs(value) / maxValue;
  const col = velColour(ratio);
  const glow = ratio > 0.8 ? 10 : ratio > 0.5 ? 4 : 0;
  const BAR_W = 80;
  const BAR_H = 10;

  withGlow(ctx, col, glow, () => {
    ctx.fillStyle = col;
    ctx.fillText(label + " " + Math.round(Math.abs(value)), x, y);
  });

  ctx.fillStyle = "rgba(255, 255, 255, 0.1)";
  ctx.fillRect(barX, y - BAR_H, BAR_W, BAR_H);
  const filled = Math.min(1, ratio);
  withGlow(ctx, col, glow, () => {
    ctx.fillStyle = col;
    ctx.fillRect(barX, y - BAR_H, Math.floor(BAR_W * filled), BAR_H);
  });

  ctx.fillStyle = "rgba(255, 255, 255, 0.3)";
  ctx.fillRect(barX + BAR_W, y - BAR_H - 2, 1, BAR_H + 4);
}

export function drawVelocityWarning(
  ctx: CanvasRenderingContext2D,
  w: number,
  h: number,
  velX: number,
  velY: number,
  now: number,
) {
  const x = 14;
  const baseY = h - 70;
  const ROW_H = 22;
  const barX = x + 68;

  ctx.textAlign = "start";
  ctx.font = "bold 13px monospace";

  drawGauge(ctx, x, baseY, barX, "SPD", velY, CRASH_VEL_Y);
  drawGauge(ctx, x, baseY + ROW_H, barX, "DFT", velX, CRASH_VEL_X);

  // Flashing OVERSPEED when either axis is in the danger zone
  const ratioY = Math.abs(velY) / CRASH_VEL_Y;
  const ratioX = Math.abs(velX) / CRASH_VEL_X;
  if (ratioY >= 0.8 || ratioX >= 0.8) {
    const flash = Math.abs(Math.sin(now * 6));
    const warnY = baseY + ROW_H * 2 + 6;
    withGlow(ctx, "#ff3333", 14, () => {
      ctx.font = "bold 16px monospace";
      ctx.fillStyle = `rgba(255, 50, 50, ${flash})`;
      ctx.fillText("OVERSPEED", x, warnY);
    });
  }
}
