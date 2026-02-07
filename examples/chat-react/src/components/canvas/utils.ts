import type { Point, Stroke } from "@/schema";

export const INTERNAL_WIDTH = 1000;
export const INTERNAL_HEIGHT = 750;
export const STROKE_WIDTH = 6;
export const ERASER_SCALE = 2.5;
export const ERASER_RADIUS = STROKE_WIDTH * ERASER_SCALE;
export const ERASER_WIDTH = ERASER_RADIUS * 2;

export function renderCanvasFrame(
  canvas: HTMLCanvasElement,
  strokes: Stroke[],
  options: {
    inProgressStroke?: Stroke | null;
    erasePoint?: Point | null;
  },
) {
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  ctx.save();
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.restore();

  ctx.save();

  const scaleX = canvas.width / INTERNAL_WIDTH;
  const scaleY = canvas.height / INTERNAL_HEIGHT;

  ctx.scale(scaleX, scaleY);

  for (const stroke of strokes) {
    drawStroke(ctx, stroke);
  }

  if (options.inProgressStroke?.points.length) {
    drawStroke(ctx, options.inProgressStroke);
  }

  if (options.erasePoint) {
    drawErasePreview(ctx, options.erasePoint, ERASER_RADIUS);
  }

  ctx.restore();
}

export function getLogicalPoint(
  event: React.PointerEvent,
  canvas: HTMLCanvasElement,
): Point {
  const rect = canvas.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;

  return {
    x: (x / rect.width) * INTERNAL_WIDTH,
    y: (y / rect.height) * INTERNAL_HEIGHT,
  };
}

export function colorFromAccountId(accountId: string) {
  let hash = 2166136261;
  for (let index = 0; index < accountId.length; index += 1) {
    hash ^= accountId.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  hash >>>= 0;

  const hue = hash % 360;
  const saturation = 60 + ((hash >>> 8) % 40); // 60% - 100%
  const lightness = (hash >>> 16) % 80; // 0% - 80%

  return `hsl(${hue} ${saturation}% ${lightness}%)`;
}

export function drawStroke(ctx: CanvasRenderingContext2D, stroke: Stroke) {
  if (stroke.points.length === 0) return;
  const [first, ...rest] = stroke.points;

  ctx.strokeStyle = stroke.color;
  ctx.lineWidth = stroke.width;
  ctx.lineJoin = "round";
  ctx.lineCap = "round";

  ctx.beginPath();
  ctx.moveTo(first.x, first.y);

  if (rest.length === 0) {
    ctx.lineTo(first.x + 0.01, first.y + 0.01);
  } else {
    for (const point of rest) {
      ctx.lineTo(point.x, point.y);
    }
  }

  ctx.stroke();
}

export function drawErasePreview(
  ctx: CanvasRenderingContext2D,
  point: Point,
  radius: number,
) {
  ctx.save();
  ctx.strokeStyle = "rgba(0, 0, 0, 0.4)";
  ctx.lineWidth = 1;
  ctx.setLineDash([6, 6]);
  ctx.beginPath();
  ctx.arc(point.x, point.y, radius, 0, Math.PI * 2);
  ctx.stroke();
  ctx.restore();
}

export const debounce = <Args extends unknown[]>(
  callback: (...args: Args) => void,
  wait: number,
) => {
  let timeoutId: number | null = null;
  return (...args: Args) => {
    if (timeoutId) {
      window.clearTimeout(timeoutId);
    }
    timeoutId = window.setTimeout(() => {
      callback(...args);
    }, wait);
  };
};
