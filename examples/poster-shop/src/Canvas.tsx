import * as React from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "../schema/app.js";

type Point = { x: number; y: number };
const CANVAS_WIDTH = 900;
const CANVAS_HEIGHT = 520;

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function toCanvasPoint(canvas: HTMLCanvasElement, event: React.PointerEvent): Point {
  const rect = canvas.getBoundingClientRect();
  const scaleX = CANVAS_WIDTH / rect.width;
  const scaleY = CANVAS_HEIGHT / rect.height;
  return {
    x: clamp((event.clientX - rect.left) * scaleX, 0, CANVAS_WIDTH),
    y: clamp((event.clientY - rect.top) * scaleY, 0, CANVAS_HEIGHT),
  };
}

function colorForUser(userId: string): string {
  let hash = 2166136261;
  for (let index = 0; index < userId.length; index += 1) {
    hash ^= userId.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  hash >>>= 0;

  const hue = hash % 360;
  const saturation = 60 + ((hash >>> 8) % 40); // 60% - 100%
  const lightness = (hash >>> 16) % 80; // 0% - 80%

  return `hsl(${hue} ${saturation}% ${lightness}%)`;
}

function parsePoints(input: unknown): Point[] {
  if (!Array.isArray(input)) return [];
  const parsed: Point[] = [];
  for (const entry of input) {
    if (
      typeof entry === "object" &&
      entry !== null &&
      "x" in entry &&
      "y" in entry &&
      typeof entry.x === "number" &&
      typeof entry.y === "number"
    ) {
      parsed.push({ x: entry.x, y: entry.y });
    }
  }
  return parsed;
}

function drawStroke(
  ctx: CanvasRenderingContext2D,
  points: Point[],
  color: string,
  scaleX: number,
  scaleY: number,
) {
  if (points.length === 0) return;
  ctx.strokeStyle = color;
  ctx.lineWidth = 4;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  ctx.beginPath();
  ctx.moveTo(points[0].x * scaleX, points[0].y * scaleY);
  for (let i = 1; i < points.length; i++) {
    ctx.lineTo(points[i].x * scaleX, points[i].y * scaleY);
  }
  ctx.stroke();
}

export function Canvas() {
  const db = useDb();
  const session = useSession();
  const canvases = useAll(app.canvases.orderBy("created_at", "asc")) ?? [];
  const strokes = useAll(app.strokes.orderBy("created_at", "asc")) ?? [];
  const activeCanvas = canvases[0];
  const userId = session?.user_id ?? null;
  const userColor = userId ? colorForUser(userId) : "#333";
  const canvasRef = React.useRef<HTMLCanvasElement | null>(null);
  const [draftPoints, setDraftPoints] = React.useState<Point[]>([]);
  const creatingCanvasRef = React.useRef(false);

  React.useEffect(() => {
    if (activeCanvas || !userId || creatingCanvasRef.current) return;
    creatingCanvasRef.current = true;
    void db
      .insert(app.canvases, {
        name: "Main canvas",
        created_at: new Date().toISOString(),
      })
      .finally(() => {
        creatingCanvasRef.current = false;
      });
  }, [activeCanvas, db, userId]);

  React.useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const width = canvas.clientWidth || CANVAS_WIDTH;
    const height = canvas.clientHeight || CANVAS_HEIGHT;
    if (canvas.width !== width || canvas.height !== height) {
      canvas.width = width;
      canvas.height = height;
    }

    const scaleX = canvas.width / CANVAS_WIDTH;
    const scaleY = canvas.height / CANVAS_HEIGHT;
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    for (const stroke of strokes) {
      const points = parsePoints(stroke.points);
      drawStroke(ctx, points, colorForUser(stroke.user_id), scaleX, scaleY);
    }

    if (draftPoints.length > 0) {
      drawStroke(ctx, draftPoints, userColor, scaleX, scaleY);
    }
  }, [draftPoints, strokes, userColor]);

  const onPointerDown = (event: React.PointerEvent<HTMLCanvasElement>) => {
    if (!userId || !activeCanvas) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    canvas.setPointerCapture(event.pointerId);
    setDraftPoints([toCanvasPoint(canvas, event)]);
  };

  const onPointerMove = (event: React.PointerEvent<HTMLCanvasElement>) => {
    if (draftPoints.length === 0) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const next = toCanvasPoint(canvas, event);
    setDraftPoints((prev) => [...prev, next]);
  };

  const endStroke = (pointerId: number | null) => {
    const points = draftPoints;
    if (pointerId !== null) {
      const canvas = canvasRef.current;
      if (canvas?.hasPointerCapture(pointerId)) {
        canvas.releasePointerCapture(pointerId);
      }
    }
    setDraftPoints([]);
    if (!activeCanvas || !userId || points.length < 2) return;
    db.insert(app.strokes, {
      canvas_id: activeCanvas.id,
      user_id: userId,
      points,
      created_at: new Date().toISOString(),
    });
  };

  return (
    <section>
      <p>Draw freehand strokes. Each user gets a color.</p>
      <canvas
        ref={canvasRef}
        width={CANVAS_WIDTH}
        height={CANVAS_HEIGHT}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={(event) => endStroke(event.pointerId)}
        onPointerCancel={(event) => endStroke(event.pointerId)}
        onPointerLeave={() => endStroke(null)}
        style={{
          width: "100%",
          maxWidth: 900,
          aspectRatio: `${CANVAS_WIDTH} / ${CANVAS_HEIGHT}`,
          border: "1px solid #999",
          borderRadius: 8,
          touchAction: "none",
          background: "#fff",
          display: "block",
        }}
      />
      <p>
        You are: <code>{userId ?? "not signed in"}</code> ({userColor})
      </p>
    </section>
  );
}
