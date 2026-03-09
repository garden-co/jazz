import { useCallback, useEffect, useRef, useState } from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { app, Canvas as CanvasModel, Stroke } from "../schema/app.js";
import { getRandomName } from "./profileName.js";
import { debounce } from "./utils.js";

type Point = { x: number; y: number };
const CANVAS_WIDTH = 900;
const CANVAS_HEIGHT = 520;

const DRAW_BROADCAST_DEBOUNCE_MS = 16;

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

function CollaboratorBadge({ userId, isMe }: { userId: string; isMe: boolean }) {
  const user = useAll(app.users.where({ user_id: { eq: userId } }))?.[0];
  if (!user) return null;
  const color = colorForUser(userId);
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
      <span
        style={{
          display: "inline-block",
          width: 12,
          height: 12,
          borderRadius: "9999px",
          border: "1px solid #e7e5e4",
          backgroundColor: color,
        }}
      />
      <span style={{ fontWeight: isMe ? 700 : 400 }}>{user.name}</span>
    </span>
  );
}

function useGetOrCreateCanvas(): CanvasModel | undefined {
  const db = useDb();
  const canvases = useAll(app.canvases.orderBy("created_at", "asc"));
  const activeCanvas = canvases?.[0];
  useEffect(() => {
    if (!canvases || activeCanvas) return;
    db.insert(app.canvases, {
      name: "Main canvas",
      created_at: new Date().toISOString(),
    });
  }, [db, canvases]);
  return activeCanvas;
}

function useGetOrCreateUser(): string | null {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;
  // TODO avoid fetching all users! -> add filter + `enabled`
  const users = useAll(app.users);
  useEffect(() => {
    if (!userId || !users) return;
    const userExists = users.some((user) => user.user_id === userId);
    if (userExists) {
      return;
    }
    db.insert(app.users, {
      user_id: userId,
      name: getRandomName(),
      created_at: new Date().toISOString(),
    });
  }, [db, userId, users]);
  return userId;
}

export function Canvas() {
  const db = useDb();
  const activeCanvas = useGetOrCreateCanvas();
  // TODO return only strokes for the active canvas
  const strokes = useAll(app.strokes.orderBy("created_at", "asc")) ?? [];
  const userId = useGetOrCreateUser();
  const userColor = userId ? colorForUser(userId) : "#333";
  const collaboratorIds = [...new Set(strokes.map((stroke) => stroke.user_id))];
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [inProgressStroke, setInProgressStroke] = useState<Stroke | null>(null);

  useEffect(() => {
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
      drawStroke(ctx, stroke.points, colorForUser(stroke.user_id), scaleX, scaleY);
    }

    if (inProgressStroke) {
      drawStroke(ctx, inProgressStroke.points, userColor, scaleX, scaleY);
    }
  }, [inProgressStroke, strokes, userColor]);

  const onPointerDown = (event: React.PointerEvent<HTMLCanvasElement>) => {
    if (!userId || !activeCanvas) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    canvas.setPointerCapture(event.pointerId);
    const initialPoint = toCanvasPoint(canvas, event);

    const newStroke = db.insert(app.strokes, {
      canvas_id: activeCanvas.id,
      user_id: userId,
      points: [initialPoint],
      created_at: new Date().toISOString(),
    });
    setInProgressStroke(newStroke);
  };

  // Update strokes 60 times per second at most (may be less during rapid mouse movement)
  const debouncedUpdateStroke = useCallback(
    debounce((stroke: Stroke, newPoint: Point) => {
      db.update(app.strokes, stroke.id, {
        points: [...stroke.points, newPoint],
      });
    }, DRAW_BROADCAST_DEBOUNCE_MS),
    [db],
  );

  const onPointerMove = (event: React.PointerEvent<HTMLCanvasElement>) => {
    if (!inProgressStroke) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const newPoint = toCanvasPoint(canvas, event);
    setInProgressStroke((prev) => {
      if (!prev) return null;
      const points = [...prev.points, newPoint];
      return { ...inProgressStroke, points };
    });
    debouncedUpdateStroke(inProgressStroke, newPoint);
  };

  const endStroke = (pointerId: number | null) => {
    if (!inProgressStroke) return;
    if (pointerId !== null) {
      const canvas = canvasRef.current;
      if (canvas?.hasPointerCapture(pointerId)) {
        canvas.releasePointerCapture(pointerId);
      }
    }
    setInProgressStroke(null);
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
      {collaboratorIds.length > 0 && (
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 12,
            marginTop: 12,
            fontSize: 14,
            color: "#57534e",
          }}
        >
          {collaboratorIds.map((id) => (
            <CollaboratorBadge key={id} userId={id} isMe={id === userId} />
          ))}
        </div>
      )}
    </section>
  );
}
