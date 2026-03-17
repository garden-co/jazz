import { type PointerEvent, useEffect, useMemo, useRef, useState } from "react";
import { useDb, useAll, useSession } from "jazz-tools/react";
import { Eraser, Pencil } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Toggle } from "@/components/ui/toggle";
import { app } from "../../../schema/app.js";
import { CollaboratorBadge } from "./CollaboratorBadge";
import { useResponsiveCanvas } from "./useResponsiveCanvas";
import {
  colorFromUserId,
  type Point,
  renderCanvasFrame,
  type StrokeData,
  STROKE_WIDTH,
  ERASER_WIDTH,
  getLogicalPoint,
} from "./utils";

export function CollaborativeCanvas({
  canvasId,
  showControls = true,
  className = "",
}: {
  canvasId: string;
  height?: string;
  showControls?: boolean;
  className?: string;
}) {
  const db = useDb();
  const session = useSession();
  const userId = session?.user_id ?? null;
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRefs = useRef<Map<string, HTMLCanvasElement | null>>(new Map());
  const drawingRef = useRef(false);
  const currentStrokeRef = useRef<StrokeData | null>(null);
  const erasePointRef = useRef<Point | null>(null);
  const [mode, setMode] = useState<"draw" | "erase">("draw");

  const myColor = userId ? colorFromUserId(userId) : "#000000";

  // Subscribe to the canvas row so it's present in the local runtime for FK checks
  const canvasRows = useAll(app.canvases.where({ id: canvasId })) ?? [];
  const canvasReady = canvasRows.length > 0;

  const profiles = useAll(app.profiles) ?? [];
  const profileNameByUserId = useMemo(() => {
    const map = new Map<string, string>();
    const sorted = [...profiles].sort((a, b) => a.id.localeCompare(b.id));
    for (const p of sorted) {
      if (!map.has(p.userId)) {
        map.set(p.userId, p.name);
      }
    }
    return map;
  }, [profiles]);

  // Fetch all strokes for this canvas
  const allStrokes = useAll(app.strokes.where({ canvasId })) ?? [];

  // Group strokes by ownerId
  const strokesByOwner: Record<string, StrokeData[]> = {};
  for (const s of allStrokes) {
    if (!strokesByOwner[s.ownerId]) strokesByOwner[s.ownerId] = [];
    const points: Point[] = JSON.parse(s.pointsJson);
    strokesByOwner[s.ownerId].push({
      id: s.id,
      points,
      color: s.color,
      width: s.width,
      createdAt: s.createdAt,
    });
  }

  if (userId && !strokesByOwner[userId]) {
    strokesByOwner[userId] = [];
  }

  const ownerIds = Object.keys(strokesByOwner).sort();

  const renderAll = () => {
    for (const ownerId of ownerIds) {
      const cvs = canvasRefs.current.get(ownerId);
      if (!cvs) continue;

      const isMe = ownerId === userId;
      const strokes = strokesByOwner[ownerId] ?? [];

      renderCanvasFrame(cvs, strokes, {
        inProgressStroke: isMe ? currentStrokeRef.current : null,
        erasePoint: isMe ? erasePointRef.current : null,
      });
    }
  };

  useResponsiveCanvas(containerRef, canvasRefs, renderAll);

  // Intentionally no dependency array: canvas must re-render whenever React
  // re-renders (stroke data, in-progress drawing, erase cursor all change).
  useEffect(() => {
    renderAll();
  });

  const handlePointerDown = (event: PointerEvent<HTMLCanvasElement>) => {
    if (!userId || !canvasReady || (event.button !== 0 && event.pointerType !== "touch")) return;

    event.preventDefault();
    try {
      event.currentTarget.setPointerCapture(event.pointerId);
    } catch {
      // Pointer capture can fail when the pointer has already been released
      // (e.g. synthetic events in tests, or rapid touch interactions).
    }
    drawingRef.current = true;

    const point = getLogicalPoint(event, event.currentTarget);

    const newStroke: StrokeData = {
      id: crypto.randomUUID(),
      points: [point],
      color: mode === "draw" ? myColor : "#ffffff",
      width: mode === "draw" ? STROKE_WIDTH : ERASER_WIDTH,
      createdAt: new Date(),
    };

    currentStrokeRef.current = newStroke;
    if (mode === "erase") erasePointRef.current = point;

    renderAll();
  };

  const handlePointerMove = (event: PointerEvent<HTMLCanvasElement>) => {
    if (!drawingRef.current || !currentStrokeRef.current) return;
    event.preventDefault();

    const point = getLogicalPoint(event, event.currentTarget);
    const pts = currentStrokeRef.current.points;
    const last = pts[pts.length - 1];
    const dx = point.x - last.x;
    const dy = point.y - last.y;
    if (dx * dx + dy * dy < 9) return; // min 3 logical px between samples

    pts.push(point);

    if (mode === "erase") erasePointRef.current = point;
    renderAll();
  };

  const handlePointerUp = (event: PointerEvent<HTMLCanvasElement>) => {
    if (!drawingRef.current) return;

    drawingRef.current = false;
    try {
      event.currentTarget.releasePointerCapture(event.pointerId);
    } catch {
      // See setPointerCapture catch above.
    }

    // Save the finished stroke
    if (currentStrokeRef.current && userId) {
      const stroke = currentStrokeRef.current;
      db.insert(app.strokes, {
        canvasId,
        ownerId: userId,
        color: stroke.color,
        width: stroke.width,
        pointsJson: JSON.stringify(stroke.points),
        createdAt: stroke.createdAt,
      });
    }

    currentStrokeRef.current = null;
    erasePointRef.current = null;
    renderAll();
  };

  const handleClearMyStrokes = () => {
    if (!userId) return;
    const myStrokes = allStrokes.filter((s) => s.ownerId === userId);
    for (const s of myStrokes) {
      db.delete(app.strokes, s.id);
    }
  };

  return (
    <section
      className={`mt-1 bg-muted text-muted-foreground rounded-sm p-2 ${className}`}
      onPointerDown={(e) => e.stopPropagation()}
    >
      {showControls && (
        <header className="flex flex-wrap items-center justify-between gap-2 pb-2">
          <div className="flex items-center gap-1 rounded-md border bg-background p-1 shadow-sm">
            <Toggle
              pressed={mode === "draw"}
              onPressedChange={(p) => p && setMode("draw")}
              size="sm"
            >
              <Pencil /> Draw
            </Toggle>
            <Toggle
              pressed={mode === "erase"}
              onPressedChange={(p) => p && setMode("erase")}
              size="sm"
            >
              <Eraser /> Erase
            </Toggle>
          </div>
          <Button variant="outline" size="sm" onClick={handleClearMyStrokes}>
            Clear my strokes
          </Button>
        </header>
      )}

      <div
        ref={containerRef}
        className={`relative w-full aspect-4/3 overflow-hidden bg-white border border-dashed border-stone-300 rounded-md shadow-inner ${mode === "draw" ? "cursor-crosshair" : "cursor-auto"}`}
      >
        {ownerIds.map((ownerId) => {
          const isMe = ownerId === userId;
          return (
            <canvas
              key={ownerId}
              ref={(el) => {
                if (el) canvasRefs.current.set(ownerId, el);
                else canvasRefs.current.delete(ownerId);
              }}
              className={`absolute inset-0 w-full h-full mix-blend-multiply touch-none ${
                !isMe ? "pointer-events-none" : "z-10"
              }`}
              onPointerDown={isMe ? handlePointerDown : undefined}
              onPointerMove={isMe ? handlePointerMove : undefined}
              onPointerUp={isMe ? handlePointerUp : undefined}
              onPointerLeave={isMe ? handlePointerUp : undefined}
              data-testid="canvas"
            />
          );
        })}
      </div>

      <div className="mt-2 flex flex-wrap gap-2 text-sm">
        {ownerIds.map((ownerId) => (
          <CollaboratorBadge
            key={ownerId}
            name={profileNameByUserId.get(ownerId) ?? ownerId.slice(0, 8)}
            color={colorFromUserId(ownerId)}
          />
        ))}
      </div>
    </section>
  );
}
