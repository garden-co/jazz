import { type PointerEvent, useEffect, useRef, useState } from "react";
import { useAccount, useSuspenseCoState } from "jazz-tools/react";
import { Eraser, Pencil } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Toggle } from "@/components/ui/toggle";
import { Canvas, ChatAccount, type Point, type Stroke } from "@/schema";
import { CollaboratorBadge } from "./CollaboratorBadge";
import { useResponsiveCanvas } from "./useResponsiveCanvas";
import {
  colorFromAccountId,
  debounce,
  ERASER_WIDTH,
  getLogicalPoint,
  renderCanvasFrame,
  STROKE_WIDTH,
} from "./utils";

const DRAW_BROADCAST_DEBOUNCE_MS = 16;

export function CollaborativeCanvas({
  canvasId,
  height = "",
  showControls = true,
  className = "",
}: {
  canvasId: string;
  height?: string;
  showControls?: boolean;
  className?: string;
}) {
  const me = useAccount(ChatAccount, { resolve: { profile: true } });
  const canvas = useSuspenseCoState(Canvas, canvasId);
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRefs = useRef<Map<string, HTMLCanvasElement | null>>(new Map());
  const drawingRef = useRef(false);
  const currentStrokeRef = useRef<Stroke | null>(null);
  const erasePointRef = useRef<Point | null>(null);
  const [mode, setMode] = useState<"draw" | "erase">("draw");

  const myAccountId = me.$jazz.id;
  const myColor = myAccountId ? colorFromAccountId(myAccountId) : "#000000";

  const strokesByAccount: Record<string, Stroke[]> = {};
  if (canvas) {
    for (const [accountId, strokes] of Object.entries(canvas)) {
      strokesByAccount[accountId] = strokes;
    }
  }
  if (myAccountId && !strokesByAccount[myAccountId]) {
    strokesByAccount[myAccountId] = [];
  }

  const accountIds = Object.keys(strokesByAccount).sort();

  const renderAll = () => {
    for (const accountId of accountIds) {
      const cvs = canvasRefs.current.get(accountId);
      if (!cvs) continue;

      const isMe = accountId === myAccountId;
      const strokes = strokesByAccount[accountId] ?? [];

      renderCanvasFrame(cvs, strokes, {
        inProgressStroke: isMe ? currentStrokeRef.current : null,
        erasePoint: isMe ? erasePointRef.current : null,
      });
    }
  };

  useResponsiveCanvas(containerRef, canvasRefs, renderAll);

  useEffect(() => {
    renderAll();
  });

  const debouncedUpdater = useRef(
    debounce((accountId: string, newStrokes: Stroke[]) => {
      canvas?.$jazz.set(accountId, newStrokes);
    }, DRAW_BROADCAST_DEBOUNCE_MS),
  ).current;

  const handlePointerDown = (event: PointerEvent<HTMLCanvasElement>) => {
    if (!myAccountId || (event.button !== 0 && event.pointerType !== "touch"))
      return;

    event.preventDefault();
    event.currentTarget.setPointerCapture(event.pointerId);
    drawingRef.current = true;

    const point = getLogicalPoint(event, event.currentTarget);

    const newStroke: Stroke = {
      id: crypto.randomUUID(),
      points: [point],
      color: mode === "draw" ? myColor : "#ffffff", // "White-out" eraser
      width: mode === "draw" ? STROKE_WIDTH : ERASER_WIDTH,
      createdAt: Date.now(),
    };

    currentStrokeRef.current = newStroke;
    if (mode === "erase") erasePointRef.current = point;

    renderAll();
  };

  const handlePointerMove = (event: PointerEvent<HTMLCanvasElement>) => {
    if (!drawingRef.current || !currentStrokeRef.current) return;
    event.preventDefault();

    const point = getLogicalPoint(event, event.currentTarget);
    currentStrokeRef.current.points.push(point);

    if (mode === "erase") erasePointRef.current = point;
    renderAll();

    if (myAccountId && canvas) {
      const myStrokes = strokesByAccount[myAccountId] || [];
      debouncedUpdater(myAccountId, [...myStrokes, currentStrokeRef.current]);
    }
  };

  const handlePointerUp = (event: PointerEvent<HTMLCanvasElement>) => {
    if (!drawingRef.current) return;

    drawingRef.current = false;
    event.currentTarget.releasePointerCapture(event.pointerId);

    // Final Save
    if (currentStrokeRef.current && myAccountId && canvas) {
      const myStrokes = strokesByAccount[myAccountId] || [];
      canvas.$jazz.set(myAccountId, [...myStrokes, currentStrokeRef.current]);
    }

    currentStrokeRef.current = null;
    erasePointRef.current = null;
    renderAll();
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
          <Button
            variant="outline"
            size="sm"
            onClick={() => myAccountId && canvas?.$jazz.set(myAccountId, [])}
          >
            Clear my strokes
          </Button>
        </header>
      )}

      <div
        ref={containerRef}
        className={`relative w-full aspect-4/3 ${height} overflow-hidden bg-white border border-dashed border-stone-300 rounded-md shadow-inner ${mode === "draw" ? "cursor-crosshair" : "cursor-auto"}`}
      >
        {accountIds.map((accountId) => {
          const isMe = accountId === myAccountId;
          return (
            <canvas
              key={accountId}
              ref={(el) => {
                if (el) canvasRefs.current.set(accountId, el);
                else canvasRefs.current.delete(accountId);
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
        {accountIds.map((accountId) => (
          <CollaboratorBadge
            key={accountId}
            accountId={accountId}
            color={colorFromAccountId(accountId)}
          />
        ))}
      </div>
    </section>
  );
}
