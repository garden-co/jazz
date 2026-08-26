"use client";
import { useAll } from "jazz-tools/react";
import { app } from "@/schema";
/** Shape ordering is a normal indexed relation; equal z indexes deliberately
 * have no invented canvas-specific merge rule. */
export function CanvasSurface({ canvasId }: { canvasId: string }) {
  const { data: shapes = [] } = useAll(app.shapes.where({ canvasId }).orderBy("zIndex", "asc"));
  return (
    <section aria-label="Canvas" data-shape-count={shapes.length}>
      <h2>Canvas</h2>
      <p>Shape creation is temporarily unavailable while cross-canvas admission is hardened.</p>
      <ol aria-label="Poster shapes">
        {shapes.map((shape) => (
          <li key={shape.id}>
            {shape.kind} at {shape.x}, {shape.y} · layer {shape.layerId}
          </li>
        ))}
      </ol>
    </section>
  );
}
