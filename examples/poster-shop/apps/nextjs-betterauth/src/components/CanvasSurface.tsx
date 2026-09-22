"use client";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "@/schema";
/** Shape ordering is a normal indexed relation; equal z indexes deliberately
 * have no invented canvas-specific merge rule. */
export function CanvasSurface({ canvasId, canEdit }: { canvasId: string; canEdit: boolean }) {
  const db = useDb();
  const { data: shapes = [] } = useAll(app.shapes.where({ canvasId }).orderBy("zIndex", "asc"));
  const { data: layers = [] } = useAll(app.layers.where({ canvasId }).orderBy("zIndex", "asc"));
  return (
    <section aria-label="Canvas" data-shape-count={shapes.length}>
      <h2>Canvas</h2>
      {canEdit && layers[0] ? (
        <button
          onClick={() =>
            db.insert(app.shapes, {
              canvasId,
              layerId: layers[0]!.id,
              kind: "rect",
              x: 40 + shapes.length * 12,
              y: 40 + shapes.length * 12,
              width: 180,
              height: 120,
              rotation: 0,
              zIndex: shapes.length,
              fill: "#e85d75",
            })
          }
        >
          Add rectangle
        </button>
      ) : (
        <p>{canEdit ? "Add a layer before creating shapes." : "Read-only canvas."}</p>
      )}
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
