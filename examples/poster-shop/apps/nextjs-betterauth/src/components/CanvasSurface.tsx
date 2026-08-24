"use client";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "../../schema.js";
/** Shape ordering is a normal indexed relation; equal z indexes deliberately
 * have no invented canvas-specific merge rule. */
export function CanvasSurface({ canvasId }: { canvasId: string }) {
  const db = useDb();
  const { data: layers = [] } = useAll(app.layers.where({ canvasId }).orderBy("zIndex", "asc"));
  const { data: shapes = [] } = useAll(app.shapes.where({ canvasId }).orderBy("zIndex", "asc"));
  const addShape = () => {
    const layer = layers.find((candidate) => candidate.visible);
    if (layer)
      db.insert(app.shapes, {
        canvasId,
        layerId: layer.id,
        kind: "rect",
        x: 80 + shapes.length * 12,
        y: 120 + shapes.length * 12,
        width: 360,
        height: 240,
        rotation: 0,
        zIndex: shapes.length,
        fill: "#ff5a36",
      });
  };
  return (
    <section aria-label="Canvas" data-shape-count={shapes.length}>
      <h2>Canvas</h2>
      <button onClick={addShape} disabled={!layers.some((layer) => layer.visible)}>
        Add shape
      </button>
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
