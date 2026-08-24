"use client";
import { useAll, useDb } from "jazz-tools/react";
import { app } from "../../schema.js";
export function LayerPanel({ canvasId }: { canvasId: string }) {
  const db = useDb();
  const { data: layers = [] } = useAll(app.layers.where({ canvasId }).orderBy("zIndex", "asc"));
  return (
    <section aria-label="Layers">
      <h2>Layers</h2>
      <button
        onClick={() =>
          db.insert(app.layers, {
            canvasId,
            name: `Layer ${layers.length + 1}`,
            zIndex: layers.length,
            visible: true,
          })
        }
      >
        Add layer
      </button>
      <ol>
        {layers.map((layer) => (
          <li key={layer.id}>
            <label>
              <input
                type="checkbox"
                checked={layer.visible}
                onChange={() => db.update(app.layers, layer.id, { visible: !layer.visible })}
              />
              {layer.name}
            </label>
          </li>
        ))}
      </ol>
    </section>
  );
}
