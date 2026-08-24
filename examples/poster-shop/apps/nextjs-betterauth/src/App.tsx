"use client";

import { useState } from "react";
import { JazzProvider, useAll, useDb, useSession } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { app } from "../schema.js";

export function PosterShopApp({ config }: { config: DbConfig }) {
  return (
    <JazzProvider config={config} fallback={<p>Opening poster studio…</p>}>
      <PosterStudio />
    </JazzProvider>
  );
}

export function PosterStudio() {
  const db = useDb();
  const session = useSession();
  const { data: canvases = [] } = useAll(app.canvases);
  const [activeId, setActiveId] = useState<string | null>(null);
  const active = canvases.find((canvas) => canvas.id === activeId) ?? canvases[0];
  const create = () => {
    const canvas = db.insert(app.canvases, {
      title: "Midnight headline",
      width: 1080,
      height: 1350,
    }).value;
    db.insert(app.canvasMembers, {
      canvasId: canvas.id,
      userId: session?.user_id ?? "local",
      role: "admin",
    });
    db.insert(app.layers, { canvasId: canvas.id, name: "Artwork", zIndex: 0, visible: true });
    setActiveId(canvas.id);
  };
  if (!active)
    return (
      <main>
        <h1>PosterShop</h1>
        <button onClick={create}>Create poster</button>
      </main>
    );
  return <CanvasEditor canvasId={active.id} title={active.title} />;
}

function CanvasEditor({ canvasId, title }: { canvasId: string; title: string }) {
  const db = useDb();
  const { data: layers = [] } = useAll(app.layers.where({ canvasId }).orderBy("zIndex", "asc"));
  const { data: shapes = [] } = useAll(app.shapes.where({ canvasId }).orderBy("zIndex", "asc"));
  const addShape = () => {
    const layer = layers[0];
    if (!layer) return;
    db.insert(app.shapes, {
      canvasId,
      layerId: layer.id,
      kind: "rect",
      x: 80,
      y: 120,
      width: 360,
      height: 240,
      rotation: 0,
      zIndex: shapes.length,
      fill: "#ff5a36",
    });
  };
  return (
    <main>
      <h1>{title}</h1>
      <p>
        {shapes.length} shapes · {layers.length} layers
      </p>
      <button onClick={addShape}>Add shape</button>
      <ol aria-label="Poster shapes">
        {shapes.map((shape) => (
          <li key={shape.id}>
            {shape.kind} at {shape.x}, {shape.y}
          </li>
        ))}
      </ol>
    </main>
  );
}
