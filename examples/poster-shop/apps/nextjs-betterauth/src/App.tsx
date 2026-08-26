"use client";

import { useAll, useDb, useSession } from "jazz-tools/react";
import { useState } from "react";
import { app } from "@/schema";
import { AssetShelf } from "@/src/components/AssetShelf";
import { CanvasSurface } from "@/src/components/CanvasSurface";
import { CheckpointShelf } from "@/src/components/CheckpointShelf";
import { CollaboratorCursors } from "@/src/components/CollaboratorCursors";
import { LayerPanel } from "@/src/components/LayerPanel";

export function PosterShopApp() {
  return <PosterStudio />;
}

/** The shell only reads canvas metadata. Child surfaces keep independent Jazz
 * subscriptions, so a cursor or asset update cannot invalidate the canvas UI. */
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
        <p>A local-first studio for a shared gig-poster canvas.</p>
        <button onClick={create}>Create poster</button>
      </main>
    );
  return (
    <main>
      <header>
        <h1>{active.title}</h1>
        <label>
          Poster{" "}
          <select value={active.id} onChange={(event) => setActiveId(event.target.value)}>
            {canvases.map((canvas) => (
              <option key={canvas.id} value={canvas.id}>
                {canvas.title}
              </option>
            ))}
          </select>
        </label>
      </header>
      <section aria-label="Poster editor">
        <LayerPanel canvasId={active.id} />
        <CanvasSurface canvasId={active.id} />
        <aside>
          <CollaboratorCursors canvasId={active.id} userId={session?.user_id ?? "local"} />
          <AssetShelf canvasId={active.id} />
          <CheckpointShelf canvasId={active.id} />
        </aside>
      </section>
    </main>
  );
}
