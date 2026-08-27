"use client";

import { useAll, useSession } from "jazz-tools/react";
import { useState } from "react";
import { app } from "@/schema";
import { AssetShelf } from "@/src/components/AssetShelf";
import { CanvasSurface } from "@/src/components/CanvasSurface";
import { CheckpointShelf } from "@/src/components/CheckpointShelf";
import { CollaboratorCursors } from "@/src/components/CollaboratorCursors";
import { LayerPanel } from "@/src/components/LayerPanel";
import { roleForActiveCanvas } from "@/src/lib/identity";

export function PosterShopApp() {
  return <PosterStudio />;
}

/** The shell only reads canvas metadata. Child surfaces keep independent Jazz
 * subscriptions, so a cursor or asset update cannot invalidate the canvas UI. */
export function PosterStudio() {
  const session = useSession();
  const { data: canvases = [] } = useAll(app.canvases);
  const [activeId, setActiveId] = useState<string | null>(null);
  const active = canvases.find((canvas) => canvas.id === activeId) ?? canvases[0];
  const author = session?.user ?? null;
  const { data: memberships = [] } = useAll(app.canvasMembers);
  const role = roleForActiveCanvas(memberships, active?.id, author);
  const canEdit = role === "editor" || role === "admin";
  const canAdmin = role === "admin";
  if (!active)
    return (
      <main>
        <h1>PosterShop</h1>
        <p>Preparing your issuer-scoped poster studio…</p>
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
        <LayerPanel canvasId={active.id} canEdit={canEdit} />
        <CanvasSurface canvasId={active.id} canEdit={canEdit} />
        <aside>
          <CollaboratorCursors canvasId={active.id} author={author} />
          <AssetShelf canvasId={active.id} />
          <CheckpointShelf canvasId={active.id} canAdmin={canAdmin} />
        </aside>
      </section>
    </main>
  );
}
