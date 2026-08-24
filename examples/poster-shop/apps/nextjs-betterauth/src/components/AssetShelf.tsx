"use client";
import { useAll } from "jazz-tools/react";
import { app } from "../../schema.js";
/** Metadata-only: browsing a canvas must not hydrate a future blob/large value. */
export function AssetShelf({ canvasId }: { canvasId: string }) {
  const { data: assets = [] } = useAll(app.assets.where({ canvasId }).orderBy("name", "asc"));
  return (
    <section aria-label="Assets">
      <h2>Assets</h2>
      <ul>
        {assets.map((asset) => (
          <li key={asset.id}>
            {asset.name} ({asset.mimeType}, {asset.byteLength} bytes)
          </li>
        ))}
      </ul>
    </section>
  );
}
