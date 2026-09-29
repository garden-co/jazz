"use client";

import { useAll, useDb } from "jazz-tools/react";
import { useEffect, useState } from "react";
import { app } from "@/schema";

/** Bytes are read back in bounded pages, never as one whole-column read. */
export const ASSET_PAGE_BYTES = 256 * 1024;

type Db = ReturnType<typeof useDb>;
type AssetMeta = { id: string; byteLength: number; mimeType: string };

// Assets are immutable (permissions deny updates), so an object URL per asset
// id can be shared by every thumbnail and image shape for the whole session.
const urls = new Map<string, Promise<string>>();

/**
 * Read an asset's bytes with typed large-value range selections
 * (`select({ bytes: { from, to } })`, #2088) and expose them as an object URL.
 */
export function readAssetUrl(db: Db, asset: AssetMeta): Promise<string> {
  let pending = urls.get(asset.id);
  if (!pending) {
    pending = readAssetBlob(db, asset).then((blob) => URL.createObjectURL(blob));
    // A failed read must not poison later attempts, e.g. once the bytes sync.
    pending.catch(() => urls.delete(asset.id));
    urls.set(asset.id, pending);
  }
  return pending;
}

async function readAssetBlob(db: Db, asset: AssetMeta): Promise<Blob> {
  const pages: Uint8Array<ArrayBuffer>[] = [];
  for (let from = 0; from < asset.byteLength; from += ASSET_PAGE_BYTES) {
    const to = Math.min(asset.byteLength, from + ASSET_PAGE_BYTES);
    const page = await db.one(app.assets.where({ id: asset.id }).select({ bytes: { from, to } }), {
      tier: "local-first-unless-empty",
    });
    if (!page) throw new Error(`Asset ${asset.id} is not available yet`);
    pages.push(new Uint8Array(page.bytes));
  }
  return new Blob(pages, { type: asset.mimeType });
}

/** Object URL for an asset row the caller already has metadata for. */
export function useAssetUrl(asset: AssetMeta | null | undefined): string | null {
  const db = useDb();
  const [url, setUrl] = useState<{ id: string; url: string } | null>(null);
  const id = asset?.id;
  const byteLength = asset?.byteLength;
  const mimeType = asset?.mimeType;
  useEffect(() => {
    if (!id || byteLength === undefined || !mimeType) return;
    let cancelled = false;
    void readAssetUrl(db, { id, byteLength, mimeType })
      .then((next) => {
        if (!cancelled) setUrl({ id, url: next });
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [db, id, byteLength, mimeType]);
  return url && url.id === id ? url.url : null;
}

/** Object URL for an asset id, reading only its metadata columns first. */
export function useAssetUrlById(assetId: string | null | undefined): string | null {
  const { data } = useAll(
    assetId ? app.assets.where({ id: assetId }).select("id", "byteLength", "mimeType") : undefined,
  );
  return useAssetUrl(data?.[0]);
}
