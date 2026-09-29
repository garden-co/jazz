"use client";

import { useEffect, useState } from "react";
import { useAll } from "jazz-tools/react";
import { app } from "../schema";

const PLACEHOLDER_HUES = [
  "blue",
  "cyan",
  "green",
  "orange",
  "pink",
  "purple",
  "red",
  "teal",
  "yellow",
] as const;

/** A stable token colour for albums without uploaded art. */
export function placeholderHue(seed: string): (typeof PLACEHOLDER_HUES)[number] {
  let hash = 0;
  for (const char of seed) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return PLACEHOLDER_HUES[hash % PLACEHOLDER_HUES.length]!;
}

type Size = "sm" | "md" | "lg";

/**
 * Album art. The cover bytes are a separate, lazily read column: the shelf
 * query never selects them, so browsing stays metadata-only and each cover
 * arrives on its own.
 */
export function CoverArt({ albumId, title, size }: { albumId: string; title: string; size: Size }) {
  const cover = useAll(app.albums.where({ id: albumId }).select("cover_image", "cover_mime"));
  const row = cover.data?.[0];
  const url = useObjectUrl(row?.cover_image ?? undefined, row?.cover_mime ?? undefined);
  if (url) {
    return <img className="rp-cover" data-size={size} src={url} alt="" />;
  }
  return (
    <div
      className="rp-cover rp-cover-placeholder"
      data-size={size}
      data-hue={placeholderHue(albumId)}
    >
      {size !== "sm" && <span className="rp-cover-title">{title}</span>}
    </div>
  );
}

function useObjectUrl(bytes: Uint8Array | undefined, mime: string | undefined) {
  const [url, setUrl] = useState<string>();
  useEffect(() => {
    if (!bytes || bytes.byteLength === 0) return setUrl(undefined);
    const next = URL.createObjectURL(new Blob([bytes as BlobPart], { type: mime }));
    setUrl(next);
    return () => URL.revokeObjectURL(next);
  }, [bytes, mime]);
  return url;
}
