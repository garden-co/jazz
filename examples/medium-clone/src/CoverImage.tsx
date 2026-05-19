import * as React from "react";
import { useDb } from "jazz-tools/react";
import { app } from "../schema.js";
import { logError } from "./telemetry.js";

/**
 * Load a `files` row as an object URL, revoke on unmount/change. Mirrors the
 * pattern used by `examples/chat-react/src/components/chat/ChatImage.tsx`,
 * just trimmed to what a single cover image needs.
 */
function useCoverImageUrl(fileId: string | null | undefined) {
  const db = useDb();
  const [url, setUrl] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (!fileId) {
      setUrl(null);
      return;
    }
    let cancelled = false;
    let createdUrl: string | null = null;

    db.loadFileAsBlob(app, fileId)
      .then((blob) => {
        if (cancelled) return;
        createdUrl = URL.createObjectURL(blob);
        setUrl(createdUrl);
      })
      .catch((err) => {
        if (cancelled) return;
        logError("medium.cover.load_failed", err, { "file.id": fileId.slice(0, 8) });
        setUrl(null);
      });

    return () => {
      cancelled = true;
      if (createdUrl) URL.revokeObjectURL(createdUrl);
    };
  }, [db, fileId]);

  return url;
}

/**
 * Stable, deterministic gradient seeded from a string. We use it as a cover
 * placeholder so the layout reads as intentional even when an article has no
 * uploaded image.
 */
function gradientFor(seed: string): string {
  let hash = 0;
  for (let i = 0; i < seed.length; i += 1) {
    hash = (hash * 31 + seed.charCodeAt(i)) | 0;
  }
  const h1 = Math.abs(hash) % 360;
  const h2 = (h1 + 60 + (Math.abs(hash >> 8) % 120)) % 360;
  const angle = Math.abs(hash >> 16) % 360;
  return `linear-gradient(${angle}deg, hsl(${h1} 60% 55%), hsl(${h2} 55% 35%))`;
}

function initialFor(title: string): string {
  const trimmed = title.trim();
  if (!trimmed) return "·";
  const first = [...trimmed][0];
  return (first ?? "·").toUpperCase();
}

type CoverProps = {
  fileId?: string | null;
  title: string;
  className?: string;
  /** Render the title initial as a watermark on the fallback gradient. */
  showInitial?: boolean;
};

/**
 * Renders the cover image for an article, falling back to a deterministic
 * gradient when none is set or while the blob is still loading.
 */
export function CoverImage({ fileId, title, className, showInitial = true }: CoverProps) {
  const url = useCoverImageUrl(fileId);
  const fallback = (
    <div
      className={["cover cover-fallback", className].filter(Boolean).join(" ")}
      style={{ backgroundImage: gradientFor(title || "untitled") }}
      aria-hidden={!showInitial || undefined}
    >
      {showInitial && <span className="cover-initial">{initialFor(title)}</span>}
    </div>
  );

  if (!fileId) return fallback;
  if (!url) {
    // While loading, show the same gradient so the layout doesn't jump.
    return fallback;
  }
  return (
    <div className={["cover cover-image", className].filter(Boolean).join(" ")}>
      <img src={url} alt="" />
    </div>
  );
}
