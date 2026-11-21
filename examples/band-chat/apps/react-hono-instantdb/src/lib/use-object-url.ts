import { useEffect, useState } from "react";

/** An object URL for in-memory bytes or a File, revoked when it changes. */
export function useObjectUrl(
  source: Uint8Array | Blob | null | undefined,
  type?: string | null,
): string | undefined {
  const [url, setUrl] = useState<string>();
  useEffect(() => {
    if (!source) {
      setUrl(undefined);
      return;
    }
    const blob =
      source instanceof Blob ? source : new Blob([source as BlobPart], { type: type ?? undefined });
    const next = URL.createObjectURL(blob);
    setUrl(next);
    return () => URL.revokeObjectURL(next);
  }, [source, type]);
  return url;
}
