import type { Db } from "jazz-tools";
import { app } from "../schema.js";

/** `size_bytes` is a 32-bit integer column. */
export const MAX_FILE_BYTES = 2 ** 31 - 1;

export class UploadCancelled extends Error {
  constructor() {
    super("Upload cancelled");
    this.name = "UploadCancelled";
  }
}

export interface UploadOptions {
  folderId: string;
  ownerId: string;
  signal?: AbortSignal;
  onProgress?: (uploadedBytes: number) => void;
  onSaving?: () => void;
}

/**
 * Stream a browser file into a new row without materializing it as one
 * application-owned Uint8Array. Aborting the signal fails the stream, and a
 * failed stream publishes no row.
 *
 * Resolves once the row is persisted in this browser's local storage, not
 * merely published: `insertStreaming` returns before the write is durable,
 * and a reload in between loses the file. After this, a reload keeps it and
 * it syncs whenever the server is reachable. `onSaving` marks the end of the
 * stream, while the bytes are written to storage.
 */
export async function uploadFile(db: Db, file: File, options: UploadOptions) {
  const { folderId, ownerId, signal, onProgress, onSaving } = options;
  if (file.size > MAX_FILE_BYTES) throw new Error("Files larger than 2 GB are not supported yet");
  const write = await db.insertStreaming(app.files, {
    folder_id: folderId,
    name: file.name,
    content_type: file.type || "application/octet-stream",
    size_bytes: file.size,
    owner_id: ownerId,
    contents: trackedStream(file.stream(), signal, onProgress),
  });
  onSaving?.();
  await write.wait({ tier: "local" });
  return write.value;
}

async function* trackedStream(
  stream: ReadableStream<Uint8Array>,
  signal: AbortSignal | undefined,
  onProgress: ((uploadedBytes: number) => void) | undefined,
): AsyncGenerator<Uint8Array> {
  const reader = stream.getReader();
  let uploaded = 0;
  try {
    while (true) {
      if (signal?.aborted) throw new UploadCancelled();
      const { done, value } = await reader.read();
      if (done) return;
      uploaded += value.byteLength;
      onProgress?.(uploaded);
      yield value;
    }
  } finally {
    await reader.cancel().catch(() => undefined);
  }
}

/** Read a whole file into a Blob, for download or full previews. */
export async function readFileBlob(db: Db, fileId: string, contentType: string): Promise<Blob> {
  const row = await db.one(app.files.where({ id: fileId }).select("contents"));
  if (!row) throw new Error("This file is not available");
  return new Blob([row.contents as Uint8Array<ArrayBuffer>], { type: contentType });
}

/**
 * Read the half-open byte range `[from, to)` of a file with a partial
 * large-value selection. The range is clamped to the file's size.
 */
export async function readFileRange(
  db: Db,
  file: { id: string; size_bytes: number },
  from: number,
  to: number,
): Promise<Uint8Array> {
  const start = Math.max(0, Math.min(from, file.size_bytes));
  const end = Math.max(start, Math.min(to, file.size_bytes));
  if (end === start) return new Uint8Array();
  const row = await db.one(
    app.files.where({ id: file.id }).select({ contents: { from: start, to: end } }),
  );
  if (!row) throw new Error("This file is not available");
  return row.contents;
}

/** Hand a Blob to the browser as a download. */
export function saveBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1_000);
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value < 10 ? value.toFixed(1) : Math.round(value)} ${units[unit]}`;
}

export type PreviewKind = "image" | "audio" | "video" | "pdf" | "text" | "binary";

const TEXT_TYPES = new Set([
  "application/json",
  "application/xml",
  "application/javascript",
  "application/x-sh",
  "application/yaml",
  "application/toml",
]);

export function previewKind(contentType: string): PreviewKind {
  const type = contentType.split(";")[0]!.trim().toLowerCase();
  if (type.startsWith("image/")) return "image";
  if (type.startsWith("audio/")) return "audio";
  if (type.startsWith("video/")) return "video";
  if (type === "application/pdf") return "pdf";
  if (type.startsWith("text/") || TEXT_TYPES.has(type) || type.endsWith("+json")) return "text";
  return "binary";
}

/** Offset, hex and printable ASCII, `perRow` bytes per line. */
export function hexDump(bytes: Uint8Array, perRow = 16): string {
  const lines: string[] = [];
  for (let offset = 0; offset < bytes.length; offset += perRow) {
    const row = bytes.subarray(offset, offset + perRow);
    const hex = [...row].map((byte) => byte.toString(16).padStart(2, "0")).join(" ");
    const ascii = [...row]
      .map((byte) => (byte >= 0x20 && byte < 0x7f ? String.fromCharCode(byte) : "."))
      .join("");
    lines.push(`${offset.toString(16).padStart(8, "0")}  ${hex.padEnd(perRow * 3 - 1)}  ${ascii}`);
  }
  return lines.join("\n");
}
