// These bound only what the pickers in this app accept. Neither the schema nor
// the storage rules constrain size, so they are UX validation, not an
// authorization or security boundary for direct uploads.
export const ATTACHMENT_PICKER_MAX_BYTES = 10 * 1024 * 1024;
export const AVATAR_MAX_EDGE = 256;

const allowedAttachmentTypes = new Set([
  "image/png",
  "image/jpeg",
  "image/webp",
  "image/gif",
  "audio/mpeg",
  "audio/mp4",
  "audio/wav",
  "text/plain",
  "application/pdf",
]);

export const attachmentAccept = [...allowedAttachmentTypes].join(",");

/** Returns a user-facing problem with the file, or null when it can be sent. */
export function attachmentProblem(file: File): string | null {
  if (!allowedAttachmentTypes.has(file.type))
    return `${file.name}: use an image, audio, text or PDF file.`;
  if (file.size > ATTACHMENT_PICKER_MAX_BYTES)
    return `${file.name} is larger than 10 MB. The picker limit is client-side validation only.`;
  return null;
}

export function isImageType(type: string | null | undefined): boolean {
  return !!type && type.startsWith("image/");
}

export function isAudioType(type: string | null | undefined): boolean {
  return !!type && type.startsWith("audio/");
}

export function formatBytes(bytes: number | null | undefined): string {
  if (bytes == null) return "";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

/** Save a stored file under its name, through a temporary object URL. */
export async function downloadUrl(url: string, name: string) {
  const blob = await (await fetch(url)).blob();
  const objectUrl = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = objectUrl;
  link.download = name;
  link.click();
  setTimeout(() => URL.revokeObjectURL(objectUrl), 0);
}

/** Downscale a picked image to a small square-ish WebP for a profile avatar. */
export async function avatarFromFile(file: File): Promise<{ dataUrl: string; type: string }> {
  if (!isImageType(file.type)) throw new Error("Choose an image for your avatar.");
  const bitmap = await createImageBitmap(file);
  const scale = Math.min(1, AVATAR_MAX_EDGE / Math.max(bitmap.width, bitmap.height));
  const canvas = document.createElement("canvas");
  canvas.width = Math.max(1, Math.round(bitmap.width * scale));
  canvas.height = Math.max(1, Math.round(bitmap.height * scale));
  canvas.getContext("2d")!.drawImage(bitmap, 0, 0, canvas.width, canvas.height);
  bitmap.close();
  const blob = await new Promise<Blob | null>((resolve) =>
    canvas.toBlob(resolve, "image/webp", 0.85),
  );
  if (!blob) throw new Error("Could not read that image.");
  const dataUrl = await new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as string);
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(blob);
  });
  return { dataUrl, type: blob.type };
}
