import { Account, FileStream, Group } from "jazz-tools";
import type sharp from "sharp";
import { createImageFactory } from "../create-image-factory";

export type SharpImageType =
  | File
  | Blob
  | Buffer
  | ArrayBuffer
  | Uint8Array
  | Uint8ClampedArray
  | Int8Array
  | Uint16Array
  | Int16Array
  | Uint32Array
  | Int32Array
  | Float32Array
  | Float64Array;

let sharpModule: typeof import("sharp");

async function getSharp() {
  if (!sharpModule) {
    sharpModule = await import("sharp").then((m) => m.default);
  }

  return sharpModule;
}

/**
 * Creates an ImageDefinition from an image File, Blob or Buffer with built-in UX features.
 *
 * This function creates a specialized CoValue for managing images in Jazz applications.
 * It supports blurry placeholders, built-in resizing, and progressive loading patterns.
 *
 * @returns Promise that resolves to an ImageDefinition
 *
 * @example
 * ```ts
 * import fs from "node:fs";
 * import { createImage } from "jazz-tools/media";
 *
 * const imageBuffer = fs.readFileSync("path/to/image.jpg");
 * const image = await createImage(imageBuffer, {
 *   maxSize: 800,
 *   placeholder: "blur",
 *   progressive: false,
 * });
 * ```
 */
export const createImage = createImageFactory<SharpImageType, Blob>(
  {
    createFileStreamFromSource,
    getImageSize,
    getPlaceholderBase64,
    resize,
  },
  (imageBlobOrFile) => {
    if (typeof imageBlobOrFile === "string") {
      throw new Error("createImage(string) is not supported on this platform");
    }
  },
);

function formatToMimeType(format: keyof sharp.FormatEnum | undefined) {
  const formatToTypeMap: Record<
    Extract<
      keyof sharp.FormatEnum,
      "avif" | "gif" | "jpeg" | "jpg" | "png" | "webp"
    >,
    string
  > = {
    avif: "image/avif",
    gif: "image/gif",
    jpeg: "image/jpeg",
    jpg: "image/jpeg",
    png: "image/png",
    webp: "image/webp",
  };

  if (!format) {
    throw new Error("Could not determine image format");
  }

  if (!(format in formatToTypeMap)) {
    throw new Error(`Unsupported image format: ${format}`);
  }

  return formatToTypeMap[format as keyof typeof formatToTypeMap];
}

async function createFileStreamFromSource(
  imageBlobOrBuffer: SharpImageType,
  owner?: Account | Group,
): Promise<FileStream> {
  // `File` is also an instance of `Blob`
  if (imageBlobOrBuffer instanceof Blob) {
    return FileStream.createFromBlob(imageBlobOrBuffer, { owner });
  }

  const sharp = await getSharp();

  const image = sharp(imageBlobOrBuffer);
  const metadata = await image.metadata();
  const format = metadata.format;
  const mimeType = formatToMimeType(format);

  return FileStream.createFromArrayBuffer(
    imageBlobOrBuffer,
    mimeType,
    undefined,
    { owner },
  );
}

async function getImageSize(
  imageBlobOrBuffer: SharpImageType,
): Promise<{ width: number; height: number }> {
  const imageBuffer =
    imageBlobOrBuffer instanceof Blob
      ? await imageBlobOrBuffer.arrayBuffer()
      : imageBlobOrBuffer;

  const sharp = await getSharp();
  const image = sharp(imageBuffer);
  const metadata = await image.metadata();

  return { width: metadata.width!, height: metadata.height! };
}

async function getPlaceholderBase64(
  imageBlobOrBuffer: SharpImageType,
): Promise<string> {
  const imageBuffer =
    imageBlobOrBuffer instanceof Blob
      ? await imageBlobOrBuffer.arrayBuffer()
      : imageBlobOrBuffer;

  const sharp = await getSharp();

  const image = sharp(imageBuffer);
  const placeholder = await image
    .resize({
      width: 8,
      height: 8,
      fit: "inside",
    })
    .toFormat("png")
    .toBuffer();

  return `data:image/png;base64,${placeholder.toString("base64")}`;
}

async function resize(
  imageBlobOrBuffer: SharpImageType,
  width: number,
  height: number,
): Promise<Blob> {
  const imageBuffer =
    imageBlobOrBuffer instanceof Blob
      ? await imageBlobOrBuffer.arrayBuffer()
      : imageBlobOrBuffer;

  const sharp = await getSharp();

  const image = sharp(imageBuffer);
  const metadata = await image.metadata();
  const format = metadata.format;
  const mimeType = formatToMimeType(format);

  const resizedBuffer = await image
    .resize({
      width,
      height,
    })
    .toBuffer();

  return new Blob([new Uint8Array(resizedBuffer)], {
    type: mimeType,
  });
}
