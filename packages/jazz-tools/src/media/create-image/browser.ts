import { Account, FileStream, Group } from "jazz-tools";
import { createImageFactory } from "../create-image-factory";

/**
 * Creates an ImageDefinition from an image File or Blob with built-in UX features.
 *
 * This function creates a specialized CoValue for managing images in Jazz applications.
 * It supports blurry placeholders, built-in resizing, and progressive loading patterns.
 *
 * @returns Promise that resolves to an ImageDefinition
 *
 * @example
 * ```ts
 * import { createImage } from "jazz-tools/media";
 *
 * // Create an image from a file input
 * async function handleFileUpload(event: React.ChangeEvent<HTMLInputElement>) {
 *   const file = event.target.files?.[0];
 *   if (file) {
 *     // Creates ImageDefinition with a blurry placeholder, limited to 1024px
 *     // on the longest side, and multiple resolutions automatically
 *     const image = await createImage(file, {
 *       owner: me._owner,
 *       maxSize: 1024,
 *       placeholder: "blur",
 *       progressive: true,
 *     });
 *
 *     // Store the image in your application data
 *     me.profile.image = image;
 *   }
 * }
 * ```
 */
export const createImage = createImageFactory(
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

//  Image Manipulations
async function createFileStreamFromSource(
  imageBlobOrFile: Blob | File,
  owner?: Account | Group,
): Promise<FileStream> {
  return FileStream.createFromBlob(imageBlobOrFile, owner);
}

// using createImageBitmap is ~10x slower than Image object
// Image object: 640 milliseconds
// createImageBitmap: 8128 milliseconds
function getImageFromBlob(blob: Blob): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => {
      resolve(img);
      URL.revokeObjectURL(img.src);
    };
    img.onerror = () => {
      reject(new Error("Failed to load image"));
      URL.revokeObjectURL(img.src);
    };
    img.src = URL.createObjectURL(blob);
  });
}

async function getImageSize(
  imageBlobOrFile: Blob | File,
): Promise<{ width: number; height: number }> {
  const image = await getImageFromBlob(imageBlobOrFile);

  return { width: image.width, height: image.height };
}

async function getPlaceholderBase64(
  imageBlobOrFile: Blob | File,
): Promise<string> {
  const image = await getImageFromBlob(imageBlobOrFile);

  const { width, height } = resizeDimensionsKeepingAspectRatio(
    image.width,
    image.height,
    8,
  );

  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;

  const ctx = canvas.getContext("2d");

  if (!ctx) {
    throw new Error("Failed to get context");
  }

  ctx.drawImage(image, 0, 0, width, height);

  return canvas.toDataURL("image/png");
}

const resizeDimensionsKeepingAspectRatio = (
  width: number,
  height: number,
  maxSize: number,
): { width: number; height: number } => {
  if (width <= maxSize && height <= maxSize) {
    return { width, height };
  }

  const aspectRatio = width / height;

  if (width >= height) {
    return { width: maxSize, height: Math.round(maxSize / aspectRatio) };
  } else {
    return { width: Math.round(maxSize * aspectRatio), height: maxSize };
  }
};

async function resize(
  imageBlobOrFile: Blob | File,
  width: number,
  height: number,
): Promise<Blob> {
  const mimeType = imageBlobOrFile.type;

  const image = await getImageFromBlob(imageBlobOrFile);

  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;

  const ctx = canvas.getContext("2d");

  if (!ctx) {
    throw new Error("Failed to get context");
  }

  ctx.drawImage(image, 0, 0, width, height);

  return new Promise<Blob>((resolve, reject) => {
    canvas.toBlob(
      (blob) => {
        if (!blob) {
          reject(new Error("Failed to convert canvas to blob"));
          return;
        }
        resolve(blob);
      },
      mimeType,
      0.8,
    );
  });
}
