import { Account, FileStream, Group, ImageDefinition } from "jazz-tools";
import { CreateImageOptions, createImageFactory } from "./create-image.js";

export { highestResAvailable, loadImage, loadImageBySize } from "./utils.js";

export { createImageFactory };

export async function createImage(
  imageBlobOrFile: Blob | File | string,
  options?: CreateImageOptions,
) {
  return createImageFactory({
    createFileStreamFromSource,
    getImageSize,
    getPlaceholderBase64,
    resize,
  })(imageBlobOrFile, options || {});
}

//  Image Manipulations
async function createFileStreamFromSource(
  imageBlobOrFile: Blob | File | string,
  owner?: Account | Group,
): Promise<FileStream> {
  if (typeof imageBlobOrFile === "string") {
    throw new Error(
      "createFileStreamFromSource(string) is not supported on this platform",
    );
  }

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
  imageBlobOrFile: Blob | File | string,
): Promise<{ width: number; height: number }> {
  if (typeof imageBlobOrFile === "string") {
    throw new Error("getImageSize(string) is not supported on browser");
  }

  const image = await getImageFromBlob(imageBlobOrFile);

  return { width: image.width, height: image.height };
}

async function getPlaceholderBase64(
  imageBlobOrFile: Blob | File | string,
): Promise<string> {
  if (typeof imageBlobOrFile === "string") {
    throw new Error("getPlaceholderBase64(string) is not supported on browser");
  }

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
) => {
  const aspectRatio = width / height;
  if (width > height) {
    return {
      width: Math.min(width, maxSize),
      height: Math.round(width / aspectRatio),
    };
  }
  return {
    width: Math.round(height * aspectRatio),
    height: Math.min(height, maxSize),
  };
};

async function resize(
  imageBlobOrFile: Blob | File | string,
  width: number,
  height: number,
): Promise<Blob> {
  if (typeof imageBlobOrFile === "string") {
    throw new Error("resize(string) is not supported on browser");
  }

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
