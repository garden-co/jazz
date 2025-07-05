import ImageBlobReduce from "image-blob-reduce";
import { Account, FileStream, Group, ImageDefinition } from "jazz-tools";
import Pica from "pica";
import { CreateImageOptions, createImageFactory } from "./create-image";

export { highestResAvailable, loadImage, loadImageBySize } from "./utils";

export { createImageFactory };

export async function createImage(
  imageBlobOrFile: Blob | File | string,
  options?: CreateImageOptions,
): Promise<ImageDefinition> {
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

async function getImageSize(
  imageBlobOrFile: Blob | File | string,
): Promise<{ width: number; height: number }> {
  if (typeof imageBlobOrFile === "string") {
    throw new Error("getImageSize(string) is not supported on browser");
  }
  // using createImageBitmap is ~10x slower than Image object
  // Image object: 640 milliseconds
  // createImageBitmap: 8128 milliseconds
  const { width, height } = await new Promise<{
    width: number;
    height: number;
  }>((resolve, reject) => {
    const img = new Image();
    img.onload = () => {
      resolve({ width: img.width, height: img.height });
      URL.revokeObjectURL(img.src);
    };
    img.onerror = () => {
      reject(new Error("Failed to load image"));
      URL.revokeObjectURL(img.src);
    };

    img.src = URL.createObjectURL(imageBlobOrFile);
  });

  return { width, height };
}

async function getPlaceholderBase64(
  imageBlobOrFile: Blob | File | string,
): Promise<string> {
  // Inizialize Reducer here to not have module side effects
  if (!reducer) {
    reducer = new ImageBlobReduce({ pica: new Pica() });
  }

  if (typeof imageBlobOrFile === "string") {
    throw new Error("getPlaceholderBase64(string) is not supported on browser");
  }

  const canvas = await reducer.toCanvas(imageBlobOrFile, { max: 8 });
  return canvas.toDataURL("image/png");
}

async function resize(
  imageBlobOrFile: Blob | File | string,
  width: number,
  height: number,
): Promise<Blob> {
  // Inizialize Reducer here to not have module side effects
  if (!reducer) {
    reducer = new ImageBlobReduce({ pica: new Pica() });
  }

  if (typeof imageBlobOrFile === "string") {
    throw new Error("resize(string) is not supported on browser");
  }

  return reducer.toBlob(imageBlobOrFile, { max: Math.max(width, height) });
}
