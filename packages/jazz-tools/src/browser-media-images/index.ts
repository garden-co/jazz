import ImageBlobReduce from "image-blob-reduce";
import {
  Account,
  FileStream,
  Group,
  ImageDefinition,
  Loaded,
} from "jazz-tools";
import Pica from "pica";

let reducer: ImageBlobReduce.ImageBlobReduce | undefined;

/** @category Image creation */
export async function createImage(
  imageBlobOrFile: Blob | File,
  options?: {
    owner?: Group | Account;
    maxSize?: 256 | 1024 | 2048;
  },
): Promise<Loaded<typeof ImageDefinition, { $each: true }>> {
  // Get the original size of the image
  const { width: originalWidth, height: originalHeight } =
    await getImageSize(imageBlobOrFile);

  const highestDimension = Math.max(originalWidth, originalHeight);

  // Calculate the sizes to resize the image to
  const resizes = [256, 1024, 2048, highestDimension]
    .filter((s) => s <= (options?.maxSize ?? highestDimension))
    .toSorted((a, b) => a - b);

  // Get the highest resolution to use as final original size
  // In case of options.maxSize, it's not the originalWidth/Height
  const { width: finalWidth, height: finalHeight } = getNewDimensions(
    originalWidth,
    originalHeight,
    resizes.at(-1)!,
  );

  const imageDefinition = ImageDefinition.create(
    { originalSize: [finalWidth, finalHeight] },
    options?.owner,
  );
  const owner = imageDefinition.$jazz.owner;

  // Placeholder 8x8
  imageDefinition.placeholderDataURL =
    await getPlaceholderBase64(imageBlobOrFile);

  // Resizes for progressive loading
  for (let size of resizes) {
    // Calculate width and height respecting the aspect ratio
    const { width, height } = getNewDimensions(
      originalWidth,
      originalHeight,
      size,
    );

    const image = await resize(imageBlobOrFile, width, height);

    const binaryStream = await FileStream.createFromBlob(image, owner);
    imageDefinition[`${width}x${height}`] = binaryStream;
  }

  return imageDefinition;
}

async function getImageSize(
  imageBlobOrFile: Blob | File,
): Promise<{ width: number; height: number }> {
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
  imageBlobOrFile: Blob | File,
): Promise<string> {
  // Inizialize Reducer here to not have module side effects
  if (!reducer) {
    reducer = new ImageBlobReduce({ pica: new Pica() });
  }

  const canvas = await reducer.toCanvas(imageBlobOrFile, { max: 8 });
  return canvas.toDataURL("image/png");
}

async function resize(
  imageBlobOrFile: Blob | File,
  width: number,
  height: number,
): Promise<Blob> {
  // Inizialize Reducer here to not have module side effects
  if (!reducer) {
    reducer = new ImageBlobReduce({ pica: new Pica() });
  }

  return reducer.toBlob(imageBlobOrFile, { max: Math.max(width, height) });
}

const getNewDimensions = (
  originalWidth: number,
  originalHeight: number,
  maxSize: number,
) => {
  const width =
    originalWidth > originalHeight
      ? maxSize
      : Math.round(maxSize * (originalWidth / originalHeight));

  const height =
    originalHeight > originalWidth
      ? maxSize
      : Math.round(maxSize * (originalHeight / originalWidth));

  return { width, height };
};
