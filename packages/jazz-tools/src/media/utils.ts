import { FileStream, ImageDefinition } from "jazz-tools";

export function getBestImage(
  update: ImageDefinition,
  wantedWidth: number,
  wantedHeight: number,
): FileStream | null {
  // no progressive means we must wait for the original
  if (update.progressive === false) {
    return update.original;
  }

  // the resizes are based on the biggest dimension of the original image
  const originalSize = Math.max(update.originalSize[0], update.originalSize[1]);

  const availableSizes = [256, 1024, 2048].filter((size) => {
    console.log(size, update[`${size}`]);
    // add check for wanted dimensions
    return update[`${size}`] !== undefined;
  });

  console.log({ availableSizes });

  const loadedSizes = availableSizes.filter((size) => {
    return update[`${size}`]?.getChunks();
  });

  const originalLoaded = update.original?.getChunks() ? update.original : null;

  console.log({ loadedSizes, originalLoaded });

  // if no sizes are loaded, return the original
  if (loadedSizes.length === 0) {
    return originalLoaded;
  }

  if (originalLoaded) {
    loadedSizes.push(originalSize);
  }

  const currentBestFit = findClosestSize(
    loadedSizes,
    wantedWidth,
    wantedHeight,
  );

  console.log({ currentBestFit });

  if (!currentBestFit) {
    return null;
  }

  if (currentBestFit === originalSize) {
    return update.original;
  }

  return update[currentBestFit] || null;
}

function sizesMatchWanted(
  w: number,
  h: number,
  wantedW: number,
  wantedH: number,
): number {
  // Calculate the Euclidean distance between (w, h) and (wantedW, wantedH)
  // Lower value means closer match
  const widthDiff = w - wantedW;
  const heightDiff = h - wantedH;
  return Math.sqrt(widthDiff * widthDiff + heightDiff * heightDiff);
}

function findClosestSize(
  sizes: number[],
  wantedWidth: number,
  wantedHeight: number,
): number {
  if (sizes.length === 0) {
    return 0;
  }

  return sizes.reduce((closest, size) => {
    if (
      sizesMatchWanted(size, size, wantedWidth, wantedHeight) <
      sizesMatchWanted(closest, closest, wantedWidth, wantedHeight)
    ) {
      return size;
    }
    return closest;
  }, sizes[0]!);
}
