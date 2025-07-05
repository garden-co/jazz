import { FileStream, ImageDefinition } from "jazz-tools";

const INTERNAL_SIZES = [256, 1024, 2048] as const;

export function highestResAvailable(
  image: ImageDefinition,
  wantedWidth: number,
  wantedHeight: number,
): FileStream | null {
  const availableSizes = Object.keys(image)
    .filter((key) => /^\d+x\d+$/.test(key))
    .map((key) => key.split("x").map(Number) as [number, number]);

  if (availableSizes.length === 0) {
    return image.original;
  }

  console.count("CALLED");

  const sortedSizes = sortByBestFit(availableSizes, wantedWidth, wantedHeight);

  // This still triggers the shallow load, waiting for the best fit to be loaded
  const findLoaded = sortedSizes.find(({ size }) => {
    return image[`${size[0]}x${size[1]}`]?.getChunks();
  });

  if (!findLoaded) {
    return null;
  }

  return image[`${findLoaded.size[0]}x${findLoaded.size[1]}`] || null;
}

function sizesMatchWanted(
  w: number,
  h: number,
  wantedW: number,
  wantedH: number,
): number {
  const area1 = w * h;
  const area2 = wantedW * wantedH;

  const areaRatio = area1 / area2;

  // Below 0.95 means the image is too small, we don't want to upscale it
  if (areaRatio < 0.95) {
    return 9999;
  }

  return areaRatio;
}

function sortByBestFit(
  sizes: [number, number][],
  wantedWidth: number,
  wantedHeight: number,
): Array<{ size: [number, number]; match: number }> {
  if (sizes.length === 0) {
    return [];
  }

  const bestFit = sizes
    .map((size) => {
      return {
        size,
        match: sizesMatchWanted(size[0], size[1], wantedWidth, wantedHeight),
      };
    })
    .sort((a, b) => a.match - b.match);

  // We might want to cut unprobable sizes
  return bestFit;
}

export async function loadImage(
  imageOrId: ImageDefinition | string,
): Promise<{ width: number; height: number; image: FileStream } | null> {
  if (typeof imageOrId === "string") {
    const image = await ImageDefinition.load(imageOrId, {
      resolve: {
        original: true,
      },
    });

    if (image === null || image.original === null) {
      return null;
    }

    return {
      width: image.originalSize[0],
      height: image.originalSize[1],
      image: image.original,
    };
  }

  await imageOrId.ensureLoaded({
    resolve: {
      original: true,
    },
  });

  if (!imageOrId.original) {
    console.warn("Unable to find the original image");
    return null;
  }

  return {
    width: imageOrId.originalSize[0],
    height: imageOrId.originalSize[1],
    image: imageOrId.original,
  };
}

export async function loadImageBySize(
  imageOrId: ImageDefinition | string,
  wantedWidth: number,
  wantedHeight: number,
): Promise<{ width: number; height: number; image: FileStream } | null> {
  const image =
    typeof imageOrId === "string"
      ? await ImageDefinition.load(imageOrId)
      : imageOrId;

  if (image === null) {
    return null;
  }

  const availableSizes = Object.keys(image)
    .filter((key) => /^\d+x\d+$/.test(key))
    .map((key) => key.split("x").map(Number) as [number, number]);

  if (availableSizes.length === 0) {
    return null;
  }

  if (image.progressive === false) {
    return loadImage(imageOrId);
  }

  const sortedSizes = sortByBestFit(availableSizes, wantedWidth, wantedHeight);

  const bestFitSize = sortedSizes[0];

  if (!bestFitSize) {
    return null;
  }

  const deepLoaded = await ImageDefinition.load(image.id, {
    resolve: {
      [`${bestFitSize.size[0]}x${bestFitSize.size[1]}`]: true,
    },
  });

  if (
    deepLoaded === null ||
    deepLoaded[`${bestFitSize.size[0]}x${bestFitSize.size[1]}`] === undefined
  ) {
    return null;
  }

  return {
    width: bestFitSize.size[0],
    height: bestFitSize.size[1],
    image: deepLoaded[`${bestFitSize.size[0]}x${bestFitSize.size[1]}`]!,
  };
}
