import type { CoID } from "cojson";
import { Account, FileStream, ImageDefinition } from "jazz-tools";

export function highestResAvailable(
  image: ImageDefinition,
  wantedWidth: number,
  wantedHeight: number,
): { width: number; height: number; image: FileStream } | null {
  const availableSizes: [number, number, string][] = image.resolutions.$jazz.raw
    .keys()
    .filter((key) => /^\d+x\d+$/.test(key))
    .map((key) => {
      const [w, h] = key.split("x").map(Number) as [number, number];
      return [w, h, key];
    });

  if (availableSizes.length === 0) {
    return image.original
      ? {
          width: image.originalSize[0],
          height: image.originalSize[1],
          image: image.original,
        }
      : null;
  }

  const sortedSizes = availableSizes
    .map((size) => {
      return {
        size,
        match: sizesMatchWanted(size[0], size[1], wantedWidth, wantedHeight),
        isLoaded: isLoaded(
          image.resolutions.$jazz.raw.get(size[2]) as CoID<any> | undefined,
        ),
      };
    })
    .sort((a, b) => a.match - b.match);

  // We try to find the better already loaded image
  // note: `toReversed` is not available in react-native.
  const bestLoaded = [...sortedSizes]
    .reverse()
    .find((el) => el.isLoaded && image.resolutions?.[el.size[2]]?.getChunks());

  // if I can't find a good match, let's use the highest resolution
  const bestTarget =
    sortedSizes.find((el) => el.match > 0.95) || sortedSizes.at(-1);

  // if the best target is already loaded, we are done
  const bestTargetImage = image.resolutions?.[bestTarget!.size[2]];
  if (bestTargetImage?.getChunks()) {
    return bestTargetImage
      ? {
          width: bestTarget!.size[0],
          height: bestTarget!.size[1],
          image: bestTargetImage,
        }
      : null;
  }

  // if the best already loaded is not the best target
  // let's trigger the load of the best target
  if (bestLoaded) {
    bestTargetImage?.getChunks();
    const bestLoadedImage = image.resolutions?.[bestLoaded.size[2]];
    return bestLoadedImage
      ? {
          width: bestLoaded.size[0],
          height: bestLoaded.size[1],
          image: bestLoadedImage,
        }
      : null;
  }

  // if nothing is loaded, then start fetching all the images till the best
  for (let size of sortedSizes) {
    if (size.match <= bestTarget!.match) {
      image.resolutions?.[size.size[2]]?.getChunks();
    }
  }

  return null;
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

  // // Below 0.95 means the image is too small, we don't want to upscale it
  // if (areaRatio < 0.95) {
  //   return 9999;
  // }

  return areaRatio;
}

function isLoaded(id: CoID<any> | null | undefined): boolean {
  if (!id) {
    return false;
  }

  return !!Account.getMe().$jazz.localNode.getLoaded(id);
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

  if (!imageOrId.original) {
    console.warn("Unable to find the original image");
    return null;
  }

  const loadedOriginal = await FileStream.load(imageOrId.original.$jazz.id);

  if (!loadedOriginal) {
    console.warn("Unable to find the original image");
    return null;
  }

  return {
    width: imageOrId.originalSize[0],
    height: imageOrId.originalSize[1],
    image: loadedOriginal,
  };
}

export async function loadImageBySize(
  imageOrId: ImageDefinition | string,
  wantedWidth: number,
  wantedHeight: number,
): Promise<{ width: number; height: number; image: FileStream } | null> {
  const image: ImageDefinition | null =
    typeof imageOrId === "string"
      ? await ImageDefinition.load(imageOrId, {
          resolve: { resolutions: true },
        })
      : imageOrId;

  if (image === null) {
    return null;
  }

  if (image.progressive === false) {
    return loadImage(imageOrId);
  }

  const availableSizes: [number, number, string][] = image.resolutions.$jazz.raw
    .keys()
    .filter((key) => /^\d+x\d+$/.test(key))
    .map((key) => {
      const [w, h] = key.split("x").map(Number) as [number, number];
      return [w, h, key];
    });

  if (availableSizes.length === 0) {
    return null;
  }

  const sortedSizes = availableSizes
    .map((size) => ({
      size,
      match: sizesMatchWanted(size[0], size[1], wantedWidth, wantedHeight),
    }))
    .sort((a, b) => a.match - b.match);

  const bestTarget =
    sortedSizes.find((el) => el.match > 0.95) || sortedSizes.at(-1)!;

  // The image's `wxh` keys reference FileStream.
  // image[bestTarget.size[2]] returns undefined if FileStream hasn't loaded yet.
  // Since we only need the file's ID to fetch it later, we check the raw _refs
  // which contain only the linked covalue's ID.
  const file = image.resolutions?.$jazz.refs[bestTarget.size[2]];

  if (!file) {
    return null;
  }

  const loadedFile = await FileStream.load(file.id);

  if (!loadedFile) {
    return null;
  }

  return {
    width: bestTarget.size[0],
    height: bestTarget.size[1],
    image: loadedFile,
  };
}
