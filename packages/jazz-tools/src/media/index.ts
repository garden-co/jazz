import type { ImageDefinition } from "jazz-tools";
import {
  CreateImageOptions,
  SourceType,
  createImageFactory,
} from "./create-image.js";

export { highestResAvailable, loadImage, loadImageBySize } from "./utils.js";
export { createImageFactory };

export declare function createImage(
  imageBlobOrFile: SourceType,
  options?: CreateImageOptions,
): Promise<ImageDefinition>;
