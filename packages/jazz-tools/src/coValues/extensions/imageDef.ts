import z from "zod/v4";
import {
  Loaded,
  co,
  coFileStreamDefiner,
  coMapDefiner,
} from "../../internal.js";

// avoiding circularity by using the standalone definers instead of `co`
const ImageDefinitionBase = coMapDefiner({
  originalSize: z.tuple([z.number(), z.number()]),
  placeholderDataURL: z.string().optional(),
}).catchall(coFileStreamDefiner());

/** @category Media */
export const ImageDefinition = ImageDefinitionBase.withHelpers((Self) => ({
  highestResAvailable(
    imageDef: Loaded<typeof Self>,
    options?: {
      maxWidth?: number;
      targetWidth?: number;
    },
  ) {
    const resolutions = Object.keys(imageDef).filter((key) =>
      key.match(/^\d+x\d+$/),
    ) as `${number}x${number}`[];

    let maxWidth = options?.maxWidth;

    if (options?.targetWidth) {
      const targetWidth = options.targetWidth;
      const widths = resolutions.map((res) => Number(res.split("x")[0]));

      maxWidth = Math.min(...widths.filter((w) => w >= targetWidth));
    }

    const validResolutions = resolutions.filter(
      (key) => maxWidth === undefined || Number(key.split("x")[0]) <= maxWidth,
    ) as `${number}x${number}`[];

    // Sort the resolutions by width, smallest to largest
    validResolutions.sort((a, b) => {
      const aWidth = Number(a.split("x")[0]);
      const bWidth = Number(b.split("x")[0]);
      return aWidth - bWidth; // Sort smallest to largest
    });

    let highestAvailableResolution: `${number}x${number}` | undefined;

    for (const resolution of validResolutions) {
      if (imageDef[resolution] && imageDef[resolution]?.getChunks()) {
        highestAvailableResolution = resolution;
      }
    }

    // Return the highest complete resolution if we found one
    return (
      highestAvailableResolution && {
        res: highestAvailableResolution,
        stream: imageDef[highestAvailableResolution]!,
      }
    );
  },
}));
export type ImageDefinition = co.loaded<typeof ImageDefinition>;
