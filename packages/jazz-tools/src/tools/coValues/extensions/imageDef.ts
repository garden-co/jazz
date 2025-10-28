import { z } from "../../implementation/zodSchema/zodReExport.js";
import { Loaded, coFileStreamDefiner, coMapDefiner } from "../../internal.js";

// avoiding circularity by using the standalone definers instead of `co`
const ImageDefinitionBase = coMapDefiner({
  original: coFileStreamDefiner(),
  originalSize: z.tuple([z.number(), z.number()]),
  placeholderDataURL: z.string().optional(),
  progressive: z.boolean(),
}).catchall(coFileStreamDefiner());

/** @category Media */
export const ImageDefinition = ImageDefinitionBase;
export type ImageDefinition = Loaded<typeof ImageDefinition>;
