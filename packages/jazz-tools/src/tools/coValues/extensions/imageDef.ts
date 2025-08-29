import { z } from "../../implementation/zodSchema/zodReExport.js";
import {
  Loaded,
  coFileStreamDefiner,
  coMapDefiner,
  coRecordDefiner,
} from "../../internal.js";

// avoiding circularity by using the standalone definers instead of `co`
const ImageDefinitionBase = coMapDefiner({
  original: coFileStreamDefiner(),
  originalSize: z.tuple([z.number(), z.number()]),
  placeholderDataURL: z.string().optional(),
  progressive: z.boolean(),
  resolutions: coRecordDefiner(z.string(), coFileStreamDefiner()),
});

/** @category Media */
export const ImageDefinition = ImageDefinitionBase;
export type ImageDefinition = Loaded<
  typeof ImageDefinition,
  { resolutions: true }
>;
