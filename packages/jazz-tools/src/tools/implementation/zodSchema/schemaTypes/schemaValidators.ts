import { Account, Group } from "../../../internal.js";
import { z } from "../zodReExport.js";
import type { CoreCoValueSchema } from "./CoValueSchema.js";

export const isCoValueSchema = (item: any): item is CoreCoValueSchema => {
  return (
    typeof item === "object" &&
    item !== null &&
    "collaborative" in item &&
    item.collaborative === true
  );
};

export const isAnyCoValue = z.object({
  $jazz: z.object({
    id: z.string(),
  }),
});

// any $jazz.id can be a valid account during validation
// TODO: improve this, validating some corner cases
export const isAccount = isAnyCoValue;
