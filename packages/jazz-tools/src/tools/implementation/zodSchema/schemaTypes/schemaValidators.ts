import { Account, Group, isCoValueSchema, TypeSym } from "../../../internal.js";
import { z } from "../zodReExport.js";
import type { CoreCoValueSchema } from "./CoValueSchema.js";

type InputSchema =
  | typeof Group
  | typeof Account
  | CoreCoValueSchema
  | z.ZodType
  | z.core.$ZodType;

export function generateValidationSchemaFromItem(item: InputSchema): z.ZodType {
  // item is Group class
  // This is because users can define the schema
  // using Group class instead of GroupSchema
  // e.g. `co.map({ group: Group })` vs `co.map({ group: co.group() })`
  if ("prototype" in item && item.prototype?.[TypeSym] === "Group") {
    return z.instanceof(Group);
  }
  // Same as above: `co.map({ account: Account })` vs `co.map({ account: co.account() })`
  if ("prototype" in item && item.prototype?.[TypeSym] === "Account") {
    return z.instanceof(Account);
  }

  if (isCoValueSchema(item)) {
    return item.getValidationSchema();
  }

  if (item instanceof z.core.$ZodType) {
    // the following zod types are not supported:
    if (
      // codecs are managed lower level
      (item as z.ZodType).def.type === "pipe"
    ) {
      return z.any();
    }

    return item as z.ZodType;
  }

  throw new Error(`Unsupported schema type: ${item}`);
}
