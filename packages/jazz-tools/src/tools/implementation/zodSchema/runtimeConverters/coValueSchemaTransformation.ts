import { RawCoList, RawCoMap } from "cojson";
import {
  Account,
  AccountSchema,
  CoDiscriminatedUnionSchema,
  CoFeed,
  CoFeedSchema,
  CoList,
  CoListSchema,
  CoMap,
  CoMapSchema,
  CoPlainText,
  CoRichText,
  CoValueClass,
  FileStream,
  FileStreamSchema,
  CoVectorSchema,
  PlainTextSchema,
  SchemaUnion,
  isCoValueClass,
  Group,
  CoVector,
  CoMapFieldSchema,
  ItemsMarker,
} from "../../../internal.js";

import { CoreCoValueSchema } from "../schemaTypes/CoValueSchema.js";
import { RichTextSchema } from "../schemaTypes/RichTextSchema.js";
import { GroupSchema } from "../schemaTypes/GroupSchema.js";
import { schemaUnionDiscriminatorFor } from "../unionUtils.js";
import {
  AnyCoreCoValueSchema,
  AnyZodOrCoValueSchema,
  CoValueSchemaFromCoreSchema,
} from "../zodSchema.js";
import {
  SchemaField,
  schemaFieldToFieldDescriptor,
} from "./schemaFieldToFieldDescriptor.js";

// Note: if you're editing this function, edit the `isAnyCoValueSchema`
// function in `zodReExport.ts` as well
export function isAnyCoValueSchema(
  schema: unknown,
): schema is AnyCoreCoValueSchema {
  return (
    typeof schema === "object" &&
    schema !== null &&
    "collaborative" in schema &&
    schema.collaborative === true
  );
}

export function isCoValueSchema(
  schema: AnyZodOrCoValueSchema,
): schema is CoValueSchemaFromCoreSchema<AnyCoreCoValueSchema> {
  return isAnyCoValueSchema(schema);
}

/**
 * Convert a "core" CoValue schema into a CoValue schema.
 * See {@link CoreCoValueSchema} for more information.
 *
 * @returns The CoValue schema matching the provided CoreCoValueSchema
 */
export function asConstructable<S extends AnyCoreCoValueSchema>(
  schema: S,
): CoValueSchemaFromCoreSchema<S> {
  if (isCoValueSchema(schema)) {
    // If the schema is already a CoValue schema, return it as is
    return schema as any;
  } else {
    throw new Error("Schema is not a CoValue schema");
  }
}
