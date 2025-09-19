import { RawCoList, RawCoMap } from "cojson";
import {
  Account,
  CoDiscriminatedUnionSchema,
  CoFeed,
  CoFeedSchema,
  CoList,
  CoListSchema,
  CoMap,
  CoPlainText,
  CoRichText,
  CoValueClass,
  FileStream,
  FileStreamSchema,
  PlainTextSchema,
  SchemaUnion,
  enrichAccountSchema,
  enrichCoMapSchema,
  isCoValueClass,
  Group,
} from "../../../internal.js";
import { coField } from "../../schema.js";

import { CoreCoValueSchema } from "../schemaTypes/CoValueSchema.js";
import { RichTextSchema } from "../schemaTypes/RichTextSchema.js";
import { GroupSchema } from "../schemaTypes/GroupSchema.js";
import { schemaUnionDiscriminatorFor } from "../unionUtils.js";
import {
  AnyCoreCoValueSchema,
  AnyZodOrCoValueSchema,
  CoValueClassFromAnySchema,
  CoValueClassOrSchema,
  CoValueSchemaFromCoreSchema,
} from "../zodSchema.js";
import {
  SchemaField,
  schemaFieldToCoFieldDef,
} from "./schemaFieldToCoFieldDef.js";

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
  schema: AnyZodOrCoValueSchema | CoValueClass,
): schema is CoValueSchemaFromCoreSchema<AnyCoreCoValueSchema> {
  return isAnyCoValueSchema(schema) && "getCoValueClass" in schema;
}

/**
 * Convert a "core" CoValue schema into a CoValue schema.
 * See {@link CoreCoValueSchema} for more information.
 *
 * @returns The CoValue schema matching the provided CoreCoValueSchema
 */
export function hydrateCoreCoValueSchema<S extends AnyCoreCoValueSchema>(
  schema: S,
): CoValueSchemaFromCoreSchema<S> {
  if (isCoValueSchema(schema)) {
    // If the schema is already a CoValue schema, return it as is
    return schema as any;
  }

  if (schema.builtin === "CoOptional") {
    throw new Error(
      `co.optional() of collaborative types is not supported as top-level schema: ${JSON.stringify(schema)}`,
    );
  } else if (schema.builtin === "CoMap" || schema.builtin === "Account") {
    const def = schema.getDefinition();
    const ClassToExtend = schema.builtin === "Account" ? Account : CoMap;

    const coValueClass = class ZCoMap extends ClassToExtend {
      constructor(options: { fromRaw: RawCoMap } | undefined) {
        super(options);
        for (const [fieldName, fieldType] of Object.entries(def.shape)) {
          (this as any)[fieldName] = schemaFieldToCoFieldDef(
            fieldType as SchemaField,
          );
        }
        if (def.catchall) {
          (this as any)[coField.items] = schemaFieldToCoFieldDef(
            def.catchall as SchemaField,
          );
        }
      }
    };

    const coValueSchema =
      ClassToExtend === Account
        ? enrichAccountSchema(schema as any, coValueClass as any)
        : enrichCoMapSchema(schema as any, coValueClass as any);

    return coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoList") {
    const element = schema.element;
    const coValueClass = class ZCoList extends CoList {
      constructor(options: { fromRaw: RawCoList } | undefined) {
        super(options);
        (this as any)[coField.items] = schemaFieldToCoFieldDef(
          element as SchemaField,
        );
      }
    };

    const coValueSchema = new CoListSchema(element, coValueClass as any);

    return coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoFeed") {
    const coValueClass = CoFeed.Of(
      schemaFieldToCoFieldDef(schema.element as SchemaField),
    );
    const coValueSchema = new CoFeedSchema(schema.element, coValueClass);
    return coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "FileStream") {
    const coValueClass = FileStream;
    return new FileStreamSchema(coValueClass) as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoPlainText") {
    const coValueClass = CoPlainText;
    return new PlainTextSchema(coValueClass) as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoRichText") {
    const coValueClass = CoRichText;
    return new RichTextSchema(coValueClass) as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoDiscriminatedUnion") {
    const coValueClass = SchemaUnion.Of(schemaUnionDiscriminatorFor(schema));
    const coValueSchema = new CoDiscriminatedUnionSchema(schema, coValueClass);
    return coValueSchema as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "Group") {
    return new GroupSchema() as CoValueSchemaFromCoreSchema<S>;
  } else {
    const notReachable: never = schema;
    throw new Error(
      `Unsupported zod CoValue type for top-level schema: ${JSON.stringify(notReachable, undefined, 2)}`,
    );
  }
}

/**
 * Convert a CoValue class or a CoValue schema into a CoValue class.
 *
 * This function bridges the gap between CoValue classes created with the class syntax,
 * and CoValue classes created with our `co.` definers.
 *
 * @param schema A CoValue class or a CoValue schema
 * @returns The same CoValue class, or a CoValue class that matches the provided schema
 */
export function coValueClassFromCoValueClassOrSchema<
  S extends CoValueClassOrSchema,
>(schema: S): CoValueClassFromAnySchema<S> {
  if (isCoValueClass(schema)) {
    return schema as any;
  } else if (isCoValueSchema(schema)) {
    return schema.getCoValueClass() as any;
  }

  throw new Error(`Unsupported schema: ${JSON.stringify(schema)}`);
}
