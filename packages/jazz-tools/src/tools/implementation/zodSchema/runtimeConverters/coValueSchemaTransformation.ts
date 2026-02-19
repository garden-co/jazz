import {
  Account as AccountClass,
  AccountSchema,
  CoDiscriminatedUnionSchema,
  CoFeed as CoFeedClass,
  CoFeedSchema,
  CoList as CoListClass,
  CoListSchema,
  CoMap as CoMapClass,
  CoMapSchema,
  CoPlainText as CoPlainTextClass,
  CoRichText as CoRichTextClass,
  CoValueClass,
  FileStream as FileStreamClass,
  FileStreamSchema,
  CoVectorSchema,
  PlainTextSchema,
  schemaUnionClassFromDiscriminator,
  isCoValueClass,
  CoVector as CoVectorClass,
} from "../../../internal.js";

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
  } else if (schema.builtin === "CoMap") {
    const coValueClass = class _CoMap extends CoMapClass {};
    coValueClass.coValueSchema = new CoMapSchema(
      schema as any,
      coValueClass as any,
    );

    return coValueClass.coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "Account") {
    const coValueClass = class _Account extends AccountClass {};
    coValueClass.coValueSchema = new AccountSchema(
      schema as any,
      coValueClass as any,
    );

    return coValueClass.coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoList") {
    const element = schema.element;
    const coValueClass = class _CoList extends CoListClass {};
    coValueClass.coValueSchema = new CoListSchema(element, coValueClass as any);

    return coValueClass.coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoFeed") {
    const coValueClass = class _CoFeed extends CoFeedClass {};
    coValueClass.coValueSchema = new CoFeedSchema(
      schema.element,
      coValueClass as any,
    );
    return coValueClass.coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "FileStream") {
    const coValueClass = class _FileStream extends FileStreamClass {};
    coValueClass.coValueSchema = new FileStreamSchema(coValueClass as any);
    return coValueClass.coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoVector") {
    const dimensions = schema.dimensions;

    const coValueClass = class _CoVector extends CoVectorClass {
      protected static requiredDimensionsCount = dimensions;
    };
    coValueClass.coValueSchema = new CoVectorSchema(
      dimensions,
      coValueClass as any,
    );

    return coValueClass.coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoPlainText") {
    const coValueClass = class _CoPlainText extends CoPlainTextClass {};
    coValueClass.coValueSchema = new PlainTextSchema(coValueClass as any);
    return coValueClass.coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoRichText") {
    const coValueClass = class _CoRichText extends CoRichTextClass {};
    coValueClass.coValueSchema = new RichTextSchema(coValueClass as any);
    return coValueClass.coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoDiscriminatedUnion") {
    const coValueClass = schemaUnionClassFromDiscriminator(
      schemaUnionDiscriminatorFor(schema),
    );
    coValueClass.coValueSchema = new CoDiscriminatedUnionSchema(
      schema,
      coValueClass,
    );
    return coValueClass.coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
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
