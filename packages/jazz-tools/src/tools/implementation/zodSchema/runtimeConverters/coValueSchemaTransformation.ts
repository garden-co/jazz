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
  CoValueClassFromAnySchema,
  CoValueClassOrSchema,
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

    let coValueClass: typeof Account | typeof CoMap;

    let cachedFields: CoMapFieldSchema;

    const getFields = () => {
      if (cachedFields) return cachedFields;
      const fields = Object.fromEntries(
        Object.entries(def.shape).map(([fieldName, fieldType]) => [
          fieldName,
          schemaFieldToFieldDescriptor(fieldType as SchemaField),
        ]),
      );
      if (def.catchall) {
        fields[ItemsMarker] = schemaFieldToFieldDescriptor(
          def.catchall as SchemaField,
        );
      }
      cachedFields = fields;
      return fields;
    };

    if (schema.builtin === "Account") {
      coValueClass = class ZAccount extends Account {
        // lazy to allow for shape to have circular references
        static get fields() {
          return getFields() as any;
        }
      };
    } else {
      coValueClass = class ZCoMap extends CoMap {
        // lazy to allow for shape to have circular references
        static get fields() {
          return getFields();
        }
      };
    }

    const coValueSchema =
      schema.builtin === "Account"
        ? new AccountSchema(schema as any, coValueClass as any)
        : new CoMapSchema(schema as any, coValueClass as any);

    return coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoList") {
    return new CoListSchema(
      schema.element,
      CoList,
    ) as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoFeed") {
    return new CoFeedSchema(
      schema.element,
      CoFeed,
    ) as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "FileStream") {
    const coValueClass = FileStream;
    return new FileStreamSchema(coValueClass) as CoValueSchemaFromCoreSchema<S>;
  } else if (schema.builtin === "CoVector") {
    const dimensions = schema.dimensions;

    const coValueClass = class CoVectorWithDimensions extends CoVector {
      protected static requiredDimensionsCount = dimensions;
    };

    return new CoVectorSchema(
      dimensions,
      coValueClass,
    ) as CoValueSchemaFromCoreSchema<S>;
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
