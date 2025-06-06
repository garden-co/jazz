import { RawCoList, RawCoMap } from "cojson";
import {
  Account,
  CoFeed,
  CoList,
  CoMap,
  CoPlainText,
  CoRichText,
  CoValueClass,
  FileStream,
  SchemaUnion,
  isCoValueClass,
} from "../../../internal.js";
import { coField } from "../../schema.js";
import {
  isUnionOfCoMapsDeeply,
  schemaUnionDiscriminatorFor,
} from "../unionUtils.js";
import { z } from "../zodReExport.js";
import {
  CoValueClassFromZodSchema,
  ZodPrimitiveSchema,
  getDef,
  isZodArray,
  isZodCustom,
  isZodObject,
} from "../zodSchema.js";
import { zodFieldToCoFieldDef } from "./zodFieldToCoFieldDef.js";

let coSchemasForZodSchemas = new Map<z.core.$ZodType, CoValueClass>();

export function tryZodSchemaToCoSchema<S extends z.core.$ZodType>(
  schema: S,
): CoValueClassFromZodSchema<S> | null {
  if ("collaborative" in schema && schema.collaborative) {
    if (coSchemasForZodSchemas.has(schema)) {
      return coSchemasForZodSchemas.get(schema) as CoValueClassFromZodSchema<S>;
    }

    if (isZodObject(schema)) {
      const def = getDef(schema);

      const ClassToExtend =
        "builtin" in schema && schema.builtin === "Account" ? Account : CoMap;

      const coSchema = class ZCoMap extends ClassToExtend {
        constructor(options: { fromRaw: RawCoMap } | undefined) {
          super(options);
          for (const [field, fieldType] of Object.entries(
            def.shape as z.core.$ZodShape,
          )) {
            (this as any)[field] = zodFieldToCoFieldDef(
              zodSchemaToCoSchemaOrKeepPrimitive(fieldType),
            );
          }
          if (def.catchall) {
            (this as any)[coField.items] = zodFieldToCoFieldDef(
              zodSchemaToCoSchemaOrKeepPrimitive(def.catchall),
            );
          }
        }
      };

      coSchemasForZodSchemas.set(schema, coSchema as unknown as CoValueClass);
      return coSchema as unknown as CoValueClassFromZodSchema<S>;
    } else if (isZodArray(schema)) {
      const def = getDef(schema);
      const coSchema = class ZCoList extends CoList {
        constructor(options: { fromRaw: RawCoList } | undefined) {
          super(options);
          (this as any)[coField.items] = zodFieldToCoFieldDef(
            zodSchemaToCoSchemaOrKeepPrimitive(def.element),
          );
        }
      };

      coSchemasForZodSchemas.set(schema, coSchema);
      return coSchema as unknown as CoValueClassFromZodSchema<S>;
    } else if (isZodCustom(schema)) {
      if ("builtin" in schema) {
        if (schema.builtin === "CoFeed" && "element" in schema) {
          return CoFeed.Of(
            zodFieldToCoFieldDef(
              zodSchemaToCoSchemaOrKeepPrimitive(
                schema.element as z.core.$ZodType,
              ),
            ),
          ) as unknown as CoValueClassFromZodSchema<S>;
        } else if (schema.builtin === "FileStream") {
          return FileStream as unknown as CoValueClassFromZodSchema<S>;
        } else if (schema.builtin === "CoPlainText") {
          return CoPlainText as unknown as CoValueClassFromZodSchema<S>;
        } else if (schema.builtin === "CoRichText") {
          return CoRichText as unknown as CoValueClassFromZodSchema<S>;
        } else {
          throw new Error(`Unsupported builtin type: ${schema.builtin}`);
        }
      } else {
        throw new Error(`Unsupported custom zod type`);
      }
    } else {
      throw new Error(
        `Unsupported zod CoValue type for top-level schema: ${schema._zod?.def?.type || JSON.stringify(schema, undefined, 2)}`,
      );
    }
  } else if (schema instanceof z.core.$ZodDiscriminatedUnion) {
    if (isUnionOfCoMapsDeeply(schema)) {
      return SchemaUnion.Of(
        schemaUnionDiscriminatorFor(schema),
      ) as unknown as CoValueClassFromZodSchema<S>;
    } else {
      throw new Error(
        "z.discriminatedUnion() of non-collaborative types is not supported as a top-level schema",
      );
    }
  } else {
    return null;
  }
}

export function zodSchemaToCoSchema<
  S extends
    | z.core.$ZodType
    | (z.core.$ZodObject<any, any> & {
        builtin: "Account";
        migration?: (account: any, creationProps?: { name: string }) => void;
      })
    | (z.core.$ZodCustom<any, any> & { builtin: "FileStream" })
    | (z.core.$ZodCustom<any, any> & {
        builtin: "CoFeed";
        element: z.core.$ZodType;
      }),
>(schema: S): CoValueClassFromZodSchema<S> {
  const coSchema = tryZodSchemaToCoSchema(schema);
  if (!coSchema) {
    throw new Error(
      `Unsupported zod type: ${schema._zod?.def?.type || JSON.stringify(schema)}`,
    );
  }
  return coSchema;
}

export function anySchemaToCoSchema<
  S extends
    | CoValueClass
    | z.core.$ZodType
    | (z.core.$ZodObject<any, any> & {
        builtin: "Account";
        migration?: (account: any, creationProps?: { name: string }) => void;
      })
    | (z.core.$ZodCustom<any, any> & { builtin: "FileStream" })
    | (z.core.$ZodCustom<any, any> & {
        builtin: "CoFeed";
        element: z.core.$ZodType;
      }),
>(
  schema: S,
): S extends CoValueClass
  ? S
  : S extends z.core.$ZodType
    ? CoValueClassFromZodSchema<S>
    : never {
  if (isCoValueClass(schema)) {
    return schema as any;
  } else if ("getCoSchema" in schema) {
    return (schema as any).getCoSchema() as any;
  } else if ("def" in schema) {
    const coSchema = tryZodSchemaToCoSchema(schema as z.core.$ZodType);
    if (!coSchema) {
      throw new Error(
        `Unsupported zod type: ${(schema.def as any)?.type || JSON.stringify(schema)}`,
      );
    }
    return coSchema as any;
  }

  throw new Error(`Unsupported schema: ${JSON.stringify(schema)}`);
}

export function zodSchemaToCoSchemaOrKeepPrimitive<S extends z.core.$ZodType>(
  schema: S,
): CoValueClassFromZodSchema<S> | ZodPrimitiveSchema {
  const coSchema = tryZodSchemaToCoSchema(schema);
  if (!coSchema) {
    return schema as any;
  }
  return coSchema;
}
