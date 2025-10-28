import type { JsonValue } from "cojson";
import { CoValueClass, isCoValueClass } from "../../../internal.js";
import { coField } from "../../schema.js";
import { CoreCoValueSchema } from "../schemaTypes/CoValueSchema.js";
import { isUnionOfPrimitivesDeeply } from "../unionUtils.js";
import {
  ZodCatch,
  ZodDefault,
  ZodLazy,
  ZodReadonly,
  z,
} from "../zodReExport.js";
import { ZodPrimitiveSchema } from "../zodSchema.js";
import { isCoValueSchema } from "./coValueSchemaTransformation.js";

/**
 * Types of objects that can be nested inside CoValue schema containers
 */
export type SchemaField =
  // Schemas created with co.map(), co.record(), co.list(), etc.
  | CoreCoValueSchema
  // CoValue classes created with class syntax, or framework-provided classes like Group
  | CoValueClass
  | ZodPrimitiveSchema
  | z.core.$ZodOptional<z.core.$ZodType>
  | z.core.$ZodNullable<z.core.$ZodType>
  | z.core.$ZodUnion<z.core.$ZodType[]>
  | z.core.$ZodDiscriminatedUnion<z.core.$ZodType[]>
  | z.core.$ZodIntersection<z.core.$ZodType, z.core.$ZodType>
  | z.core.$ZodObject<z.core.$ZodLooseShape>
  | z.core.$ZodRecord<z.core.$ZodRecordKey, z.core.$ZodType>
  | z.core.$ZodArray<z.core.$ZodType>
  | z.core.$ZodTuple<z.core.$ZodType[]>
  | z.core.$ZodReadonly<z.core.$ZodType>
  | z.core.$ZodLazy<z.core.$ZodType>
  | z.core.$ZodTemplateLiteral<any>
  | z.core.$ZodLiteral<any>
  | z.core.$ZodEnum<any>
  | z.core.$ZodCodec<z.core.$ZodType, z.core.$ZodType>
  | z.core.$ZodDefault<z.core.$ZodType>
  | z.core.$ZodCatch<z.core.$ZodType>;

function makeCodecCoField(
  codec: z.core.$ZodCodec<z.core.$ZodType, z.core.$ZodType>,
) {
  return coField.optional.encoded({
    encode: (value: any) => {
      if (value === undefined) return undefined as unknown as JsonValue;
      if (value === null) return null;
      return codec._zod.def.reverseTransform(value, {
        value,
        issues: [],
      }) as JsonValue;
    },
    decode: (value) => {
      if (value === null) return null;
      if (value === undefined) return undefined;
      return codec._zod.def.transform(value, { value, issues: [] });
    },
  });
}

export function schemaFieldToCoFieldDef(schema: SchemaField) {
  if (isCoValueClass(schema)) {
    return coField.ref(schema);
  } else if (isCoValueSchema(schema)) {
    if (schema.builtin === "CoOptional") {
      return coField.ref(schema.getCoValueClass(), {
        optional: true,
      });
    }
    return coField.ref(schema.getCoValueClass());
  } else {
    if ("_zod" in schema) {
      const zodSchemaDef = schema._zod.def;
      if (
        zodSchemaDef.type === "optional" ||
        zodSchemaDef.type === "nullable"
      ) {
        const inner = zodSchemaDef.innerType as SchemaField;
        const coFieldDef: any = schemaFieldToCoFieldDef(inner);
        if (
          zodSchemaDef.type === "nullable" &&
          coFieldDef === coField.optional.Date
        ) {
          // We do not currently have a way to encode null Date coFields.
          // We only support encoding optional (i.e. Date | undefined) coFields.
          throw new Error("Nullable z.date() is not supported");
        }
        // Primitive coField types support null and undefined as values,
        // so we can just return the inner type here and rely on support
        // for null/undefined at the type level
        return coFieldDef;
      } else if (zodSchemaDef.type === "string") {
        return coField.string;
      } else if (zodSchemaDef.type === "number") {
        return coField.number;
      } else if (zodSchemaDef.type === "boolean") {
        return coField.boolean;
      } else if (zodSchemaDef.type === "null") {
        return coField.null;
      } else if (zodSchemaDef.type === "enum") {
        return coField.string;
      } else if (zodSchemaDef.type === "readonly") {
        return schemaFieldToCoFieldDef(
          (schema as unknown as ZodReadonly).def.innerType as SchemaField,
        );
      } else if (zodSchemaDef.type === "date") {
        return coField.optional.Date;
      } else if (zodSchemaDef.type === "template_literal") {
        return coField.string;
      } else if (zodSchemaDef.type === "lazy") {
        // Mostly to support z.json()
        return schemaFieldToCoFieldDef(
          (schema as unknown as ZodLazy).unwrap() as SchemaField,
        );
      } else if (
        zodSchemaDef.type === "default" ||
        zodSchemaDef.type === "catch"
      ) {
        console.warn(
          "z.default()/z.catch() are not supported in collaborative schemas. They will be ignored.",
        );

        return schemaFieldToCoFieldDef(
          (schema as unknown as ZodDefault | ZodCatch).def
            .innerType as SchemaField,
        );
      } else if (zodSchemaDef.type === "literal") {
        if (
          zodSchemaDef.values.some((literal) => typeof literal === "undefined")
        ) {
          throw new Error("z.literal() with undefined is not supported");
        }
        if (zodSchemaDef.values.some((literal) => literal === null)) {
          throw new Error("z.literal() with null is not supported");
        }
        if (
          zodSchemaDef.values.some((literal) => typeof literal === "bigint")
        ) {
          throw new Error("z.literal() with bigint is not supported");
        }
        return coField.literal(
          ...(zodSchemaDef.values as Exclude<
            (typeof zodSchemaDef.values)[number],
            undefined | null | bigint
          >[]),
        );
      } else if (
        zodSchemaDef.type === "object" ||
        zodSchemaDef.type === "record" ||
        zodSchemaDef.type === "array" ||
        zodSchemaDef.type === "tuple" ||
        zodSchemaDef.type === "intersection"
      ) {
        return coField.json();
      } else if (zodSchemaDef.type === "union") {
        if (isUnionOfPrimitivesDeeply(schema)) {
          return coField.json();
        } else {
          throw new Error(
            "z.union()/z.discriminatedUnion() of collaborative types is not supported. Use co.discriminatedUnion() instead.",
          );
        }
      } else if (zodSchemaDef.type === "pipe") {
        const isCodec =
          zodSchemaDef.transform !== undefined &&
          zodSchemaDef.reverseTransform !== undefined;

        if (!isCodec) {
          throw new Error(
            "z.pipe() is not supported. Only z.codec() is supported.",
          );
        }

        try {
          schemaFieldToCoFieldDef(zodSchemaDef.in as SchemaField);
        } catch (error) {
          if (error instanceof Error) {
            error.message = `z.codec() is only supported if the input schema is already supported. ${error.message}`;
          }

          throw error;
        }

        return makeCodecCoField(
          schema as z.core.$ZodCodec<z.core.$ZodType, z.core.$ZodType>,
        );
      } else {
        throw new Error(
          `Unsupported zod type: ${(schema._zod?.def as any)?.type || JSON.stringify(schema)}`,
        );
      }
    } else {
      throw new Error(`Unsupported zod type: ${schema}`);
    }
  }
}
