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
  | z.core.$ZodUnion<z.core.$ZodType[]>
  | z.core.$ZodDiscriminatedUnion<z.core.$ZodType[]>
  | z.core.$ZodObject<z.core.$ZodLooseShape>
  | z.core.$ZodArray<z.core.$ZodType>
  | z.core.$ZodTuple<z.core.$ZodType[]>
  | z.core.$ZodReadonly<z.core.$ZodType>
  | z.core.$ZodLazy<z.core.$ZodType>
  | z.core.$ZodTemplateLiteral<any>
  | z.core.$ZodLiteral<any>
  | z.core.$ZodCatch<z.core.$ZodType>
  | z.core.$ZodEnum<any>
  | z.core.$ZodDefault<z.core.$ZodType>
  | z.core.$ZodCatch<z.core.$ZodType>;

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
      if (zodSchemaDef.type === "optional") {
        const inner = zodSchemaDef.innerType as ZodPrimitiveSchema;
        return schemaFieldToCoFieldDef(inner);
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
        zodSchemaDef.type === "array" ||
        zodSchemaDef.type === "tuple"
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
