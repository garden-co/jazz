import { CoMap, CoValueClass, isCoValueClass } from "../../../internal.js";
import { coField } from "../../schema.js";
import {
  isUnionOfCoMapsDeeply,
  isUnionOfPrimitivesDeeply,
  schemaUnionDiscriminatorFor,
} from "../unionUtils.js";
import {
  ZodCatch,
  ZodDefault,
  ZodLazy,
  ZodReadonly,
  z,
} from "../zodReExport.js";
import { ZodPrimitiveSchema } from "../zodSchema.js";
import { zodSchemaToCoSchemaOrKeepPrimitive } from "./zodSchemaToCoSchema.js";

type FieldSchema =
  | CoValueClass
  | ZodPrimitiveSchema
  | z.core.$ZodOptional<z.core.$ZodType>
  | z.core.$ZodUnion<z.core.$ZodType[]>
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
  | z.core.$ZodCatch<z.core.$ZodType>
  | (z.core.$ZodCustom<any, any> & { builtin: any });

export function zodFieldToCoFieldDef(schema: FieldSchema) {
  if (isCoValueClass(schema)) {
    return coField.ref(schema);
  } else {
    if ("_zod" in schema) {
      if (schema._zod.def.type === "optional") {
        const inner = zodSchemaToCoSchemaOrKeepPrimitive(
          schema._zod.def.innerType,
        );
        if (isCoValueClass(inner)) {
          return coField.ref(inner, { optional: true });
        } else {
          return zodFieldToCoFieldDef(inner);
        }
      } else if (schema._zod.def.type === "string") {
        return coField.string;
      } else if (schema._zod.def.type === "number") {
        return coField.number;
      } else if (schema._zod.def.type === "boolean") {
        return coField.boolean;
      } else if (schema._zod.def.type === "null") {
        return coField.null;
      } else if (schema._zod.def.type === "enum") {
        return coField.string;
      } else if (schema._zod.def.type === "readonly") {
        return zodFieldToCoFieldDef(
          (schema as unknown as ZodReadonly).def.innerType as FieldSchema,
        );
      } else if (schema._zod.def.type === "date") {
        return coField.optional.Date;
      } else if (schema._zod.def.type === "template_literal") {
        return coField.string;
      } else if (schema._zod.def.type === "lazy") {
        // Mostly to support z.json()
        return zodFieldToCoFieldDef(
          (schema as unknown as ZodLazy).unwrap() as FieldSchema,
        );
      } else if (
        schema._zod.def.type === "default" ||
        schema._zod.def.type === "catch"
      ) {
        console.warn(
          "z.default()/z.catch() are not supported in collaborative schemas. They will be ignored.",
        );

        return zodFieldToCoFieldDef(
          (schema as unknown as ZodDefault | ZodCatch).def
            .innerType as FieldSchema,
        );
      } else if (schema._zod.def.type === "literal") {
        if (
          schema._zod.def.values.some(
            (literal) => typeof literal === "undefined",
          )
        ) {
          throw new Error("z.literal() with undefined is not supported");
        }
        if (schema._zod.def.values.some((literal) => literal === null)) {
          throw new Error("z.literal() with null is not supported");
        }
        if (
          schema._zod.def.values.some((literal) => typeof literal === "bigint")
        ) {
          throw new Error("z.literal() with bigint is not supported");
        }
        return coField.literal(
          ...(schema._zod.def.values as Exclude<
            (typeof schema._zod.def.values)[number],
            undefined | null | bigint
          >[]),
        );
      } else if (
        schema._zod.def.type === "object" ||
        schema._zod.def.type === "array" ||
        schema._zod.def.type === "tuple"
      ) {
        return coField.json();
      } else if (schema._zod.def.type === "custom") {
        if ("builtin" in schema) {
          return zodFieldToCoFieldDef(schema.builtin);
        } else {
          throw new Error(`Unsupported custom zod type`);
        }
      } else if (schema._zod.def.type === "union") {
        if (isUnionOfPrimitivesDeeply(schema)) {
          return coField.json();
        } else if (isUnionOfCoMapsDeeply(schema)) {
          return coField.ref<CoValueClass<CoMap>>(
            schemaUnionDiscriminatorFor(schema),
          );
        } else {
          throw new Error(
            "z.union()/z.discriminatedUnion() of mixed collaborative and non-collaborative types is not supported",
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
