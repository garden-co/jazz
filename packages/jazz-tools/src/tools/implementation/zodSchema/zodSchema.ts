import { LocalNode, RawAccount } from "cojson";
import {
  Account,
  AccountClass,
  CoValueClass,
  CoValueFromRaw,
  InstanceOfSchema,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
} from "../../internal.js";
import {
  AccountSchema,
  AnyAccountSchema,
  BaseAccountShape,
} from "./schemaTypes/AccountSchema.js";
import { CoDiscriminatedUnionSchema } from "./schemaTypes/CoDiscriminatedUnionSchema.js";
import { AnyCoFeedSchema, CoFeedSchema } from "./schemaTypes/CoFeedSchema.js";
import { AnyCoListSchema, CoListSchema } from "./schemaTypes/CoListSchema.js";
import {
  AnyCoMapSchema,
  CoMapInitZod,
  CoMapSchema,
} from "./schemaTypes/CoMapSchema.js";
import { AnyCoOptionalSchema } from "./schemaTypes/CoOptionalSchema.js";
import {
  AnyCoRecordSchema,
  CoRecordSchema,
} from "./schemaTypes/CoRecordSchema.js";
import {
  AnyFileStreamSchema,
  FileStreamSchema,
} from "./schemaTypes/FileStreamSchema.js";
import {
  AnyPlainTextSchema,
  PlainTextSchema,
} from "./schemaTypes/PlainTextSchema.js";
import {
  AnyRichTextSchema,
  RichTextSchema,
} from "./schemaTypes/RichTextSchema.js";
import { InstanceOfSchemaCoValuesNullable } from "./typeConverters/InstanceOfSchemaCoValuesNullable.js";
import { z } from "./zodReExport.js";

// defining an extra type for this, otherwise BaseSchema & {...} often
// gets expanded into a n inferred type that's too long for typescript to print
export type WithHelpers<
  Base extends z.core.$ZodType,
  Helpers extends object,
> = Base & Helpers;

export type ZodPrimitiveSchema =
  | z.core.$ZodString
  | z.core.$ZodNumber
  | z.core.$ZodBoolean
  | z.core.$ZodNull
  | z.core.$ZodDate
  | z.core.$ZodLiteral;

export type AnyCoUnionSchema = z.core.$ZodDiscriminatedUnion<
  (
    | (z.core.$ZodType & { collaborative: true })
    | z.core.$ZodDiscriminatedUnion
  )[]
>;

// this is a series of hacks to work around z4 removing _zod at runtime from z.core.$ZodType
export function isZodObject(
  schema: z.core.$ZodType,
): schema is z.ZodObject<any, any> {
  return (schema as any).def?.type === "object";
}

export function isZodArray(
  schema: z.core.$ZodType,
): schema is z.core.$ZodArray<any> {
  return (schema as any).def?.type === "array";
}

export function isZodCustom(
  schema: z.core.$ZodType,
): schema is z.core.$ZodCustom<any, any> {
  return (schema as any).def?.type === "custom";
}

export function getDef<S extends z.core.$ZodType>(schema: S): S["_zod"]["def"] {
  return (schema as any).def;
}

// TODO rename. This represents a CoValue class or a CoValue schema
export type CoValueOrZodSchema = CoValueClass | AnyCoSchema;

// TODO rename to CoValueSchemaFromCoProtoSchema
export type CoValueSchemaFromZodSchema<S extends z.core.$ZodType> =
  S extends z.core.$ZodType
    ? S extends AnyAccountSchema<infer Shape extends BaseAccountShape>
      ? AccountSchema<Shape>
      : S extends AnyCoRecordSchema<infer K, infer V>
        ? CoRecordSchema<K, V>
        : S extends AnyCoMapSchema<infer Shape, infer Config>
          ? CoMapSchema<Shape, Config>
          : S extends AnyCoListSchema<infer T>
            ? CoListSchema<T>
            : S extends AnyCoFeedSchema<infer T>
              ? CoFeedSchema<T>
              : S extends AnyPlainTextSchema
                ? PlainTextSchema
                : S extends AnyRichTextSchema
                  ? RichTextSchema
                  : S extends AnyFileStreamSchema
                    ? FileStreamSchema
                    : S extends z.core.$ZodOptional<infer Inner>
                      ? CoValueSchemaFromZodSchema<Inner>
                      : S extends z.core.$ZodUnion<
                            infer Members extends readonly [
                              z.core.$ZodTypeDiscriminable,
                              ...z.core.$ZodTypeDiscriminable[],
                            ]
                          >
                        ? CoDiscriminatedUnionSchema<Members>
                        : never
    : never;

export type CoValueClassFromAnySchema<S extends CoValueOrZodSchema> =
  S extends CoValueClass<any>
    ? S
    : CoValueClass<InstanceOfSchema<S>> &
        CoValueFromRaw<InstanceOfSchema<S>> &
        (S extends AnyAccountSchema ? AccountClassEssentials : {});

type AccountClassEssentials = {
  fromRaw: <A extends Account>(this: AccountClass<A>, raw: RawAccount) => A;
  fromNode: <A extends Account>(this: AccountClass<A>, node: LocalNode) => A;
};

// TODO rename to ProtoCoSchema?
export type AnyCoSchema =
  | AnyCoMapSchema
  | AnyAccountSchema
  | AnyCoRecordSchema
  | AnyCoListSchema
  | AnyCoFeedSchema
  | AnyCoUnionSchema
  | AnyCoOptionalSchema
  | AnyPlainTextSchema
  | AnyRichTextSchema
  | AnyFileStreamSchema;

export type Loaded<
  T extends CoValueClass | AnyCoSchema,
  R extends ResolveQuery<T> = true,
> = Resolved<NonNullable<InstanceOfSchemaCoValuesNullable<T>>, R>;

export type ResolveQuery<T extends CoValueClass | AnyCoSchema> = RefsToResolve<
  NonNullable<InstanceOfSchemaCoValuesNullable<T>>
>;

export type ResolveQueryStrict<
  T extends CoValueClass | AnyCoSchema,
  R extends ResolveQuery<T>,
> = RefsToResolveStrict<NonNullable<InstanceOfSchemaCoValuesNullable<T>>, R>;

export type InitFor<T extends CoValueClass | AnyCoSchema> =
  T extends AnyCoMapSchema<infer Shape> ? Simplify<CoMapInitZod<Shape>> : never;
