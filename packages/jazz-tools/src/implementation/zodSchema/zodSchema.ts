import { LocalNode, RawAccount } from "cojson";
import {
  Account,
  AccountClass,
  CoValue,
  CoValueClass,
  CoValueFromRaw,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
} from "../../internal.js";
import { AnyAccountSchema } from "./schemaTypes/AccountSchema.js";
import { AnyCoFeedSchema } from "./schemaTypes/CoFeedSchema.js";
import { AnyCoListSchema } from "./schemaTypes/CoListSchema.js";
import { AnyCoMapSchema, CoMapInitZod } from "./schemaTypes/CoMapSchema.js";
import { AnyCoRecordSchema } from "./schemaTypes/CoRecordSchema.js";
import { FileStreamSchema } from "./schemaTypes/FileStreamSchema.js";
import { PlainTextSchema } from "./schemaTypes/PlainTextSchema.js";
import { RichTextSchema } from "./schemaTypes/RichTextSchema.js";
import { InstanceOfSchema } from "./typeConverters/InstanceOfSchema.js";
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
): schema is z.core.$ZodObject<any, any> {
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

export type CoValueOrZodSchema = CoValueClass | AnyCoSchema;

export type CoValueClassFromZodSchema<S extends z.core.$ZodType> = CoValueClass<
  InstanceOfSchema<S>
> &
  CoValueFromRaw<InstanceOfSchema<S>> &
  (S extends AnyAccountSchema ? AccountClassEssentials : {});

type AccountClassEssentials = {
  fromRaw: <A extends Account>(this: AccountClass<A>, raw: RawAccount) => A;
  fromNode: <A extends Account>(this: AccountClass<A>, node: LocalNode) => A;
};

export type AnyCoSchema =
  | AnyCoMapSchema
  | AnyAccountSchema
  | AnyCoRecordSchema
  | AnyCoListSchema
  | AnyCoFeedSchema
  | AnyCoUnionSchema
  | PlainTextSchema
  | RichTextSchema
  | FileStreamSchema;

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
