import { LocalNode, RawAccount } from "cojson";
import {
  Account,
  AccountClass,
  CoRecordSchema,
  CoValueClass,
  CoValueFromRaw,
  CoreCoRecordSchema,
  InstanceOfSchema,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
} from "../../internal.js";
import {
  AccountSchema,
  BaseAccountShape,
  CoreAccountSchema,
} from "./schemaTypes/AccountSchema.js";
import {
  CoDiscriminatedUnionSchema,
  CoreCoDiscriminatedUnionSchema,
} from "./schemaTypes/CoDiscriminatedUnionSchema.js";
import { CoFeedSchema, CoreCoFeedSchema } from "./schemaTypes/CoFeedSchema.js";
import { CoListSchema, CoreCoListSchema } from "./schemaTypes/CoListSchema.js";
import {
  CoMapInitZod,
  CoMapSchema,
  CoreCoMapSchema,
} from "./schemaTypes/CoMapSchema.js";
import {
  CoOptionalSchema,
  CoreCoOptionalSchema,
} from "./schemaTypes/CoOptionalSchema.js";
import { CoreCoValueSchema } from "./schemaTypes/CoValueSchema.js";
import {
  CoreFileStreamSchema,
  FileStreamSchema,
} from "./schemaTypes/FileStreamSchema.js";
import {
  CorePlainTextSchema,
  PlainTextSchema,
} from "./schemaTypes/PlainTextSchema.js";
import {
  CoreRichTextSchema,
  RichTextSchema,
} from "./schemaTypes/RichTextSchema.js";
import { InstanceOfSchemaCoValuesNullable } from "./typeConverters/InstanceOfSchemaCoValuesNullable.js";
import { z } from "./zodReExport.js";

// defining an extra type for this, otherwise BaseSchema & {...} often
// gets expanded into a n inferred type that's too long for typescript to print
export type WithHelpers<
  Base extends CoreCoValueSchema,
  Helpers extends object,
> = Base & Helpers;

export type ZodPrimitiveSchema =
  | z.core.$ZodString
  | z.core.$ZodNumber
  | z.core.$ZodBoolean
  | z.core.$ZodNull
  | z.core.$ZodDate
  | z.core.$ZodLiteral;

export type CoValueClassOrSchema = CoValueClass | CoreCoValueSchema;

export type CoValueSchemaFromCoreSchema<S extends CoreCoValueSchema> =
  S extends CoreAccountSchema<infer Shape extends BaseAccountShape>
    ? AccountSchema<Shape>
    : S extends CoreCoRecordSchema<infer K, infer V>
      ? CoRecordSchema<K, V>
      : S extends CoreCoMapSchema<infer Shape, infer Config>
        ? CoMapSchema<Shape, Config>
        : S extends CoreCoListSchema<infer T>
          ? CoListSchema<T>
          : S extends CoreCoFeedSchema<infer T>
            ? CoFeedSchema<T>
            : S extends CorePlainTextSchema
              ? PlainTextSchema
              : S extends CoreRichTextSchema
                ? RichTextSchema
                : S extends CoreFileStreamSchema
                  ? FileStreamSchema
                  : S extends CoreCoOptionalSchema<infer Inner>
                    ? CoOptionalSchema<Inner>
                    : S extends CoreCoDiscriminatedUnionSchema<infer Members>
                      ? CoDiscriminatedUnionSchema<Members>
                      : never;

export type CoValueClassFromAnySchema<S extends CoValueClassOrSchema> =
  S extends CoValueClass<any>
    ? S
    : CoValueClass<InstanceOfSchema<S>> &
        CoValueFromRaw<InstanceOfSchema<S>> &
        (S extends CoreAccountSchema ? AccountClassEssentials : {});

type AccountClassEssentials = {
  fromRaw: <A extends Account>(this: AccountClass<A>, raw: RawAccount) => A;
  fromNode: <A extends Account>(this: AccountClass<A>, node: LocalNode) => A;
};

export type AnyCoreCoValueSchema =
  | CoreCoMapSchema
  | CoreAccountSchema
  | CoreCoRecordSchema
  | CoreCoListSchema
  | CoreCoFeedSchema
  | CoreCoDiscriminatedUnionSchema<any>
  | CoreCoOptionalSchema
  | CorePlainTextSchema
  | CoreRichTextSchema
  | CoreFileStreamSchema;

type AnyZodSchema = z.core.$ZodType;

export type AnyZodOrCoValueSchema = AnyZodSchema | CoreCoValueSchema;

export type Loaded<
  T extends CoValueClassOrSchema,
  R extends ResolveQuery<T> = true,
> = Resolved<NonNullable<InstanceOfSchemaCoValuesNullable<T>>, R>;

export type ResolveQuery<T extends CoValueClassOrSchema> = RefsToResolve<
  NonNullable<InstanceOfSchemaCoValuesNullable<T>>
>;

export type ResolveQueryStrict<
  T extends CoValueClassOrSchema,
  R extends ResolveQuery<T>,
> = RefsToResolveStrict<NonNullable<InstanceOfSchemaCoValuesNullable<T>>, R>;

export type InitFor<T extends CoValueClassOrSchema> = T extends CoreCoMapSchema<
  infer Shape
>
  ? Simplify<CoMapInitZod<Shape>>
  : never;
