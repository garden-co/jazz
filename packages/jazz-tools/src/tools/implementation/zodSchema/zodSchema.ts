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
import { CoMapSchema, CoreCoMapSchema } from "./schemaTypes/CoMapSchema.js";
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
  CoreCoVectorSchema,
  CoVectorSchema,
} from "./schemaTypes/CoVectorSchema.js";
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
import { CoreGroupSchema } from "./schemaTypes/GroupSchema.js";
import { GroupSchema } from "./schemaTypes/GroupSchema.js";

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
    : S extends CoreGroupSchema
      ? GroupSchema
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
                    : S extends CoreCoVectorSchema
                      ? CoVectorSchema
                      : S extends CoreCoOptionalSchema<infer Inner>
                        ? CoOptionalSchema<Inner>
                        : S extends CoreCoDiscriminatedUnionSchema<
                              infer Members
                            >
                          ? CoDiscriminatedUnionSchema<Members>
                          : never;

export type CoValueClassFromAnySchema<S extends CoValueClassOrSchema> =
  S extends CoValueClass<any>
    ? S
    : CoValueClass<NonNullable<InstanceOfSchema<S>>> &
        CoValueFromRaw<NonNullable<InstanceOfSchema<S>>> &
        (S extends CoreAccountSchema ? AccountClassEssentials : {});

type AccountClassEssentials = {
  fromRaw: <A extends Account>(this: AccountClass<A>, raw: RawAccount) => A;
  fromNode: <A extends Account>(this: AccountClass<A>, node: LocalNode) => A;
};

export type AnyCoreCoValueSchema =
  | CoreCoMapSchema
  | CoreAccountSchema
  | CoreGroupSchema
  | CoreCoRecordSchema
  | CoreCoListSchema
  | CoreCoFeedSchema
  | CoreCoDiscriminatedUnionSchema<any>
  | CoreCoOptionalSchema
  | CorePlainTextSchema
  | CoreRichTextSchema
  | CoreFileStreamSchema
  | CoreCoVectorSchema;

export type AnyZodSchema = z.core.$ZodType;

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
