import { JsonValue } from "cojson";
import {
  Account,
  AnyZodOrCoValueSchema,
  CoDiscriminatedUnionSchema,
  CoFeed,
  CoList,
  CoMap,
  CoPlainText,
  CoRichText,
  CoValueClass,
  CoreAccountSchema,
  CoreCoRecordSchema,
  FileStream,
  Profile,
} from "../../../internal.js";
import { CoreCoFeedSchema } from "../schemaTypes/CoFeedSchema.js";
import { CoreCoListSchema } from "../schemaTypes/CoListSchema.js";
import { CoreCoMapSchema } from "../schemaTypes/CoMapSchema.js";
import { CoreCoOptionalSchema } from "../schemaTypes/CoOptionalSchema.js";
import { CoreCoValueSchema } from "../schemaTypes/CoValueSchema.js";
import { CoreFileStreamSchema } from "../schemaTypes/FileStreamSchema.js";
import { CorePlainTextSchema } from "../schemaTypes/PlainTextSchema.js";
import { CoreRichTextSchema } from "../schemaTypes/RichTextSchema.js";
import { z } from "../zodReExport.js";

export type InstanceOrPrimitiveOfSchema<
  S extends CoValueClass | AnyZodOrCoValueSchema,
> = S extends CoreCoValueSchema
  ? S extends CoreAccountSchema<infer Shape>
    ? {
        -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
      } & { profile: Profile } & Account
    : S extends CoreCoRecordSchema<infer K, infer V>
      ? {
          -readonly [key in z.output<K> &
            string]: InstanceOrPrimitiveOfSchema<V>;
        } & CoMap
      : S extends CoreCoMapSchema<infer Shape, infer CatchAll>
        ? {
            -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<
              Shape[key]
            >;
          } & (CatchAll extends AnyZodOrCoValueSchema
            ? {
                [key: string]: InstanceOrPrimitiveOfSchema<CatchAll>;
              }
            : {}) &
            CoMap
        : S extends CoreCoListSchema<infer T>
          ? CoList<InstanceOrPrimitiveOfSchema<T>>
          : S extends CoreCoFeedSchema<infer T>
            ? CoFeed<InstanceOrPrimitiveOfSchema<T>>
            : S extends CorePlainTextSchema
              ? CoPlainText
              : S extends CoreRichTextSchema
                ? CoRichText
                : S extends CoreFileStreamSchema
                  ? FileStream
                  : S extends CoreCoOptionalSchema<infer T>
                    ? InstanceOrPrimitiveOfSchema<T> | undefined
                    : S extends CoDiscriminatedUnionSchema<infer Members>
                      ? InstanceOrPrimitiveOfSchema<Members[number]>
                      : never
  : S extends z.core.$ZodType
    ? S extends z.core.$ZodOptional<infer Inner extends z.core.$ZodType>
      ? InstanceOrPrimitiveOfSchema<Inner> | undefined
      : S extends z.ZodJSONSchema
        ? JsonValue
        : S extends z.core.$ZodUnion<infer Members extends z.core.$ZodType[]>
          ? InstanceOrPrimitiveOfSchema<Members[number]>
          : // primitives below here - we manually traverse to ensure we only allow what we can handle
            S extends z.core.$ZodObject<infer Shape>
            ? {
                -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<
                  Shape[key]
                >;
              }
            : S extends z.core.$ZodArray<infer Item extends z.core.$ZodType>
              ? InstanceOrPrimitiveOfSchema<Item>[]
              : S extends z.core.$ZodTuple<
                    infer Items extends readonly z.core.$ZodType[]
                  >
                ? {
                    [key in keyof Items]: InstanceOrPrimitiveOfSchema<
                      Items[key]
                    >;
                  }
                : S extends z.core.$ZodString
                  ? string
                  : S extends z.core.$ZodNumber
                    ? number
                    : S extends z.core.$ZodBoolean
                      ? boolean
                      : S extends z.core.$ZodLiteral<infer Literal>
                        ? Literal
                        : S extends z.core.$ZodDate
                          ? Date
                          : S extends z.core.$ZodEnum<infer Enum>
                            ? Enum[keyof Enum]
                            : S extends z.core.$ZodTemplateLiteral<
                                  infer pattern
                                >
                              ? pattern
                              : S extends z.core.$ZodReadonly<
                                    infer Inner extends z.core.$ZodType
                                  >
                                ? InstanceOrPrimitiveOfSchema<Inner>
                                : S extends z.core.$ZodDefault<
                                      infer Default extends z.core.$ZodType
                                    >
                                  ? InstanceOrPrimitiveOfSchema<Default>
                                  : S extends z.core.$ZodCatch<
                                        infer Catch extends z.core.$ZodType
                                      >
                                    ? InstanceOrPrimitiveOfSchema<Catch>
                                    : never
    : S extends CoValueClass
      ? InstanceType<S>
      : never;
