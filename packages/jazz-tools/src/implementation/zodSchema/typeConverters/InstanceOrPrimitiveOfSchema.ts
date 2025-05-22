import { JsonValue } from "cojson";
import z from "zod/v4";
import {
  Account,
  CoFeed,
  CoList,
  CoMap,
  CoPlainText,
  CoRichText,
  CoValueClass,
  FileStream,
  Profile,
} from "../../../internal.js";
import { AnyCoFeedSchema } from "../schemaTypes/CoFeedSchema.js";
import { AnyCoListSchema } from "../schemaTypes/CoListSchema.js";
import { AnyCoMapSchema } from "../schemaTypes/CoMapSchema.js";
import { FileStreamSchema } from "../schemaTypes/FileStreamSchema.js";
import { PlainTextSchema } from "../schemaTypes/PlainTextSchema.js";
import { RichTextSchema } from "../schemaTypes/RichTextSchema.js";

export type InstanceOrPrimitiveOfSchema<
  S extends CoValueClass | z.core.$ZodType,
> = S extends z.core.$ZodType
  ? S extends z.core.$ZodObject<infer Shape> & {
      collaborative: true;
      builtin: "Account";
    }
    ? {
        -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
      } & { profile: Profile } & Account
    : S extends z.core.$ZodRecord<infer K, infer V> & {
          collaborative: true;
        }
      ? {
          -readonly [key in z.output<K> &
            string]: InstanceOrPrimitiveOfSchema<V>;
        } & CoMap
      : S extends AnyCoMapSchema<infer Shape, infer Config>
        ? {
            -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<
              Shape[key]
            >;
          } & (unknown extends Config["out"][string]
            ? {}
            : {
                [key: string]: Config["out"][string];
              }) &
            CoMap
        : S extends AnyCoListSchema<infer T>
          ? CoList<InstanceOrPrimitiveOfSchema<T>>
          : S extends AnyCoFeedSchema<infer T>
            ? CoFeed<InstanceOrPrimitiveOfSchema<T>>
            : S extends PlainTextSchema
              ? CoPlainText
              : S extends RichTextSchema
                ? CoRichText
                : S extends FileStreamSchema
                  ? FileStream
                  : S extends z.core.$ZodOptional<infer Inner>
                    ? InstanceOrPrimitiveOfSchema<Inner> | undefined
                    : S extends z.ZodJSONSchema
                      ? JsonValue
                      : S extends z.core.$ZodUnion<infer Members>
                        ? InstanceOrPrimitiveOfSchema<Members[number]>
                        : // primitives below here - we manually traverse to ensure we only allow what we can handle
                          S extends z.core.$ZodObject<infer Shape>
                          ? {
                              -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<
                                Shape[key]
                              >;
                            }
                          : S extends z.core.$ZodArray<infer Item>
                            ? InstanceOrPrimitiveOfSchema<Item>[]
                            : S extends z.core.$ZodTuple<infer Items>
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
                                    : S extends z.core.$ZodLiteral<
                                          infer Literal
                                        >
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
                                                  infer Inner
                                                >
                                              ? InstanceOrPrimitiveOfSchema<Inner>
                                              : never
  : S extends CoValueClass
    ? InstanceType<S>
    : never;
