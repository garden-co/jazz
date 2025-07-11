import {
  Account,
  AnyAccountSchema,
  AnyCoRecordSchema,
  CoFeed,
  CoList,
  CoMap,
  CoPlainText,
  CoRichText,
  CoValueClass,
  FileStream,
} from "../../../internal.js";
import { AnyCoFeedSchema } from "../schemaTypes/CoFeedSchema.js";
import { AnyCoListSchema } from "../schemaTypes/CoListSchema.js";
import { AnyCoMapSchema } from "../schemaTypes/CoMapSchema.js";
import { AnyFileStreamSchema } from "../schemaTypes/FileStreamSchema.js";
import { AnyPlainTextSchema } from "../schemaTypes/PlainTextSchema.js";
import { AnyRichTextSchema } from "../schemaTypes/RichTextSchema.js";
import { z } from "../zodReExport.js";
import { InstanceOrPrimitiveOfSchema } from "./InstanceOrPrimitiveOfSchema.js";

export type InstanceOfSchema<S extends CoValueClass | z.core.$ZodType> =
  S extends z.core.$ZodType
    ? S extends AnyAccountSchema<infer Shape>
      ? {
          [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
        } & Account
      : S extends AnyCoRecordSchema<infer K, infer V>
        ? {
            [key in z.output<K> & string]: InstanceOrPrimitiveOfSchema<V>;
          } & CoMap
        : S extends AnyCoMapSchema<infer Shape, infer Config>
          ? {
              [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
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
              : S extends AnyPlainTextSchema
                ? CoPlainText
                : S extends AnyRichTextSchema
                  ? CoRichText
                  : S extends AnyFileStreamSchema
                    ? FileStream
                    : S extends z.core.$ZodOptional<infer Inner>
                      ? InstanceOrPrimitiveOfSchema<Inner>
                      : S extends z.core.$ZodUnion<infer Members>
                        ? InstanceOrPrimitiveOfSchema<Members[number]>
                        : never
    : S extends CoValueClass
      ? InstanceType<S>
      : never;
