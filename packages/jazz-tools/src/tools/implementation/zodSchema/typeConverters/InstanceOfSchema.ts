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
import { InstanceOrPrimitiveOfSchema } from "./InstanceOrPrimitiveOfSchema.js";

export type InstanceOfSchema<S extends CoValueClass | AnyZodOrCoValueSchema> =
  S extends CoreCoValueSchema
    ? S extends CoreAccountSchema<infer Shape>
      ? {
          [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
        } & Account
      : S extends CoreCoRecordSchema<infer K, infer V>
        ? {
            [key in z.output<K> & string]: InstanceOrPrimitiveOfSchema<V>;
          } & CoMap
        : S extends CoreCoMapSchema<infer Shape, infer CatchAll>
          ? {
              [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
            } & (unknown extends CatchAll
              ? {}
              : {
                  // @ts-expect-error
                  [key: string]: InstanceOrPrimitiveOfSchema<CatchAll>;
                }) &
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
                      ? InstanceOrPrimitiveOfSchema<T>
                      : S extends CoDiscriminatedUnionSchema<infer Members>
                        ? InstanceOrPrimitiveOfSchema<Members[number]>
                        : never
    : S extends CoValueClass
      ? InstanceType<S>
      : never;
