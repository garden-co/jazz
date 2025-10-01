import {
  Account,
  AnyZodOrCoValueSchema,
  CoFeed,
  CoList,
  CoMap,
  CoPlainText,
  CoRichText,
  CoValueClass,
  CoreAccountSchema,
  CoreCoDiscriminatedUnionSchema,
  CoreCoFeedSchema,
  CoreCoListSchema,
  CoreCoMapSchema,
  CoreCoRecordSchema,
  FileStream,
  Group,
  MaybeLoaded,
} from "../../../internal.js";
import { CoreCoOptionalSchema } from "../schemaTypes/CoOptionalSchema.js";
import { CoreCoValueSchema } from "../schemaTypes/CoValueSchema.js";
import { CoreFileStreamSchema } from "../schemaTypes/FileStreamSchema.js";
import { CorePlainTextSchema } from "../schemaTypes/PlainTextSchema.js";
import { CoreRichTextSchema } from "../schemaTypes/RichTextSchema.js";
import { CoreGroupSchema } from "../schemaTypes/GroupSchema.js";
import { z } from "../zodReExport.js";
import { InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded } from "./InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded.js";

/**
 * A loaded CoValue whose references may or may not be loaded.
 */
export type InstanceOfSchemaCoValuesMaybeLoaded<
  S extends CoValueClass | AnyZodOrCoValueSchema,
> = S extends CoreCoValueSchema
  ? S extends CoreAccountSchema<infer Shape>
    ? MaybeLoaded<
        {
          readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<
            Shape[key]
          >;
        } & Account
      >
    : S extends CoreGroupSchema
      ? MaybeLoaded<Group>
      : S extends CoreCoRecordSchema<infer K, infer V>
        ? MaybeLoaded<
            {
              readonly [key in z.output<K> &
                string]: InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<V>;
            } & CoMap
          >
        : S extends CoreCoMapSchema<infer Shape, infer CatchAll>
          ? MaybeLoaded<
              {
                readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<
                  Shape[key]
                >;
              } & (CatchAll extends AnyZodOrCoValueSchema
                ? {
                    readonly [
                      key: string
                    ]: InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<CatchAll>;
                  }
                : {}) &
                CoMap
            >
          : S extends CoreCoListSchema<infer T>
            ? MaybeLoaded<
                CoList<InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<T>>
              >
            : S extends CoreCoFeedSchema<infer T>
              ? MaybeLoaded<
                  CoFeed<InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<T>>
                >
              : S extends CorePlainTextSchema
                ? MaybeLoaded<CoPlainText>
                : S extends CoreRichTextSchema
                  ? MaybeLoaded<CoRichText>
                  : S extends CoreFileStreamSchema
                    ? MaybeLoaded<FileStream>
                    : S extends CoreCoOptionalSchema<infer Inner>
                      ?
                          | InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<Inner>
                          | undefined
                      : S extends CoreCoDiscriminatedUnionSchema<infer Members>
                        ? InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<
                            Members[number]
                          >
                        : never
  : S extends CoValueClass
    ? MaybeLoaded<InstanceType<S>>
    : never;
