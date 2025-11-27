import {
  CoDiscriminatedUnionSchema,
  CoValueClass,
  CoreCoFeedSchema,
  CoreCoListSchema,
  CoreCoMapSchema,
  CoreCoRecordSchema,
  CoreCoVectorSchema,
  CorePlainTextSchema,
  Simplify,
} from "../../../internal.js";
import { CoreCoOptionalSchema } from "../schemaTypes/CoOptionalSchema.js";
import { CoreCoValueSchema } from "../schemaTypes/CoValueSchema.js";
import { CoreRichTextSchema } from "../schemaTypes/RichTextSchema.js";
import { z } from "../zodReExport.js";
import { AnyZodOrCoValueSchema, Loaded } from "../zodSchema.js";
import { TypeOfZodSchema } from "./TypeOfZodSchema.js";

/**
 * The type of value that can be used to initialize a CoField of the given schema.
 *
 * For CoValue fields, this can be either a shallowly-loaded CoValue instance
 * or a JSON object that will be used to create the CoValue.
 */
export type CoFieldSchemaInit<S extends CoValueClass | AnyZodOrCoValueSchema> =
  S extends CoreCoValueSchema
    ?
        | Loaded<S>
        | (S extends CoreCoRecordSchema<infer K, infer V>
            ? CoMapSchemaInit<{ [key in z.output<K> & string]: V }>
            : S extends CoreCoMapSchema<infer Shape>
              ? CoMapSchemaInit<Shape>
              : S extends CoreCoListSchema<infer T>
                ? CoListSchemaInit<T>
                : S extends CoreCoFeedSchema<infer T>
                  ? CoFeedSchemaInit<T>
                  : S extends CoreCoVectorSchema
                    ? CoVectorSchemaInit
                    : S extends CorePlainTextSchema | CoreRichTextSchema
                      ? string
                      : S extends CoreCoOptionalSchema<infer T>
                        ? CoFieldSchemaInit<T> | undefined
                        : S extends CoDiscriminatedUnionSchema<infer Members>
                          ? CoFieldSchemaInit<Members[number]>
                          : never)
    : S extends z.core.$ZodType
      ? TypeOfZodSchema<S>
      : S extends CoValueClass
        ? InstanceType<S>
        : never;

// Due to a TS limitation with types that contain known properties and
// an index signature, we cannot accept catchall properties on creation
export type CoMapSchemaInit<Shape extends z.core.$ZodLooseShape> = Simplify<
  {
    /**
     * Cannot use {@link PartialOnUndefined} because evaluating CoFieldInit<Shape[Key]>
     * to know if the value can be undefined does not work with recursive types.
     */
    [Key in keyof Shape as Shape[Key] extends
      | CoreCoOptionalSchema
      | z.core.$ZodOptional
      ? never
      : Key]: CoFieldSchemaInit<Shape[Key]>;
  } & {
    [Key in keyof Shape as Shape[Key] extends
      | CoreCoOptionalSchema
      | z.core.$ZodOptional
      ? Key
      : never]?: CoFieldSchemaInit<Shape[Key]>;
  }
>;

export type CoListSchemaInit<T extends AnyZodOrCoValueSchema> = Simplify<
  ReadonlyArray<CoFieldSchemaInit<T>>
>;

export type CoFeedSchemaInit<T extends AnyZodOrCoValueSchema> = Simplify<
  ReadonlyArray<CoFieldSchemaInit<T>>
>;

export type CoVectorSchemaInit = ReadonlyArray<number> | Float32Array;

/**
 * The convenience type for extracting the init type of a CoValue schema.
 */
export type CoInput<S extends CoValueClass | AnyZodOrCoValueSchema> =
  S extends CoreCoValueSchema
    ? Exclude<CoFieldSchemaInit<S>, Loaded<S>>
    : CoFieldSchemaInit<S>;
