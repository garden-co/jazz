import { JsonValue } from "cojson";
import { z } from "../zodReExport.js";

// Copied from https://github.com/colinhacks/zod/blob/7e7e3461aceecf3633e158df50d6bc852e7cdf45/packages/zod/src/v4/core/schemas.ts#L1591,
// since this type is not exported by Zod
type OptionalInSchema = {
  _zod: {
    optin: "optional";
  };
};

/**
 * Get type from Zod schema definition.
 *
 * Similar to `z.infer`, but we manually traverse Zod types to ensure we only allow what we can handle
 */
export type TypeOfZodSchema<S extends z.core.$ZodType> =
  S extends z.core.$ZodOptional<infer Inner extends z.core.$ZodType>
    ? TypeOfZodSchema<Inner> | undefined
    : S extends z.core.$ZodNullable<infer Inner extends z.core.$ZodType>
      ? TypeOfZodSchema<Inner> | null
      : S extends z.ZodJSONSchema
        ? JsonValue
        : S extends z.core.$ZodUnion<
              infer Members extends readonly z.core.$ZodType[]
            >
          ? TypeOfZodSchema<Members[number]>
          : S extends z.core.$ZodIntersection<
                infer Left extends z.core.$ZodType,
                infer Right extends z.core.$ZodType
              >
            ? TypeOfZodSchema<Left> & TypeOfZodSchema<Right>
            : S extends z.core.$ZodObject<infer Shape>
              ? /**
                 * Cannot use {@link PartialOnUndefined} because evaluating TypeOfZodSchema<Shape[key]>
                 * to know if the value can be undefined does not work with recursive types.
                 */
                {
                  -readonly [key in keyof Shape as Shape[key] extends OptionalInSchema
                    ? never
                    : key]: TypeOfZodSchema<Shape[key]>;
                } & {
                  -readonly [key in keyof Shape as Shape[key] extends OptionalInSchema
                    ? key
                    : never]?: TypeOfZodSchema<Shape[key]>;
                }
              : S extends z.core.$ZodRecord<
                    infer Key,
                    infer Value extends z.core.$ZodType
                  >
                ? {
                    [key in z.output<Key>]: TypeOfZodSchema<Value>;
                  }
                : S extends z.core.$ZodArray<infer Item extends z.core.$ZodType>
                  ? TypeOfZodSchema<Item>[]
                  : S extends z.core.$ZodTuple<
                        infer Items extends readonly z.core.$ZodType[]
                      >
                    ? {
                        [key in keyof Items]: TypeOfZodSchema<Items[key]>;
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
                                    ? TypeOfZodSchema<Inner>
                                    : S extends z.core.$ZodDefault<
                                          infer Default extends z.core.$ZodType
                                        >
                                      ? TypeOfZodSchema<Default>
                                      : S extends z.core.$ZodCodec<
                                            any,
                                            infer Out extends z.core.$ZodType
                                          >
                                        ? Out["_zod"]["output"]
                                        : S extends z.core.$ZodCatch<
                                              infer Catch extends
                                                z.core.$ZodType
                                            >
                                          ? TypeOfZodSchema<Catch>
                                          : never;
