import {
  core,
  ZodObject,
  object as zodObject,
  strictObject as zodStrictObject,
} from "zod/v4";
import { removeGetters } from "../schemaUtils.js";

export {
  array,
  base64,
  base64url,
  boolean,
  cidrv4,
  cidrv6,
  cuid,
  cuid2,
  date,
  discriminatedUnion,
  email,
  emoji,
  enum,
  // record,
  // intersection,
  int,
  int32,
  ipv4,
  ipv6,
  iso,
  json,
  literal,
  nanoid,
  number,
  optional,
  type output as infer,
  string,
  templateLiteral,
  tuple,
  ulid,
  union,
  url,
  uuid,
  type ZodCatch,
  type ZodDefault,
  type ZodDiscriminatedUnion,
  type ZodLazy,
  type ZodOptional,
  type ZodReadonly,
  z,
} from "zod/v4";

export function object<
  T extends core.$ZodLooseShape = Partial<Record<never, core.SomeType>>,
>(
  shape?: T,
  params?: string | core.$ZodObjectParams,
): ZodObject<T, core.$strip> {
  rejectCoValueSchemas(
    shape,
    "z.object() does not support collaborative types as values. Use co.map() instead",
  );
  return zodObject(shape, params);
}

export function strictObject<T extends core.$ZodLooseShape>(
  shape: T,
  params?: string | core.$ZodObjectParams,
): ZodObject<T, core.$strict> {
  rejectCoValueSchemas(
    shape,
    "z.strictObject() does not support collaborative types as values. Use co.map() instead",
  );
  return zodStrictObject(shape, params);
}

function rejectCoValueSchemas(
  shape: core.$ZodLooseShape | undefined,
  errorMessage: string,
) {
  if (containsCoValueSchema(shape)) {
    throw Error(errorMessage);
  }
}

function containsCoValueSchema(shape?: core.$ZodLooseShape): boolean {
  // Remove getters to avoid circularity issues accessing schemas that may not be defined yet
  return Object.values(removeGetters(shape ?? {})).some(isAnyCoValueSchema);
}

// Note: if you're editing this function, edit the `isAnyCoValueSchema`
// function in `zodSchemaToCoSchema.ts` as well
function isAnyCoValueSchema(schema: any): boolean {
  return "collaborative" in schema && schema.collaborative === true;
}
