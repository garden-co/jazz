import {
  ZodObject,
  core,
  object as zodObject,
  strictObject as zodStrictObject,
} from "zod";
import { removeGetters } from "../schemaUtils.js";
export {
  string,
  number,
  boolean,
  templateLiteral,
  json,
  date,
  emoji,
  base64,
  base64url,
  nanoid,
  cuid,
  cuid2,
  ulid,
  ipv4,
  ipv6,
  email,
  url,
  uuid,
  literal,
  enum,
  cidrv4,
  cidrv6,
  iso,
  int32,
  union,
  discriminatedUnion,
  // record,
  // intersection,
  int,
  optional,
  array,
  tuple,
  type ZodOptional,
  type ZodReadonly,
  type ZodLazy,
  type ZodDefault,
  type ZodCatch,
  type output as infer,
  type ZodDiscriminatedUnion,
  z,
} from "zod";

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
