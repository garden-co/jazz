import {
  core,
  ZodObject,
  object as zodObject,
  ZodOptional,
  optional as zodOptional,
} from "zod/v4";
import { util } from "zod/v4/core";
export {
  string,
  number,
  boolean,
  union,
  array,
  templateLiteral,
  json,
  tuple,
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
  strictObject,
  discriminatedUnion,
  //   intersection,
  //   record,
  int,
  type ZodOptional,
  type ZodReadonly,
  type ZodLazy,
  type ZodDefault,
  type ZodCatch,
  type output as infer,
  z,
} from "zod/v4";

type NonCoZodType = core.$ZodType & { collaborative?: false };

export function object<T extends Record<string, NonCoZodType>>(
  shape?: T,
  params?: string | core.$ZodObjectParams,
): ZodObject<util.Writeable<T> & {}, core.$strip> {
  return zodObject(shape, params);
}

export function optional<T extends NonCoZodType>(schema: T): ZodOptional<T> {
  return zodOptional(schema);
}
