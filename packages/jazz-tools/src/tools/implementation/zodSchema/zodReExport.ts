import {
  ZodArray,
  ZodDiscriminatedUnion,
  ZodIntersection,
  ZodOptional,
  ZodRecord,
  ZodTuple,
  ZodUnion,
  core,
  array as zodArray,
  discriminatedUnion as zodDiscriminatedUnion,
  intersection as zodIntersection,
  optional as zodOptional,
  record as zodRecord,
  tuple as zodTuple,
  union as zodUnion,
} from "zod/v4";
export {
  string,
  number,
  boolean,
  object,
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
  strictObject,
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

export function record<
  Key extends core.$ZodRecordKey,
  Value extends NonCoZodType,
>(
  keyType: Key,
  valueType: Value,
  params?: string | core.$ZodRecordParams,
): ZodRecord<Key, Value> {
  return zodRecord(keyType, valueType, params);
}

export function optional<T extends NonCoZodType>(schema: T): ZodOptional<T> {
  return zodOptional(schema);
}

export function discriminatedUnion<
  T extends readonly [
    NonCoZodType & core.$ZodTypeDiscriminable,
    ...(NonCoZodType & core.$ZodTypeDiscriminable)[],
  ],
>(discriminator: string, schemas: T): ZodDiscriminatedUnion<T> {
  return zodDiscriminatedUnion(discriminator, schemas as any);
}

export function union<const T extends readonly NonCoZodType[]>(
  options: T,
  params?: string | core.$ZodUnionParams,
): ZodUnion<T> {
  return zodUnion(options, params);
}

export function intersection<T extends NonCoZodType, U extends NonCoZodType>(
  left: T,
  right: U,
): ZodIntersection<T, U> {
  return zodIntersection(left, right);
}

export function array<T extends NonCoZodType>(
  element: T,
  params?: string | core.$ZodArrayParams,
): ZodArray<T> {
  return zodArray(element, params);
}

export function tuple<T extends readonly [NonCoZodType, ...NonCoZodType[]]>(
  options: T,
  params?: string | core.$ZodTupleParams,
): ZodTuple<T> {
  return zodTuple(options, params);
}
