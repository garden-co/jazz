import {
  type AccountSchema,
  AnyZodOrCoValueSchema,
  BaseAccountShape,
  type CoFeedSchema,
  type CoListSchema,
  type CoMapSchema,
  type CoProfileSchema,
  type CoRecordSchema,
  type DefaultProfileShape,
  type FileStreamSchema,
  type CoVectorSchema,
  ImageDefinition,
  type PlainTextSchema,
  type Simplify,
  createCoreAccountSchema,
  createCoreCoFeedSchema,
  createCoreCoListSchema,
  createCoreCoMapSchema,
  createCoreCoPlainTextSchema,
  createCoreFileStreamSchema,
  createCoreCoVectorSchema,
  hydrateCoreCoValueSchema,
  isAnyCoValueSchema,
  isCoValueClass,
} from "../../internal.js";
import { removeGetters } from "../schemaUtils.js";
import {
  CoDiscriminatedUnionSchema,
  DiscriminableCoValueSchemas,
  createCoreCoDiscriminatedUnionSchema,
} from "./schemaTypes/CoDiscriminatedUnionSchema.js";
import { CoOptionalSchema } from "./schemaTypes/CoOptionalSchema.js";
import type { CoreCoValueSchema } from "./schemaTypes/CoValueSchema.js";
import {
  RichTextSchema,
  createCoreCoRichTextSchema,
} from "./schemaTypes/RichTextSchema.js";
import { z } from "./zodReExport.js";
import {
  createCoreGroupSchema,
  GroupSchema,
} from "./schemaTypes/GroupSchema.js";

/**
 * Checking for the presence of the `_zod` property is the recommended way
 * to determine if a schema is a Zod v4 schema.
 *
 * @see https://zod.dev/library-authors?id=how-to-support-zod-3-and-zod-4-simultaneously
 */
const isZodV4Schema = (schema: unknown): schema is z.core.$ZodType => {
  return typeof schema === "object" && schema !== null && "_zod" in schema;
};

const isValidShape = (shape: z.core.$ZodLooseShape) => {
  return Object.values(removeGetters(shape)).every(
    (schema) =>
      isZodV4Schema(schema) ||
      isAnyCoValueSchema(schema) ||
      isCoValueClass(schema),
  );
};

const validateCoMapShape = (shape: z.core.$ZodLooseShape) => {
  if (isAnyCoValueSchema(shape as any)) {
    throw new Error(
      "co.map() expects an object as its argument, not a CoValue schema",
    );
  } else if (!isValidShape(shape)) {
    throw new Error(
      "co.map() supports only Zod v4 schemas and CoValue schemas",
    );
  }
};

export const coMapDefiner = <Shape extends z.core.$ZodLooseShape>(
  shape: Shape,
): CoMapSchema<Shape> => {
  validateCoMapShape(shape);
  const coreSchema = createCoreCoMapSchema(shape);
  return hydrateCoreCoValueSchema(coreSchema);
};

/**
 * Defines a collaborative account schema for Jazz applications.
 *
 * Creates an account schema that represents a user account with profile and root data.
 * Accounts are the primary way to identify and manage users in Jazz applications.
 *
 * @template Shape - The shape of the account schema extending BaseAccountShape
 * @param shape - The account schema shape. Defaults to a basic profile with name, inbox, and inboxInvite fields, plus an empty root object.
 *
 * @example
 * ```typescript
 * // Basic account with default profile
 * const BasicAccount = co.account();
 *
 * // Custom account with specific profile and root structure
 * const JazzAccount = co.account({
 *   profile: co.profile({
 *     name: z.string(),
 *     avatar: z.optional(z.string()),
 *   }),
 *   root: co.map({
 *     organizations: co.list(Organization),
 *     draftOrganization: DraftOrganization,
 *   }),
 * }).withMigration(async (account) => {
 *   // Migration logic for existing accounts
 *   if (!account.$jazz.has("profile")) {
 *     const group = Group.create();
 *     account.$jazz.set("profile", co.profile().create(
 *       { name: getRandomUsername() },
 *       group
 *     ));
 *     group.addMember("everyone", "reader");
 *   }
 * });
 * ```
 */
export const coAccountDefiner = <Shape extends BaseAccountShape>(
  shape: Shape = {
    profile: coMapDefiner({
      name: z.string(),
      inbox: z.optional(z.string()),
      inboxInvite: z.optional(z.string()),
    }),
    root: coMapDefiner({}),
  } as unknown as Shape,
): AccountSchema<Shape> => {
  const coreSchema = createCoreAccountSchema(shape);
  return hydrateCoreCoValueSchema(coreSchema);
};

export const coGroupDefiner = (): GroupSchema => {
  const coreSchema = createCoreGroupSchema();
  return hydrateCoreCoValueSchema(coreSchema);
};

export const coRecordDefiner = <
  K extends z.core.$ZodString<string>,
  V extends AnyZodOrCoValueSchema,
>(
  _keyType: K,
  valueType: V,
): CoRecordSchema<K, V> => {
  return coMapDefiner({}).catchall(valueType) as unknown as CoRecordSchema<
    K,
    V
  >;
};

export const coListDefiner = <T extends AnyZodOrCoValueSchema>(
  element: T,
): CoListSchema<T> => {
  const coreSchema = createCoreCoListSchema(element);
  return hydrateCoreCoValueSchema(coreSchema);
};

const validateProfileShape = (shape: z.core.$ZodLooseShape) => {
  if (isAnyCoValueSchema(shape as any)) {
    throw new Error(
      "co.profile() expects an object as its argument, not a CoValue schema",
    );
  } else if (!isValidShape(shape)) {
    throw new Error(
      "co.profile() supports only Zod v4 schemas and CoValue schemas",
    );
  }
};

export const coProfileDefiner = <
  Shape extends z.core.$ZodLooseShape = Simplify<DefaultProfileShape>,
>(
  shape: Shape & Partial<DefaultProfileShape> = {} as any,
): CoProfileSchema<Shape> => {
  validateProfileShape(shape);
  const ehnancedShape = Object.assign(shape, {
    name: z.string(),
    inbox: z.optional(z.string()),
    inboxInvite: z.optional(z.string()),
  });
  return coMapDefiner(ehnancedShape) as unknown as CoProfileSchema<Shape>;
};

export const coFeedDefiner = <T extends AnyZodOrCoValueSchema>(
  element: T,
): CoFeedSchema<T> => {
  const coreSchema = createCoreCoFeedSchema(element);
  return hydrateCoreCoValueSchema(coreSchema);
};

export const coFileStreamDefiner = (): FileStreamSchema => {
  const coreSchema = createCoreFileStreamSchema();
  return hydrateCoreCoValueSchema(coreSchema);
};

export const coVectorDefiner = (dimensions: number): CoVectorSchema => {
  const isPositiveInteger = Number.isInteger(dimensions) && dimensions > 0;

  if (!isPositiveInteger) {
    throw new Error(
      "co.vector() expects the vector dimensions count to be a positive integer",
    );
  }

  const coreSchema = createCoreCoVectorSchema(dimensions);
  return hydrateCoreCoValueSchema(coreSchema);
};

export const coPlainTextDefiner = (): PlainTextSchema => {
  const coreSchema = createCoreCoPlainTextSchema();
  return hydrateCoreCoValueSchema(coreSchema);
};

export const coRichTextDefiner = (): RichTextSchema => {
  const coreSchema = createCoreCoRichTextSchema();
  return hydrateCoreCoValueSchema(coreSchema);
};

export type ImageDefinitionSchema = typeof ImageDefinition;
export const coImageDefiner = (): ImageDefinitionSchema => {
  return ImageDefinition;
};

export const coOptionalDefiner = <T extends CoreCoValueSchema>(
  schema: T,
): CoOptionalSchema<T> => {
  return new CoOptionalSchema(schema);
};

export const coDiscriminatedUnionDefiner = <
  Options extends DiscriminableCoValueSchemas,
>(
  discriminator: string,
  schemas: Options,
): CoDiscriminatedUnionSchema<Options> => {
  const coreSchema = createCoreCoDiscriminatedUnionSchema(
    discriminator,
    schemas,
  );
  return hydrateCoreCoValueSchema(coreSchema);
};
