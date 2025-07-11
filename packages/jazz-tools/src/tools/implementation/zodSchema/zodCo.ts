import {
  type AccountSchema,
  AnyAccountSchema,
  AnyCoFeedSchema,
  AnyCoListSchema,
  AnyCoSchema,
  AnyFileStreamSchema,
  AnyPlainTextSchema,
  BaseAccountShape,
  CoFeed,
  type CoFeedSchema,
  type CoListSchema,
  type CoMapSchema,
  CoPlainText,
  type CoProfileSchema,
  type CoRecordSchema,
  CoRichText,
  type DefaultProfileShape,
  FileStream,
  type FileStreamSchema,
  ImageDefinition,
  type PlainTextSchema,
  type Simplify,
  zodSchemaToCoSchema,
} from "../../internal.js";
import { CoDiscriminatedUnionSchema } from "./schemaTypes/CoDiscriminatedUnionSchema.js";
import {
  CoOptionalSchema,
  createCoOptionalSchema,
} from "./schemaTypes/CoOptionalSchema.js";
import {
  AnyRichTextSchema,
  RichTextSchema,
} from "./schemaTypes/RichTextSchema.js";
import { z } from "./zodReExport.js";

export const coMapDefiner = <Shape extends z.core.$ZodLooseShape>(
  shape: Shape,
): CoMapSchema<Shape> => {
  const objectSchema = z.object(shape).meta({
    collaborative: true,
  });
  const enrichedSchema = Object.assign(objectSchema, {
    collaborative: true,
  });
  return zodSchemaToCoSchema(enrichedSchema);
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
 *   if (account.profile === undefined) {
 *     const group = Group.create();
 *     account.profile = co.profile().create(
 *       { name: getRandomUsername() },
 *       group
 *     );
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
  const schema = z.object(shape).meta({
    collaborative: true,
  });
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    builtin: "Account",
  }) as AnyAccountSchema<Shape>;
  return zodSchemaToCoSchema(enrichedSchema);
};

export const coRecordDefiner = <
  K extends z.core.$ZodString<string>,
  V extends z.core.$ZodType,
>(
  _keyType: K,
  valueType: V,
): CoRecordSchema<K, V> => {
  return coMapDefiner({}).catchall(valueType) as unknown as CoRecordSchema<
    K,
    V
  >;
};

export const coListDefiner = <T extends z.core.$ZodType>(
  element: T,
): CoListSchema<T> => {
  const schema = z.array(element).meta({
    collaborative: true,
  });
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
  }) as AnyCoListSchema<T>;
  return zodSchemaToCoSchema(enrichedSchema);
};

export const coProfileDefiner = <
  Shape extends z.core.$ZodLooseShape = Simplify<DefaultProfileShape>,
>(
  shape: Shape & Partial<DefaultProfileShape> = {} as any,
): CoProfileSchema<Shape> => {
  const ehnancedShape = Object.assign(shape, {
    name: z.string(),
    inbox: z.optional(z.string()),
    inboxInvite: z.optional(z.string()),
  });
  return coMapDefiner(ehnancedShape) as CoProfileSchema<Shape>;
};

export const coFeedDefiner = <T extends z.core.$ZodType>(
  element: T,
): CoFeedSchema<T> => {
  const schema = z.instanceof(CoFeed);
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    builtin: "CoFeed",
    element,
  }) as AnyCoFeedSchema<T>;
  return zodSchemaToCoSchema(enrichedSchema);
};

export const coFileStreamDefiner = (): FileStreamSchema => {
  const schema = z.instanceof(FileStream);
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    builtin: "FileStream",
  }) as AnyFileStreamSchema;
  return zodSchemaToCoSchema(enrichedSchema);
};

export const coPlainTextDefiner = (): PlainTextSchema => {
  const schema = z.instanceof(CoPlainText);
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    builtin: "CoPlainText",
  }) as AnyPlainTextSchema;
  return zodSchemaToCoSchema(enrichedSchema);
};

export const coRichTextDefiner = (): RichTextSchema => {
  const schema = z.instanceof(CoRichText);
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    builtin: "CoRichText",
  }) as AnyRichTextSchema;
  return zodSchemaToCoSchema(enrichedSchema);
};

export const coImageDefiner = (): typeof ImageDefinition => {
  return ImageDefinition;
};

export const coOptionalDefiner = <T extends AnyCoSchema>(
  schema: T,
): CoOptionalSchema<T> => {
  return createCoOptionalSchema(schema);
};

export const coDiscriminatedUnionDefiner = <
  T extends readonly [
    AnyCoSchema & z.core.$ZodTypeDiscriminable,
    ...(AnyCoSchema & z.core.$ZodTypeDiscriminable)[],
  ],
>(
  discriminator: string,
  schemas: T,
): CoDiscriminatedUnionSchema<T> => {
  const schema = z.discriminatedUnion(discriminator, schemas as any);
  return zodSchemaToCoSchema(schema);
};
