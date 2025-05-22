import { CoValueUniqueness } from "cojson";
import z from "zod/v4";
import {
  Account,
  AccountCreationProps,
  AccountInstance,
  AccountSchema,
  AnonymousJazzAgent,
  AnyCoMapSchema,
  CoFeed,
  CoFeedSchema,
  CoListSchema,
  CoMapSchema,
  CoPlainText,
  CoProfileSchema,
  CoRecordSchema,
  CoRichText,
  DefaultProfileShape,
  FileStream,
  FileStreamSchema,
  Group,
  ImageDefinition,
  PlainTextSchema,
  Simplify,
  zodSchemaToCoSchema,
} from "../../internal.js";
import { RichTextSchema } from "./schemaTypes/RichTextSchema.js";

export const coMapDefiner = <Shape extends z.core.$ZodLooseShape>(
  shape: Shape,
): CoMapSchema<Shape> => {
  const objectSchema = z.object(shape).meta({
    collaborative: true,
  });

  type CleanedType = Pick<
    typeof objectSchema,
    "_zod" | "def" | "~standard" | "catchall"
  >;

  const coMapSchema = objectSchema as unknown as CleanedType & {
    collaborative: true;
    create: CoMapSchema<Shape>["create"];
    load: CoMapSchema<Shape>["load"];
    subscribe: CoMapSchema<Shape>["subscribe"];
    findUnique: CoMapSchema<Shape>["findUnique"];
    catchall: CoMapSchema<Shape>["catchall"];
    withHelpers: CoMapSchema<Shape>["withHelpers"];
  };

  coMapSchema.collaborative = true;

  coMapSchema.create = function (this: CoMapSchema<Shape>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).create(...args);
  } as CoMapSchema<Shape>["create"];

  coMapSchema.load = function (this: CoMapSchema<Shape>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).load(...args);
  } as CoMapSchema<Shape>["load"];

  coMapSchema.subscribe = function (this: CoMapSchema<Shape>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).subscribe(...args);
  } as CoMapSchema<Shape>["subscribe"];

  coMapSchema.findUnique = function (
    this: CoMapSchema<Shape>,
    unique: CoValueUniqueness["uniqueness"],
    ownerID: string,
    as?: Account | Group | AnonymousJazzAgent,
  ) {
    return (zodSchemaToCoSchema(this) as any).findUnique(unique, ownerID, as);
  } as CoMapSchema<Shape>["findUnique"];

  const oldCatchall = coMapSchema.catchall;
  coMapSchema.catchall = function (
    this: CoMapSchema<Shape>,
    schema: z.core.$ZodType,
  ) {
    return { ...this, ...oldCatchall(schema) } as any;
  } as CoMapSchema<Shape>["catchall"] as any;

  coMapSchema.withHelpers = function (
    this: CoMapSchema<Shape>,
    helpers: (Self: CoMapSchema<Shape>) => object,
  ) {
    return { ...this, ...helpers(this) };
  } as CoMapSchema<Shape>["withHelpers"];

  return coMapSchema as unknown as CoMapSchema<Shape>;
};

export const coAccountDefiner = <
  Shape extends {
    profile: AnyCoMapSchema<{
      name: z.core.$ZodString<string>;
      inbox?: z.core.$ZodOptional<z.core.$ZodString>;
      inboxInvite?: z.core.$ZodOptional<z.core.$ZodString>;
    }>;
    root: AnyCoMapSchema;
  },
>(
  shape: Shape = {
    profile: coMapDefiner({
      name: z.string(),
      inbox: z.optional(z.string()),
      inboxInvite: z.optional(z.string()),
    }),
    root: coMapDefiner({}),
  } as unknown as Shape,
): AccountSchema<Shape> => {
  const objectSchema = z.object(shape).meta({
    collaborative: true,
  });

  type CleanedType = Pick<
    typeof objectSchema,
    "_zod" | "def" | "~standard" | "catchall"
  >;

  const accountSchema = objectSchema as unknown as CleanedType & {
    collaborative: true;
    builtin: "Account";
    migration?: (
      account: AccountInstance<Shape>,
      creationProps?: AccountCreationProps,
    ) => void;

    create: AccountSchema<Shape>["create"];
    createAs: AccountSchema<Shape>["createAs"];
    getMe: AccountSchema<Shape>["getMe"];
    load: AccountSchema<Shape>["load"];
    subscribe: AccountSchema<Shape>["subscribe"];
    withHelpers: AccountSchema<Shape>["withHelpers"];
    withMigration: AccountSchema<Shape>["withMigration"];
  };

  accountSchema.collaborative = true;
  accountSchema.builtin = "Account";

  accountSchema.create = function (this: AccountSchema<Shape>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).create(...args);
  } as AccountSchema<Shape>["create"];

  accountSchema.createAs = function (
    this: AccountSchema<Shape>,
    ...args: any[]
  ) {
    return (zodSchemaToCoSchema(this) as any).createAs(...args);
  } as AccountSchema<Shape>["createAs"];

  accountSchema.getMe = function (this: AccountSchema<Shape>) {
    return (zodSchemaToCoSchema(this) as any).getMe();
  } as AccountSchema<Shape>["getMe"];

  accountSchema.load = function (this: AccountSchema<Shape>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).load(...args);
  } as AccountSchema<Shape>["load"];

  accountSchema.subscribe = function (
    this: AccountSchema<Shape>,
    ...args: any[]
  ) {
    return (zodSchemaToCoSchema(this) as any).subscribe(...args);
  } as AccountSchema<Shape>["subscribe"];

  accountSchema.withHelpers = function (
    this: CoMapSchema<Shape>,
    helpers: object,
  ) {
    return { ...this, ...helpers };
  } as CoMapSchema<Shape>["withHelpers"];

  accountSchema.withMigration = function (
    this: AccountSchema<Shape>,
    migration: (
      account: AccountInstance<Shape>,
      creationProps?: AccountCreationProps,
    ) => void,
  ) {
    return { ...this, migration };
  } as AccountSchema<Shape>["withMigration"];

  return accountSchema as unknown as AccountSchema<Shape>;
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
  const arraySchema = z.array(element).meta({
    collaborative: true,
  });

  type CleanedType = Pick<typeof arraySchema, "_zod" | "def" | "~standard">;

  const coListSchema = arraySchema as unknown as CleanedType & {
    collaborative: true;
    create: CoListSchema<T>["create"];
    load: CoListSchema<T>["load"];
    subscribe: CoListSchema<T>["subscribe"];
    withHelpers: CoListSchema<T>["withHelpers"];
  };

  coListSchema.collaborative = true;

  coListSchema.create = function (this: CoListSchema<T>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).create(...args);
  } as CoListSchema<T>["create"];

  coListSchema.load = function (this: CoListSchema<T>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).load(...args);
  } as CoListSchema<T>["load"];

  coListSchema.subscribe = function (this: CoListSchema<T>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).subscribe(...args);
  } as CoListSchema<T>["subscribe"];

  coListSchema.withHelpers = function (
    this: CoListSchema<T>,
    helpers: (Self: CoListSchema<T>) => object,
  ) {
    return { ...this, ...helpers(this) };
  } as CoListSchema<T>["withHelpers"];

  return coListSchema;
};

export const coProfileDefiner = <
  Shape extends z.core.$ZodLooseShape = Simplify<DefaultProfileShape>,
>(
  shape: Shape & {
    name?: z.core.$ZodString<string>;
    inbox?: z.core.$ZodOptional<z.core.$ZodString>;
    inboxInvite?: z.core.$ZodOptional<z.core.$ZodString>;
  } = {} as any,
): CoProfileSchema<Shape> => {
  const base = coMapDefiner({
    ...(shape ?? {}),
    name: z.string(),
    inbox: z.optional(z.string()),
    inboxInvite: z.optional(z.string()),
  });
  return {
    ...base,
    // enforce that the owner is a group
    create: ((init: any, options: { owner: Group } | Group) => {
      return base.create(init, options);
    }) as CoProfileSchema<Shape>["create"],
  } as CoProfileSchema<Shape>;
};

export const coFeedDefiner = <T extends z.core.$ZodType>(
  element: T,
): CoFeedSchema<T> => {
  const placeholderSchema = z.instanceof(CoFeed);

  const coFeedSchema = placeholderSchema as unknown as Pick<
    typeof placeholderSchema,
    "_zod" | "def" | "~standard"
  > & {
    collaborative: true;
    builtin: "CoFeed";
    element: T;
    create: CoFeedSchema<T>["create"];
    load: CoFeedSchema<T>["load"];
    subscribe: CoFeedSchema<T>["subscribe"];
  };

  coFeedSchema.collaborative = true;
  coFeedSchema.builtin = "CoFeed";
  coFeedSchema.element = element;

  coFeedSchema.create = function (this: CoFeedSchema<T>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).create(...args);
  } as CoFeedSchema<T>["create"];

  coFeedSchema.load = function (this: CoFeedSchema<T>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).load(...args);
  } as CoFeedSchema<T>["load"];

  coFeedSchema.subscribe = function (this: CoFeedSchema<T>, ...args: any[]) {
    return (zodSchemaToCoSchema(this) as any).subscribe(...args);
  } as CoFeedSchema<T>["subscribe"];

  return coFeedSchema as unknown as CoFeedSchema<T>;
};

export const coFileStreamDefiner = (): FileStreamSchema => {
  const placeholderSchema = z.instanceof(FileStream);

  const fileStreamSchema = placeholderSchema as unknown as Pick<
    typeof placeholderSchema,
    "_zod" | "def" | "~standard"
  > & {
    collaborative: true;
    builtin: "FileStream";
    create: FileStreamSchema["create"];
    createFromBlob: FileStreamSchema["createFromBlob"];
    load: FileStreamSchema["load"];
    loadAsBlob: FileStreamSchema["loadAsBlob"];
    subscribe: FileStreamSchema["subscribe"];
  };

  fileStreamSchema.collaborative = true;
  fileStreamSchema.builtin = "FileStream";

  fileStreamSchema.create = function (options: any) {
    return FileStream.create(options);
  } as FileStreamSchema["create"];

  fileStreamSchema.createFromBlob = function (blob: Blob, options: any) {
    return FileStream.createFromBlob(blob, options);
  } as FileStreamSchema["createFromBlob"];

  fileStreamSchema.load = function (id: string, options: any) {
    return FileStream.load(id, options);
  } as FileStreamSchema["load"];

  fileStreamSchema.loadAsBlob = function (id: string, options: any) {
    return FileStream.loadAsBlob(id, options);
  } as FileStreamSchema["loadAsBlob"];

  fileStreamSchema.subscribe = function (
    id: string,
    options: any,
    listener: any,
  ) {
    return FileStream.subscribe(id, options, listener);
  } as FileStreamSchema["subscribe"];

  return fileStreamSchema;
};

export const coPlainTextDefiner = (): PlainTextSchema => {
  const placeholderSchema = z.instanceof(CoPlainText);

  const plainTextSchema = placeholderSchema as unknown as Pick<
    typeof placeholderSchema,
    "_zod" | "def" | "~standard"
  > & {
    collaborative: true;
    builtin: "CoPlainText";
    create: PlainTextSchema["create"];
    load: PlainTextSchema["load"];
    subscribe: PlainTextSchema["subscribe"];
    fromRaw: PlainTextSchema["fromRaw"];
  };

  plainTextSchema.collaborative = true;
  plainTextSchema.builtin = "CoPlainText";

  plainTextSchema.create = function (...args: any[]) {
    return (CoPlainText as any).create(...args);
  } as PlainTextSchema["create"];

  plainTextSchema.load = function (...args: any[]) {
    return (CoPlainText as any).load(...args);
  } as PlainTextSchema["load"];

  plainTextSchema.subscribe = function (...args: any[]) {
    return (CoPlainText as any).subscribe(...args);
  } as PlainTextSchema["subscribe"];

  plainTextSchema.fromRaw = function (...args: any[]) {
    return (CoPlainText as any).fromRaw(...args);
  } as PlainTextSchema["fromRaw"];

  return plainTextSchema;
};

export const coRichTextDefiner = (): RichTextSchema => {
  const placeholderSchema = z.instanceof(CoRichText);

  const richTextSchema = placeholderSchema as unknown as Pick<
    typeof placeholderSchema,
    "_zod" | "def" | "~standard"
  > & {
    collaborative: true;
    builtin: "CoRichText";
    create: RichTextSchema["create"];
    load: RichTextSchema["load"];
    subscribe: RichTextSchema["subscribe"];
  };

  richTextSchema.collaborative = true;
  richTextSchema.builtin = "CoRichText";

  richTextSchema.create = function (...args: any[]) {
    return (CoRichText as any).create(...args);
  } as RichTextSchema["create"];

  richTextSchema.load = function (...args: any[]) {
    return (CoRichText as any).load(...args);
  } as RichTextSchema["load"];

  richTextSchema.subscribe = function (...args: any[]) {
    return (CoRichText as any).subscribe(...args);
  } as RichTextSchema["subscribe"];

  return richTextSchema;
};

export const coImageDefiner = (): typeof ImageDefinition => {
  return ImageDefinition;
};
