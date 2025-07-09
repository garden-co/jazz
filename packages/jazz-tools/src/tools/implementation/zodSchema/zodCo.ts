import {
  type Account,
  type AccountCreationProps,
  type AccountSchema,
  AnyCoSchema,
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
import { RichTextSchema } from "./schemaTypes/RichTextSchema.js";
import { z } from "./zodReExport.js";

function enrichCoMapSchema<Shape extends z.core.$ZodLooseShape>(
  schema: z.ZodObject<
    { -readonly [P in keyof Shape]: Shape[P] },
    z.core.$strip
  >,
) {
  const baseCatchall = schema.catchall;

  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    create: (...args: any[]) => {
      return coSchema.create(...args);
    },
    load: (...args: any[]) => {
      return coSchema.load(...args);
    },
    subscribe: (...args: any[]) => {
      return coSchema.subscribe(...args);
    },
    findUnique: (...args: any[]) => {
      return coSchema.findUnique(...args);
    },
    upsertUnique: (...args: any[]) => {
      return coSchema.upsertUnique(...args);
    },
    loadUnique: (...args: any[]) => {
      return coSchema.loadUnique(...args);
    },
    catchall: (index: z.core.$ZodType) => {
      return enrichCoMapSchema(baseCatchall(index));
    },
    withHelpers: (helpers: (Self: z.core.$ZodType) => object) => {
      return Object.assign(schema, helpers(schema));
    },
    withMigration: (migration: (value: any) => undefined) => {
      coSchema.prototype.migrate = migration;

      return enrichedSchema;
    },
    getCoSchema: () => {
      return coSchema;
    },
  }) as unknown as CoMapSchema<Shape>;

  // Needs to be derived from the enriched schema
  const coSchema = zodSchemaToCoSchema(enrichedSchema) as any;

  return enrichedSchema;
}

export const coMapDefiner = <Shape extends z.core.$ZodLooseShape>(
  shape: Shape,
): CoMapSchema<Shape> => {
  const objectSchema = z.object(shape).meta({
    collaborative: true,
  });

  return enrichCoMapSchema(objectSchema);
};

function enrichAccountSchema<Shape extends BaseAccountShape>(
  schema: z.ZodObject<Shape, z.core.$strip>,
) {
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    builtin: "Account",
    create: (...args: any[]) => {
      return coSchema.create(...args);
    },
    createAs: (...args: any[]) => {
      return coSchema.createAs(...args);
    },
    getMe: (...args: any[]) => {
      return coSchema.getMe(...args);
    },
    load: (...args: any[]) => {
      return coSchema.load(...args);
    },
    subscribe: (...args: any[]) => {
      return coSchema.subscribe(...args);
    },
    withHelpers: (helpers: (Self: z.core.$ZodType) => object) => {
      return Object.assign(schema, helpers(schema));
    },
    fromRaw: (...args: any[]) => {
      return coSchema.fromRaw(...args);
    },
    withMigration: (
      migration: (
        value: any,
        creationProps?: AccountCreationProps,
      ) => void | Promise<void>,
    ) => {
      (coSchema.prototype as Account).migrate = async function (
        this,
        creationProps,
      ) {
        await migration(this, creationProps);
      };

      return enrichedSchema;
    },
    getCoSchema: () => {
      return coSchema;
    },
  }) as unknown as AccountSchema<Shape>;

  // Needs to be derived from the enriched schema
  const coSchema = zodSchemaToCoSchema(enrichedSchema) as any;

  return enrichedSchema;
}

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
  const objectSchema = z.object(shape).meta({
    collaborative: true,
  });

  return enrichAccountSchema(objectSchema) as unknown as AccountSchema<Shape>;
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

function enrichCoListSchema<T extends z.core.$ZodType>(schema: z.ZodArray<T>) {
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    create: (...args: any[]) => {
      return coSchema.create(...args);
    },
    load: (...args: any[]) => {
      return coSchema.load(...args);
    },
    subscribe: (...args: any[]) => {
      return coSchema.subscribe(...args);
    },
    withHelpers: (helpers: (Self: z.core.$ZodType) => object) => {
      return Object.assign(schema, helpers(schema));
    },
    getCoSchema: () => {
      return coSchema;
    },
  }) as unknown as CoListSchema<T>;

  // Needs to be derived from the enriched schema
  const coSchema = zodSchemaToCoSchema(enrichedSchema) as any;

  return enrichedSchema;
}

export const coListDefiner = <T extends z.core.$ZodType>(
  element: T,
): CoListSchema<T> => {
  const arraySchema = z.array(element).meta({
    collaborative: true,
  });

  return enrichCoListSchema(arraySchema);
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
  const ehnancedShape = Object.assign(shape ?? {}, {
    name: z.string(),
    inbox: z.optional(z.string()),
    inboxInvite: z.optional(z.string()),
  });

  return coMapDefiner(ehnancedShape) as CoProfileSchema<Shape>;
};

function enrichCoFeedSchema<T extends z.core.$ZodType>(
  schema: z.ZodCustom<CoFeed<unknown>, unknown>,
  element: T,
) {
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    builtin: "CoFeed",
    element,
    create: (...args: any[]) => {
      return coSchema.create(...args);
    },
    load: (...args: any[]) => {
      return coSchema.load(...args);
    },
    subscribe: (...args: any[]) => {
      return coSchema.subscribe(...args);
    },
    withHelpers: (helpers: (Self: z.core.$ZodType) => object) => {
      return Object.assign(schema, helpers(schema));
    },
    getCoSchema: () => {
      return coSchema;
    },
  }) as unknown as CoFeedSchema<T>;

  // Needs to be derived from the enriched schema
  const coSchema = zodSchemaToCoSchema(enrichedSchema) as any;

  return enrichedSchema;
}

export const coFeedDefiner = <T extends z.core.$ZodType>(
  element: T,
): CoFeedSchema<T> => {
  return enrichCoFeedSchema(z.instanceof(CoFeed), element);
};

function enrichFileStreamSchema(schema: z.ZodCustom<FileStream, unknown>) {
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    builtin: "FileStream",
    create: (...args: any[]) => {
      return coSchema.create(...args);
    },
    createFromBlob: (...args: any[]) => {
      return coSchema.createFromBlob(...args);
    },
    load: (...args: any[]) => {
      return coSchema.load(...args);
    },
    loadAsBlob: (...args: any[]) => {
      return coSchema.loadAsBlob(...args);
    },
    subscribe: (...args: any[]) => {
      return coSchema.subscribe(...args);
    },
    getCoSchema: () => {
      return coSchema;
    },
  }) as unknown as FileStreamSchema;

  // Needs to be derived from the enriched schema
  const coSchema = zodSchemaToCoSchema(enrichedSchema) as any;

  return enrichedSchema;
}

export const coFileStreamDefiner = (): FileStreamSchema => {
  return enrichFileStreamSchema(z.instanceof(FileStream));
};

function enrichPlainTextSchema(schema: z.ZodCustom<CoPlainText, unknown>) {
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    builtin: "CoPlainText",
    create: (...args: any[]) => {
      return coSchema.create(...args);
    },
    load: (...args: any[]) => {
      return coSchema.load(...args);
    },
    subscribe: (...args: any[]) => {
      return coSchema.subscribe(...args);
    },
    fromRaw: (...args: any[]) => {
      return coSchema.fromRaw(...args);
    },
    getCoSchema: () => {
      return coSchema;
    },
  }) as unknown as PlainTextSchema;

  // Needs to be derived from the enriched schema
  const coSchema = zodSchemaToCoSchema(enrichedSchema) as any;

  return enrichedSchema;
}

export const coPlainTextDefiner = (): PlainTextSchema => {
  return enrichPlainTextSchema(z.instanceof(CoPlainText));
};

function enrichRichTextSchema(schema: z.ZodCustom<CoRichText, unknown>) {
  const enrichedSchema = Object.assign(schema, {
    collaborative: true,
    builtin: "CoRichText",
    create: (...args: any[]) => {
      return coSchema.create(...args);
    },
    load: (...args: any[]) => {
      return coSchema.load(...args);
    },
    subscribe: (...args: any[]) => {
      return coSchema.subscribe(...args);
    },
    getCoSchema: () => {
      return coSchema;
    },
  }) as unknown as RichTextSchema;

  // Needs to be derived from the enriched schema
  const coSchema = zodSchemaToCoSchema(enrichedSchema) as any;

  return enrichedSchema;
}

export const coRichTextDefiner = (): RichTextSchema => {
  return enrichRichTextSchema(z.instanceof(CoRichText));
};

export const coImageDefiner = (): typeof ImageDefinition => {
  return ImageDefinition;
};

export const coOptionalDefiner = <T extends AnyCoSchema>(
  schema: T,
): z.ZodOptional<T> => {
  return z.optional(schema);
};
