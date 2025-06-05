import {
  type Account,
  AccountCreationProps,
  AccountSchema,
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
  ImageDefinition,
  PlainTextSchema,
  Simplify,
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

function enrichAccountSchema<
  Shape extends {
    profile: AnyCoMapSchema<{
      name: z.core.$ZodString<string>;
      inbox?: z.core.$ZodOptional<z.core.$ZodString>;
      inboxInvite?: z.core.$ZodOptional<z.core.$ZodString>;
    }>;
    root: AnyCoMapSchema;
  },
>(schema: z.ZodObject<Shape, z.core.$strip>) {
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
