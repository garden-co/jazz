import {
  CoValueUniqueness,
  CryptoProvider,
  LocalNode,
  RawAccount,
  RawCoList,
  RawCoMap,
  RawCoPlainText,
} from "cojson";
import z from "zod";
import {
  Account,
  AccountClass,
  AnonymousJazzAgent,
  CoFeed,
  CoList,
  CoMap,
  CoPlainText,
  CoValueClass,
  CoValueFromRaw,
  FileStream,
  Group,
  ID,
  Profile,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SchemaUnion,
  Simplify,
  SubscribeListenerOptions,
  coField,
  isCoValueClass,
} from "../internal.js";

// conversion from zod to old jazz-tools schemas

export type CoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  OutExtra extends Record<string, unknown> = Record<string, unknown>,
> = z.core.$ZodObject<Shape, OutExtra> &
  z.$ZodTypeDiscriminable & {
    collaborative: true;

    create: (
      init: Simplify<
        {
          [key in keyof Shape as Shape[key] extends z.ZodOptional<any>
            ? key
            : never]?: InstanceOrPrimitiveOfSchema<Shape[key]>;
        } & {
          [key in keyof Shape as Shape[key] extends z.ZodOptional<any>
            ? never
            : key]: InstanceOrPrimitiveOfSchema<Shape[key]>;
        }
      >,
      options?:
        | {
            owner: Account | Group;
            unique?: CoValueUniqueness["uniqueness"];
          }
        | Account
        | Group,
    ) => (Shape extends Record<string, never>
      ? {}
      : {
          -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<
            Shape[key]
          >;
        }) &
      (unknown extends OutExtra[string]
        ? {}
        : {
            [key: string]: OutExtra[string];
          }) &
      CoMap;

    load<const R extends RefsToResolve<CoMapInstance<Shape>> = true>(
      id: ID<CoMapInstance<Shape>>,
      options?: {
        resolve?: RefsToResolveStrict<CoMapInstance<Shape>, R>;
        loadAs?: Account | AnonymousJazzAgent;
      },
    ): Promise<Resolved<CoMapInstance<Shape>, R> | null>;

    subscribe<const R extends RefsToResolve<CoMapInstance<Shape>> = true>(
      id: ID<CoMapInstance<Shape>>,
      options: SubscribeListenerOptions<CoMapInstance<Shape>, R>,
      listener: (
        value: Resolved<CoMapInstance<Shape>, R>,
        unsubscribe: () => void,
      ) => void,
    ): () => void;

    findUnique(
      unique: CoValueUniqueness["uniqueness"],
      ownerID: ID<Account> | ID<Group>,
      as?: Account | Group | AnonymousJazzAgent,
    ): ID<CoMapInstance<Shape>>;

    catchall<T extends z.core.$ZodType>(
      schema: T,
    ): CoMapSchema<Shape, Record<string, T["_zod"]["output"]>>;

    withHelpers<S extends z.core.$ZodType, T extends object>(
      this: S,
      helpers: (Self: S) => T,
    ): WithHelpers<S, T>;
  };

// defining an extra type for this, otherwise CoMapSchema<...> & {...} often
// gets expanded into a n inferred type that's too long for typescript to print
export type WithHelpers<
  Base extends z.core.$ZodType,
  Helpers extends object,
> = Base & Helpers;

export type AccountSchema<
  Shape extends {
    profile: AnyCoMapSchema<{ name: z.core.$ZodString<string> }>;
    root: AnyCoMapSchema;
  } = {
    profile: CoMapSchema<{ name: z.core.$ZodString<string> }>;
    root: CoMapSchema<{}>;
  },
> = Omit<CoMapSchema<Shape>, "create"> & {
  builtin: "Account";

  create: (options: {
    creationProps?: { name: string };
    crypto?: CryptoProvider;
  }) => Promise<AccountInstance<Shape>>;

  createAs: (
    as: Account,
    options: {
      creationProps?: { name: string };
    },
  ) => Promise<AccountInstance<Shape>>;

  getMe: () => AccountInstance<Shape>;

  withMigration(
    migration: (
      account: InstanceOrPrimitiveOfSchema<AccountSchema<Shape>>,
      creationProps?: { name: string },
    ) => void,
  ): AccountSchema<Shape>;
};

export type CoRecordSchema<
  K extends z.core.$ZodString<string>,
  V extends z.core.$ZodType,
> = z.core.$ZodRecord<K, V> & {
  collaborative: true;

  create: (
    init: Simplify<{
      [key in z.output<K>]?: InstanceOrPrimitiveOfSchema<V>;
    }>,
    options?:
      | {
          owner: Account | Group;
          unique?: CoValueUniqueness["uniqueness"];
        }
      | Account
      | Group,
  ) => {
    [key in z.output<K>]: InstanceOrPrimitiveOfSchema<V>;
  } & CoMap;

  load<const R extends RefsToResolve<CoRecordInstance<K, V>> = true>(
    id: ID<CoRecordInstance<K, V>>,
    options?: {
      resolve?: RefsToResolveStrict<CoRecordInstance<K, V>, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<CoRecordInstance<K, V>, R> | null>;

  subscribe<const R extends RefsToResolve<CoRecordInstance<K, V>> = true>(
    id: ID<CoRecordInstance<K, V>>,
    options: SubscribeListenerOptions<CoRecordInstance<K, V>, R>,
    listener: (
      value: Resolved<CoRecordInstance<K, V>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;

  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ): ID<CoRecordInstance<K, V>>;

  withHelpers<S extends z.core.$ZodType, T extends object>(
    this: S,
    helpers: (Self: S) => T,
  ): WithHelpers<S, T>;
};

export type CoListSchema<T extends z.core.$ZodType> = z.core.$ZodArray<T> & {
  collaborative: true;

  create: (
    items: InstanceOrPrimitiveOfSchema<T>[],
    options?: { owner: Account | Group } | Account | Group,
  ) => CoList<InstanceOrPrimitiveOfSchema<T>>;

  load<const R extends RefsToResolve<CoListInstance<T>> = true>(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoListInstance<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<CoListInstance<T>, R> | null>;

  subscribe<const R extends RefsToResolve<CoListInstance<T>> = true>(
    id: string,
    options: SubscribeListenerOptions<CoListInstance<T>, R>,
    listener: (
      value: Resolved<CoListInstance<T>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;

  withHelpers<S extends z.core.$ZodType, T extends object>(
    this: S,
    helpers: (Self: S) => T,
  ): WithHelpers<S, T>;
};

export type CoFeedSchema<T extends z.core.$ZodType> = z.core.$ZodCustom<
  CoFeed<InstanceOfSchema<T>>,
  unknown
> & {
  collaborative: true;
  builtin: "CoFeed";
  element: T;

  create(
    init: InstanceOrPrimitiveOfSchema<T>[],
    options?: { owner: Account | Group } | Account | Group,
  ): CoFeedInstance<T>;

  load<const R extends RefsToResolve<CoFeedInstance<T>> = true>(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoFeedInstance<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<CoFeedInstance<T>, R> | null>;

  subscribe(
    id: string,
    listener: (
      value: Resolved<CoFeedInstance<T>, true>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe<const R extends RefsToResolve<CoFeedInstance<T>> = true>(
    id: string,
    options: SubscribeListenerOptions<CoFeedInstance<T>, R>,
    listener: (
      value: Resolved<CoFeedInstance<T>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
};

export type FileStreamSchema = z.core.$ZodCustom<FileStream, unknown> & {
  collaborative: true;
  builtin: "FileStream";
  create: (typeof FileStream)["create"];
};

export type PlainTextSchema = z.core.$ZodCustom<CoPlainText, unknown> & {
  collaborative: true;
  builtin: "CoPlainText";
  create(
    text: string,
    options?: { owner: Account | Group } | Account | Group,
  ): CoPlainText;
  load(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
  ): Promise<CoPlainText>;
  subscribe(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
    listener: (value: CoPlainText, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(
    id: string,
    listener: (value: CoPlainText, unsubscribe: () => void) => void,
  ): () => void;
  fromRaw(raw: RawCoPlainText): CoPlainText;
};

let coSchemasForZodSchemas = new Map<z.core.$ZodType, CoValueClass>();

export function zodSchemaToCoSchema<
  S extends
    | z.core.$ZodType
    | (z.core.$ZodObject<any, any> & {
        builtin: "Account";
        migration?: (account: any, creationProps?: { name: string }) => void;
      })
    | (z.core.$ZodCustom<any, any> & { builtin: "FileStream" })
    | (z.core.$ZodCustom<any, any> & {
        builtin: "CoFeed";
        element: z.core.$ZodType;
      }),
>(schema: S): CoValueClassFromZodSchema<S> {
  const coSchema = tryZodSchemaToCoSchema(schema);
  if (!coSchema) {
    throw new Error(`Unsupported zod type: ${schema._zod.def.type}`);
  }
  return coSchema;
}

export function anySchemaToCoSchema<
  S extends
    | CoValueClass
    | z.core.$ZodType
    | (z.core.$ZodObject<any, any> & {
        builtin: "Account";
        migration?: (account: any, creationProps?: { name: string }) => void;
      })
    | (z.core.$ZodCustom<any, any> & { builtin: "FileStream" })
    | (z.core.$ZodCustom<any, any> & {
        builtin: "CoFeed";
        element: z.core.$ZodType;
      }),
>(
  schema: S,
): S extends CoValueClass
  ? S
  : S extends z.core.$ZodType
    ? CoValueClassFromZodSchema<S>
    : never {
  if (isCoValueClass(schema)) {
    return schema as any;
  } else {
    return zodSchemaToCoSchema(schema as any) as any;
  }
}

type ZodPrimitiveSchema =
  | z.core.$ZodString
  | z.core.$ZodNumber
  | z.core.$ZodBoolean
  | z.core.$ZodNull
  | z.core.$ZodDate
  | z.core.$ZodLiteral;

export function zodSchemaToCoSchemaOrKeepPrimitive<S extends z.core.$ZodType>(
  schema: S,
):
  | CoValueClassFromZodSchema<S>
  | ZodPrimitiveSchema
  | z.core.$ZodOptional<z.core.$ZodType>
  | z.core.$ZodTuple<z.core.$ZodType[]>
  | z.core.$ZodUnion<z.core.$ZodType[]>
  | (z.core.$ZodCustom<any, any> & { builtin: any }) {
  const coSchema = tryZodSchemaToCoSchema(schema);
  if (!coSchema) {
    return schema as any;
  }
  return coSchema;
}

export function tryZodSchemaToCoSchema<S extends z.core.$ZodType>(
  schema: S,
): CoValueClassFromZodSchema<S> | null {
  if ("collaborative" in schema && schema.collaborative) {
    if (coSchemasForZodSchemas.has(schema)) {
      return coSchemasForZodSchemas.get(schema) as CoValueClassFromZodSchema<S>;
    }

    if (schema instanceof z.core.$ZodObject) {
      const def = (schema as z.core.$ZodObject)._zod.def;

      const ClassToExtend =
        "builtin" in schema && schema.builtin === "Account" ? Account : CoMap;

      const coSchema = class ZCoMap extends ClassToExtend {
        constructor(options: { fromRaw: RawCoMap } | undefined) {
          super(options);
          for (const [field, fieldType] of Object.entries(def.shape)) {
            (this as any)[field] = fieldDef(
              zodSchemaToCoSchemaOrKeepPrimitive(fieldType),
            );
          }
          if (def.catchall) {
            (this as any)[coField.items] = fieldDef(
              zodSchemaToCoSchemaOrKeepPrimitive(def.catchall),
            );
          }
        }
      };

      if ("migration" in schema) {
        const migration = schema.migration;
        if (typeof migration !== "function") {
          throw new Error("migration must be a function");
        }
        (coSchema.prototype as Account).migrate = async function (
          this,
          creationProps,
        ) {
          await migration(this, creationProps);
        };
      }

      coSchemasForZodSchemas.set(schema, coSchema as unknown as CoValueClass);
      return coSchema as unknown as CoValueClassFromZodSchema<S>;
    } else if (schema instanceof z.core.$ZodArray) {
      const def = (schema as z.core.$ZodArray)._zod.def;
      const coSchema = class ZCoList extends CoList {
        constructor(options: { fromRaw: RawCoList } | undefined) {
          super(options);
          (this as any)[coField.items] = fieldDef(
            zodSchemaToCoSchemaOrKeepPrimitive(def.element),
          );
        }
      };

      coSchemasForZodSchemas.set(schema, coSchema);
      return coSchema as unknown as CoValueClassFromZodSchema<S>;
    } else if (schema._zod.def.type === "custom") {
      if ("builtin" in schema) {
        if (schema.builtin === "CoFeed" && "element" in schema) {
          return CoFeed.Of(
            fieldDef(
              zodSchemaToCoSchemaOrKeepPrimitive(
                schema.element as z.core.$ZodType,
              ),
            ),
          ) as unknown as CoValueClassFromZodSchema<S>;
        } else if (schema.builtin === "FileStream") {
          return FileStream as unknown as CoValueClassFromZodSchema<S>;
        } else {
          throw new Error(`Unsupported builtin type: ${schema.builtin}`);
        }
      } else {
        throw new Error(`Unsupported custom zod type`);
      }
    } else {
      throw new Error(
        `Unsupported zod CoValue type for top-level schema: ${schema._zod.def.type}`,
      );
    }
  } else if (schema instanceof z.core.$ZodDiscriminatedUnion) {
    if (isUnionOfCoMapsDeeply(schema)) {
      return SchemaUnion.Of(
        schemaUnionDiscriminatorFor(schema),
      ) as unknown as CoValueClassFromZodSchema<S>;
    } else {
      throw new Error(
        "z.discriminatedUnion() of non-collaborative types is not supported as a top-level schema",
      );
    }
  } else {
    return null;
  }
}

export function fieldDef(
  schema:
    | CoValueClass
    | ZodPrimitiveSchema
    | z.core.$ZodOptional<z.core.$ZodType>
    | z.core.$ZodTuple<z.core.$ZodType[]>
    | z.core.$ZodUnion<z.core.$ZodType[]>
    | (z.core.$ZodCustom<any, any> & { builtin: any }),
) {
  if (isCoValueClass(schema)) {
    return coField.ref(schema);
  } else {
    if ("_zod" in schema) {
      if (schema._zod.def.type === "optional") {
        const inner = zodSchemaToCoSchemaOrKeepPrimitive(
          schema._zod.def.innerType,
        );
        if (isCoValueClass(inner)) {
          return coField.ref(inner, { optional: true });
        } else {
          return fieldDef(inner);
        }
      } else if (schema._zod.def.type === "string") {
        return coField.string;
      } else if (schema._zod.def.type === "number") {
        return coField.number;
      } else if (schema._zod.def.type === "boolean") {
        return coField.boolean;
      } else if (schema._zod.def.type === "null") {
        return coField.null;
      } else if (schema._zod.def.type === "date") {
        return coField.Date;
      } else if (schema._zod.def.type === "literal") {
        if (
          schema._zod.def.values.some(
            (literal) => typeof literal === "undefined",
          )
        ) {
          throw new Error("z.literal() with undefined is not supported");
        }
        if (schema._zod.def.values.some((literal) => literal === null)) {
          throw new Error("z.literal() with null is not supported");
        }
        if (
          schema._zod.def.values.some((literal) => typeof literal === "bigint")
        ) {
          throw new Error("z.literal() with bigint is not supported");
        }
        return coField.literal(
          ...(schema._zod.def.values as Exclude<
            (typeof schema._zod.def.values)[number],
            undefined | null | bigint
          >[]),
        );
      } else if (schema._zod.def.type === "tuple") {
        return coField.json();
      } else if (schema._zod.def.type === "custom") {
        if ("builtin" in schema) {
          return fieldDef(schema.builtin);
        } else {
          throw new Error(`Unsupported custom zod type`);
        }
      } else if (schema._zod.def.type === "union") {
        if (isUnionOfPrimitivesDeeply(schema)) {
          return coField.json();
        } else if (isUnionOfCoMapsDeeply(schema)) {
          return coField.ref<CoValueClass<CoMap>>(
            schemaUnionDiscriminatorFor(schema),
          );
        } else {
          throw new Error(
            "z.union()/z.discriminatedUnion() of mixed collaborative and non-collaborative types is not supported",
          );
        }
      } else {
        throw new Error(
          `Unsupported zod type: ${(schema._zod.def as any).type}`,
        );
      }
    } else {
      throw new Error(`Unsupported zod type: ${schema}`);
    }
  }
}

function isUnionOfCoMapsDeeply(
  schema: z.core.$ZodType,
): schema is z.core.$ZodDiscriminatedUnion {
  if (schema instanceof z.core.$ZodUnion) {
    return schema._zod.def.options.every(isCoMapOrUnionOfCoMapsDeeply);
  } else {
    return false;
  }
}

function isCoMapOrUnionOfCoMapsDeeply(
  schema: z.core.$ZodType,
): schema is z.core.$ZodDiscriminatedUnion {
  if (
    schema instanceof z.core.$ZodObject &&
    "collaborative" in schema &&
    schema.collaborative
  ) {
    return true;
  } else if (schema instanceof z.core.$ZodUnion) {
    return schema._zod.def.options.every(isCoMapOrUnionOfCoMapsDeeply);
  } else {
    return false;
  }
}

function isUnionOfPrimitivesDeeply(schema: z.core.$ZodType) {
  if (schema instanceof z.core.$ZodUnion) {
    return schema._zod.def.options.every(isUnionOfPrimitivesDeeply);
  } else {
    return !("collaborative" in schema);
  }
}

function schemaUnionDiscriminatorFor(schema: z.core.$ZodDiscriminatedUnion) {
  if (isUnionOfCoMapsDeeply(schema)) {
    if (!schema._zod.disc || schema._zod.disc.size == 0) {
      throw new Error(
        "z.union() of collaborative types is not supported, use z.discriminatedUnion() instead",
      );
    }
    if (![...schema._zod.disc.keys()].every((key) => typeof key === "string")) {
      throw new Error(
        "z.discriminatedUnion() of collaborative types with non-string discriminator key is not supported",
      );
    }
    if (
      ![...schema._zod.disc.values()].every((v) =>
        [...v.values].every((value) => typeof value === "string"),
      )
    ) {
      throw new Error(
        "z.discriminatedUnion() of collaborative types with non-string discriminator value is not supported",
      );
    }

    const flattendOptions = [
      ...schema._zod.def.options.flatMap((option) => {
        if (option._zod.def.type === "object") {
          return [option];
        } else if (option._zod.def.type === "union") {
          return [...(option as z.core.$ZodUnion)._zod.def.options];
        } else {
          throw new Error(
            "Unsupported zod type in z.discriminatedUnion() of collaborative types",
          );
        }
      }),
    ];

    const determineSchema = (_raw: RawCoMap | RawAccount | RawCoList) => {
      if (_raw instanceof RawCoList) {
        throw new Error(
          "z.discriminatedUnion() of collaborative types is not supported for CoLists",
        );
      }
      let remainingOptions = [...flattendOptions];
      for (const discriminator of schema._zod.disc.keys()) {
        const discriminatorValue = (_raw as RawCoMap).get(
          discriminator as string,
        );
        if (typeof discriminatorValue !== "string") {
          throw new Error("Discriminator must be a string");
        }
        remainingOptions = remainingOptions.filter((option) => {
          if (option._zod.def.type !== "object") {
            return false;
          }
          const discriminatorDef = (option as z.core.$ZodObject)._zod.def.shape[
            discriminator as string
          ];
          if (!discriminatorDef) {
            return false;
          }

          console.log(discriminatorDef._zod.def);

          if (discriminatorDef._zod.def.type !== "literal") {
            console.warn(
              "Non-literal discriminator found in z.discriminatedUnion() of collaborative types",
            );
            return false;
          }
          if (
            (discriminatorDef._zod.def as z.core.$ZodLiteralDef).values
              .length !== 1
          ) {
            console.warn(
              "Literal discriminator with more than one value found in z.discriminatedUnion() of collaborative types",
            );
            return false;
          }
          return (
            (discriminatorDef._zod.def as z.core.$ZodLiteralDef).values[0] ===
            discriminatorValue
          );
        });
        if (remainingOptions.length === 1) {
          return zodSchemaToCoSchema(remainingOptions[0]!);
        }
      }
      throw new Error(
        "z.discriminatedUnion() of collaborative types with no matching discriminator value found",
      );
    };

    return determineSchema;
  } else {
    throw new Error(
      "z.discriminatedUnion() of non-collaborative types is not supported",
    );
  }
}

// these are less precise versions of CoMapSchema, CoRecordSchema, and CoListSchema
// they are used to infer the types of the zod schemas from the cojson schemas
// without inducing all kinds of circularity issues

export type AnyCoMapSchema<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
  OutExtra extends Record<string, unknown> = Record<string, unknown>,
> = z.core.$ZodObject<Shape, OutExtra> & { collaborative: true };

export type AnyAccountSchema<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
> = z.core.$ZodObject<Shape> & {
  collaborative: true;
  builtin: "Account";
};

export type AnyCoRecordSchema<
  K extends z.core.$ZodString<string> = z.core.$ZodString<string>,
  V extends z.core.$ZodType = z.core.$ZodType,
> = z.core.$ZodRecord<K, V> & { collaborative: true };

export type AnyCoListSchema<T extends z.core.$ZodType = z.core.$ZodType> =
  z.core.$ZodArray<T> & { collaborative: true };

export type AnyCoFeedSchema<T extends z.core.$ZodType = z.core.$ZodType> =
  z.core.$ZodCustom<any, unknown> & {
    collaborative: true;
    builtin: "CoFeed";
    element: T;
  };

export type AnyCoUnionSchema = z.core.$ZodDiscriminatedUnion<
  (
    | (z.core.$ZodType & { collaborative: true })
    | z.core.$ZodDiscriminatedUnion
  )[]
>;

export type CoValueOrZodSchema =
  | CoValueClass
  | AnyCoMapSchema
  | AnyCoFeedSchema
  | AnyCoRecordSchema
  | AnyCoListSchema
  | AnyCoUnionSchema;

export type CoValueClassFromZodSchema<S extends z.core.$ZodType> = CoValueClass<
  InstanceOfSchema<S>
> &
  CoValueFromRaw<InstanceOfSchema<S>> &
  (S extends AnyAccountSchema
    ? {
        fromRaw: <A extends Account>(
          this: AccountClass<A>,
          raw: RawAccount,
        ) => A;
        fromNode: <A extends Account>(
          this: AccountClass<A>,
          node: LocalNode,
        ) => A;
      }
    : {});

export type CoMapInstance<Shape extends z.core.$ZodLooseShape> = {
  -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
} & CoMap;

export type CoRecordInstance<
  K extends z.core.$ZodString<string>,
  V extends z.core.$ZodType,
> = {
  [key in z.output<K>]: InstanceOrPrimitiveOfSchema<V>;
} & CoMap;

export type AccountInstance<Shape extends z.core.$ZodLooseShape> = {
  -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
} & Account;

export type CoListInstance<T extends z.core.$ZodType> = CoList<
  InstanceOrPrimitiveOfSchema<T>
>;

export type CoFeedInstance<T extends z.core.$ZodType> = CoFeed<
  InstanceOrPrimitiveOfSchema<T>
>;

export type InstanceOrPrimitiveOfSchema<
  S extends CoValueClass | z.core.$ZodType,
> = S extends z.core.$ZodType
  ? S extends z.core.$ZodObject<infer Shape> & {
      collaborative: true;
      builtin: "Account";
    }
    ? {
        -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
      } & { profile: Profile } & Account
    : S extends z.core.$ZodRecord<infer K, infer V> & {
          collaborative: true;
        }
      ? {
          -readonly [key in z.output<K> &
            string]: InstanceOrPrimitiveOfSchema<V>;
        } & CoMap
      : S extends AnyCoMapSchema<infer Shape, infer OutExtra>
        ? {
            -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<
              Shape[key]
            >;
          } & (unknown extends OutExtra[string]
            ? {}
            : {
                [key: string]: OutExtra[string];
              }) &
            CoMap
        : S extends AnyCoListSchema<infer T>
          ? CoList<InstanceOrPrimitiveOfSchema<T>>
          : S extends AnyCoFeedSchema<infer T>
            ? CoFeed<InstanceOrPrimitiveOfSchema<T>>
            : S extends PlainTextSchema
              ? CoPlainText
              : S extends z.core.$ZodOptional<infer Inner>
                ? InstanceOrPrimitiveOfSchema<Inner> | undefined
                : S extends z.core.$ZodTuple<infer Items>
                  ? {
                      [key in keyof Items]: InstanceOrPrimitiveOfSchema<
                        Items[key]
                      >;
                    }
                  : S extends z.core.$ZodUnion<infer Members>
                    ? InstanceOrPrimitiveOfSchema<Members[number]>
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
                              : never
  : S extends CoValueClass
    ? InstanceType<S>
    : never;

// same as above type but excluding primitives
export type InstanceOfSchema<S extends CoValueClass | z.core.$ZodType> =
  S extends z.core.$ZodType
    ? S extends z.core.$ZodObject<infer Shape> & {
        collaborative: true;
        builtin: "Account";
      }
      ? {
          [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
        } & Account
      : S extends z.core.$ZodRecord<infer K, infer V> & {
            collaborative: true;
          }
        ? {
            [key in z.output<K> & string]: InstanceOrPrimitiveOfSchema<V>;
          } & CoMap
        : S extends AnyCoMapSchema<infer Shape, infer OutExtra>
          ? {
              [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
            } & (unknown extends OutExtra[string]
              ? {}
              : {
                  [key: string]: OutExtra[string];
                }) &
              CoMap
          : S extends AnyCoListSchema<infer T>
            ? CoList<InstanceOrPrimitiveOfSchema<T>>
            : S extends AnyCoFeedSchema<infer T>
              ? CoFeed<InstanceOrPrimitiveOfSchema<T>>
              : S extends z.core.$ZodOptional<infer Inner>
                ? InstanceOrPrimitiveOfSchema<Inner>
                : S extends z.core.$ZodUnion<infer Members>
                  ? InstanceOrPrimitiveOfSchema<Members[number]>
                  : never
    : S extends CoValueClass
      ? InstanceType<S>
      : never;

export type Loaded<
  T extends
    | CoValueClass
    | AnyCoMapSchema
    | AnyAccountSchema
    | AnyCoRecordSchema
    | AnyCoListSchema
    | AnyCoFeedSchema
    | AnyCoUnionSchema
    | PlainTextSchema,
  R extends RefsToResolve<InstanceOfSchema<T>> = true,
> = Resolved<InstanceOfSchema<T>, R>;
