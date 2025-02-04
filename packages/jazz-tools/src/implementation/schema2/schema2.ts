import { makeCoMapValueClass } from "./makeCoMapValueClass.js";

type Optionable = { optional<S extends Schema>(this: S): Optional<S> };

export const sym = {
  SchemaType: " type" as const,
  CoMapEntries: " entries" as const,
  CoListItems: " items" as const,
  ValueClass: " ValueClass" as const,
  Schema: " schema" as const,
};

type StringPrimitive = { [sym.SchemaType]: "string" } & Optionable;
type NumberPrimitive = { [sym.SchemaType]: "number" } & Optionable;
type BooleanPrimitive = { [sym.SchemaType]: "boolean" } & Optionable;
type NullPrimitive = { [sym.SchemaType]: "null" } & Optionable;
type LiteralPrimitive<T extends string | number | boolean | null> = {
  [sym.SchemaType]: "literal";
  value: T;
} & Optionable;

type PrimitiveSchema =
  | StringPrimitive
  | NumberPrimitive
  | BooleanPrimitive
  | NullPrimitive
  | LiteralPrimitive<any>;
type JSONSchema =
  | PrimitiveSchema
  | ArraySchema<any>
  | ObjectSchema<any>
  | Encoded<any>;

type Encoded<T> = {
  [sym.SchemaType]: "encoded";
  encode: (value: T) => InferJSON<JSONSchema>;
  decode: (value: InferJSON<JSONSchema>) => T;
} & Optionable;
type EncodedDate = Encoded<Date>;

type ArraySchema<S extends JSONSchema> = {
  [sym.SchemaType]: "array";
  items: S;
} & Optionable;
type ObjectSchema<S extends { [key: string]: JSONSchema }> = {
  [sym.SchemaType]: "object";
  entries: S;
} & Optionable;

type Union<Ss extends Schema[]> = {
  [sym.SchemaType]: "union";
  schemas: Ss;
} & Optionable;
type Optional<S extends Schema> = Union<[S, NullPrimitive]>;

export type CoMapDef<E extends { [key: string]: Schema }> = {
  [sym.SchemaType]: "comap";
  [sym.CoMapEntries]: E;
};

type CoMap<E extends { [key: string]: Schema }> = CoMapDef<E> & {
  create(init: E): Loaded<CoMap<E>>;
} & Optionable;

type CoListDef<I extends Schema> = {
  [sym.SchemaType]: "colist";
  [sym.CoListItems]: I;
};

type CoList<I extends Schema> = CoListDef<I> & {
  create(init: I[]): Loaded<CoList<I>>;
} & Optionable;

export type CoValueDef = CoMapDef<any> | CoListDef<any>;
type Schema = CoValueDef | JSONSchema | Union<Schema[]>;

function optionalThis<S extends Schema>(this: S): Optional<S> {
  return {
    [sym.SchemaType]: "union",
    schemas: [this, jBase.null()],
    optional: optionalThis,
  };
}

const jBase = {
  string(): StringPrimitive {
    return { [sym.SchemaType]: "string", optional: optionalThis };
  },
  number(): NumberPrimitive {
    return { [sym.SchemaType]: "number", optional: optionalThis };
  },
  boolean(): BooleanPrimitive {
    return { [sym.SchemaType]: "boolean", optional: optionalThis };
  },
  null(): NullPrimitive {
    return { [sym.SchemaType]: "null", optional: optionalThis };
  },
  date(): EncodedDate {
    return {
      [sym.SchemaType]: "encoded",
      encode: (value) => value.toISOString(),
      decode: (value) => new Date(value),
      optional: optionalThis,
    };
  },
  literal<T extends string | number | boolean | null>(
    value: T,
  ): LiteralPrimitive<T> {
    return { [sym.SchemaType]: "literal", value, optional: optionalThis };
  },
  array<I extends JSONSchema>(items: I): ArraySchema<I> {
    return { [sym.SchemaType]: "array", items, optional: optionalThis };
  },
  object<E extends { [key: string]: JSONSchema }>(entries: E): ObjectSchema<E> {
    return { [sym.SchemaType]: "object", entries, optional: optionalThis };
  },
  CoMap<E extends { [key: string]: Schema }>(entries: E): CoMap<E> {
    const coMapSchema = {
      [sym.SchemaType]: "comap" as const,
      [sym.CoMapEntries]: entries,
      [sym.ValueClass]: null as any,
      optional: optionalThis,
    };

    coMapSchema[sym.ValueClass] = makeCoMapValueClass(coMapSchema);

    return coMapSchema;
  },
  CoList<I extends Schema>(items: I): CoList<I> {
    return {
      [sym.SchemaType]: "colist",
      [sym.CoListItems]: items,
      optional: optionalThis,
    };
  },
  optional<S extends Schema>(schema: S): Optional<S> {
    return {
      [sym.SchemaType]: "union",
      schemas: [schema, jBase.null()],
      optional: optionalThis,
    };
  },
};

export const ImageDef = jBase.CoMap({
  originalResolution: jBase.string(),
});

export const j = {
  ...jBase,
};

type InferJSON<T extends JSONSchema> = T extends Encoded<infer T>
  ? T
  : T extends LiteralPrimitive<infer T>
    ? T
    : T extends StringPrimitive
      ? string
      : T extends NumberPrimitive
        ? number
        : T extends BooleanPrimitive
          ? boolean
          : T extends NullPrimitive
            ? null
            : T extends LiteralPrimitive<infer L>
              ? L
              : T extends ArraySchema<infer I>
                ? Loaded<I>[]
                : T extends ObjectSchema<infer E>
                  ? { [K in keyof E]: Loaded<E[K]> }
                  : never;

export type Loaded<T extends Schema> = T extends CoMapDef<infer E>
  ? { [K in keyof E]: Loaded<E[K]> }
  : T extends CoListDef<infer I>
    ? Loaded<I>[]
    : // T extends Union<infer Ss> ? Loaded<Ss[number]> :
      T extends JSONSchema
      ? InferJSON<T>
      : never;

export const Message = j.CoMap({
  text: j.string(),
  image: j.media.ImageDef.optional(),
});
