import { RawCoList, RawCoMap } from "cojson";
import {
  Account,
  AccountSchema,
  CoDiscriminatedUnionSchema,
  CoFeed,
  CoFeedSchema,
  CoList,
  CoListSchema,
  CoMap,
  CoMapSchema,
  CoPlainText,
  CoRichText,
  CoValueClass,
  FileStream,
  FileStreamSchema,
  CoVectorSchema,
  PlainTextSchema,
  SchemaUnion,
  isCoValueClass,
  Group,
  CoVector,
} from "../../../internal.js";
import { coField } from "../../schema.js";

import { CoreCoValueSchema } from "../schemaTypes/CoValueSchema.js";
import { RichTextSchema } from "../schemaTypes/RichTextSchema.js";
import { GroupSchema } from "../schemaTypes/GroupSchema.js";
import { schemaUnionDiscriminatorFor } from "../unionUtils.js";
import {
  AnyCoreCoValueSchema,
  AnyZodOrCoValueSchema,
  CoValueClassFromAnySchema,
  CoValueClassOrSchema,
  CoValueSchemaFromCoreSchema,
} from "../zodSchema.js";
import {
  SchemaField,
  schemaFieldToCoFieldDef,
} from "./schemaFieldToCoFieldDef.js";

const hydratedSchemaCache = new WeakMap<object, unknown>();

type FieldEntry = [PropertyKey, () => unknown];

function getHydrationCacheKey(schema: AnyCoreCoValueSchema) {
  if (
    (typeof schema === "object" && schema !== null) ||
    typeof schema === "function"
  ) {
    return schema as object;
  }
  return undefined;
}

function buildFieldEntries(def: {
  shape: Record<string, SchemaField>;
  catchall?: SchemaField;
}): FieldEntry[] {
  const entries: FieldEntry[] = [];
  const shape = def.shape;

  for (const fieldName of Object.keys(shape)) {
    const descriptor = Object.getOwnPropertyDescriptor(shape, fieldName);
    if (!descriptor) continue;

    const resolveSchemaField = () =>
      (descriptor.get
        ? descriptor.get.call(shape)
        : descriptor.value) as SchemaField;

    entries.push([fieldName, makeLazyCoFieldDef(resolveSchemaField)]);
  }

  if (def.catchall) {
    entries.push([
      coField.items,
      makeLazyCoFieldDef(() => def.catchall as SchemaField),
    ]);
  }

  return entries;
}

type MutableTarget = Record<string | number | symbol, unknown>;

function assignFieldEntries(target: object, entries: FieldEntry[]) {
  const mutableTarget = target as MutableTarget;
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    if (!entry) continue;
    const [fieldName, getFieldDef] = entry;
    mutableTarget[fieldName] = getFieldDef();
  }
}

function makeLazyCoFieldDef(resolver: () => SchemaField) {
  let cached: unknown;
  let resolved = false;
  return () => {
    if (!resolved) {
      cached = schemaFieldToCoFieldDef(resolver());
      resolved = true;
    }

    return cached;
  };
}

function createFieldEntriesResolver(def: {
  shape: Record<string, SchemaField>;
  catchall?: SchemaField;
}) {
  let cachedEntries: FieldEntry[] | undefined;
  return () => {
    if (!cachedEntries) {
      cachedEntries = buildFieldEntries(def);
    }

    return cachedEntries;
  };
}

// Note: if you're editing this function, edit the `isAnyCoValueSchema`
// function in `zodReExport.ts` as well
export function isAnyCoValueSchema(
  schema: unknown,
): schema is AnyCoreCoValueSchema {
  return (
    typeof schema === "object" &&
    schema !== null &&
    "collaborative" in schema &&
    schema.collaborative === true
  );
}

export function isCoValueSchema(
  schema: AnyZodOrCoValueSchema | CoValueClass,
): schema is CoValueSchemaFromCoreSchema<AnyCoreCoValueSchema> {
  return isAnyCoValueSchema(schema) && "getCoValueClass" in schema;
}

/**
 * Convert a "core" CoValue schema into a CoValue schema.
 * See {@link CoreCoValueSchema} for more information.
 *
 * @returns The CoValue schema matching the provided CoreCoValueSchema
 */
export function hydrateCoreCoValueSchema<S extends AnyCoreCoValueSchema>(
  schema: S,
): CoValueSchemaFromCoreSchema<S> {
  if (isCoValueSchema(schema)) {
    // If the schema is already a CoValue schema, return it as is
    return schema as any;
  }

  const cacheKey = getHydrationCacheKey(schema);

  if (cacheKey) {
    const cached = hydratedSchemaCache.get(cacheKey) as
      | CoValueSchemaFromCoreSchema<AnyCoreCoValueSchema>
      | undefined;
    if (cached) {
      return cached as CoValueSchemaFromCoreSchema<S>;
    }
  }

  const hydratedSchema = (() => {
    if (schema.builtin === "CoOptional") {
      throw new Error(
        `co.optional() of collaborative types is not supported as top-level schema: ${JSON.stringify(schema)}`,
      );
    } else if (schema.builtin === "CoMap" || schema.builtin === "Account") {
      const def = schema.getDefinition();
      const defForFields = {
        shape: def.shape as Record<string, SchemaField>,
        catchall: def.catchall as SchemaField | undefined,
      };
      const ClassToExtend = schema.builtin === "Account" ? Account : CoMap;
      const getFieldEntries = createFieldEntriesResolver(defForFields);

      const coValueClass = class ZCoMap extends ClassToExtend {
        constructor(options: { fromRaw: RawCoMap } | undefined) {
          super(options);
          assignFieldEntries(this, getFieldEntries());
        }
      };

      const coValueSchema =
        ClassToExtend === Account
          ? new AccountSchema(schema as any, coValueClass as any)
          : new CoMapSchema(schema as any, coValueClass as any);

      return coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
    } else if (schema.builtin === "CoList") {
      const element = schema.element;
      const listFieldEntries: FieldEntry[] = [
        [coField.items, makeLazyCoFieldDef(() => element as SchemaField)],
      ];
      const coValueClass = class ZCoList extends CoList {
        constructor(options: { fromRaw: RawCoList } | undefined) {
          super(options);
          assignFieldEntries(this, listFieldEntries);
        }
      };

      const coValueSchema = new CoListSchema(element, coValueClass as any);

      return coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
    } else if (schema.builtin === "CoFeed") {
      const coValueClass = CoFeed.Of(
        schemaFieldToCoFieldDef(schema.element as SchemaField),
      );
      const coValueSchema = new CoFeedSchema(schema.element, coValueClass);
      return coValueSchema as unknown as CoValueSchemaFromCoreSchema<S>;
    } else if (schema.builtin === "FileStream") {
      const coValueClass = FileStream;
      return new FileStreamSchema(
        coValueClass,
      ) as CoValueSchemaFromCoreSchema<S>;
    } else if (schema.builtin === "CoVector") {
      const dimensions = schema.dimensions;

      const coValueClass = class CoVectorWithDimensions extends CoVector {
        protected static requiredDimensionsCount = dimensions;
      };

      return new CoVectorSchema(
        dimensions,
        coValueClass,
      ) as CoValueSchemaFromCoreSchema<S>;
    } else if (schema.builtin === "CoPlainText") {
      const coValueClass = CoPlainText;
      return new PlainTextSchema(
        coValueClass,
      ) as CoValueSchemaFromCoreSchema<S>;
    } else if (schema.builtin === "CoRichText") {
      const coValueClass = CoRichText;
      return new RichTextSchema(coValueClass) as CoValueSchemaFromCoreSchema<S>;
    } else if (schema.builtin === "CoDiscriminatedUnion") {
      const coValueClass = SchemaUnion.Of(schemaUnionDiscriminatorFor(schema));
      const coValueSchema = new CoDiscriminatedUnionSchema(
        schema,
        coValueClass,
      );
      return coValueSchema as CoValueSchemaFromCoreSchema<S>;
    } else if (schema.builtin === "Group") {
      return new GroupSchema() as CoValueSchemaFromCoreSchema<S>;
    } else {
      const notReachable: never = schema;
      throw new Error(
        `Unsupported zod CoValue type for top-level schema: ${JSON.stringify(notReachable, undefined, 2)}`,
      );
    }
  })();

  if (cacheKey) {
    hydratedSchemaCache.set(cacheKey, hydratedSchema);
  }

  return hydratedSchema;
}

/**
 * Convert a CoValue class or a CoValue schema into a CoValue class.
 *
 * This function bridges the gap between CoValue classes created with the class syntax,
 * and CoValue classes created with our `co.` definers.
 *
 * @param schema A CoValue class or a CoValue schema
 * @returns The same CoValue class, or a CoValue class that matches the provided schema
 */
export function coValueClassFromCoValueClassOrSchema<
  S extends CoValueClassOrSchema,
>(schema: S): CoValueClassFromAnySchema<S> {
  if (isCoValueClass(schema)) {
    return schema as any;
  } else if (isCoValueSchema(schema)) {
    return schema.getCoValueClass() as any;
  }

  throw new Error(`Unsupported schema: ${JSON.stringify(schema)}`);
}
