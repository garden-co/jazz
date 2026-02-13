import { JsonValue, RawCoMap } from "cojson";
import {
  AnyZodOrCoValueSchema,
  CoDiscriminatedUnionSchema,
  CoMap,
  CoreCoDiscriminatedUnionSchema,
  CoreCoMapSchema,
  DiscriminableCoValueSchemas,
  DiscriminableCoreCoValueSchema,
  SchemaUnionDiscriminator,
  createCoreCoMapSchema,
} from "../../internal.js";
import {
  hydrateCoreCoValueSchema,
  isAnyCoValueSchema,
} from "./runtimeConverters/coValueSchemaTransformation.js";
import { z } from "./zodReExport.js";

export function schemaUnionDiscriminatorFor(
  schema: CoreCoDiscriminatedUnionSchema<DiscriminableCoValueSchemas>,
) {
  if (isUnionOfCoMapsDeeply(schema)) {
    const definition = schema.getDefinition();
    const { discriminatorMap, discriminator, options } = definition;

    const field = discriminatorMap[discriminator];
    if (!field) {
      throw new Error(
        "co.discriminatedUnion() of collaborative types with non-existent discriminator key is not supported",
      );
    }

    for (const value of field) {
      if (typeof value !== "string" && typeof value !== "number") {
        throw new Error(
          "co.discriminatedUnion() of collaborative types with non-string or non-number discriminator value is not supported",
        );
      }
    }

    const availableOptions = getFlattenedUnionOptions(schema);

    const determineSchema: SchemaUnionDiscriminator<CoMap> = (
      discriminable,
    ) => {
      // collect all keys of nested CoValues
      const allNestedRefKeys = new Set<string>();
      for (const option of availableOptions) {
        const coMapShape = (option as CoreCoMapSchema).getDefinition().shape;
        for (const [key, value] of Object.entries(coMapShape)) {
          if (isAnyCoValueSchema(value)) {
            allNestedRefKeys.add(key);
          }
        }
      }

      for (const option of availableOptions) {
        let match = true;
        const optionDef = (option as CoreCoMapSchema).getDefinition();

        for (const key of Object.keys(discriminatorMap)) {
          const discriminatorDef = optionDef.shape[key];
          const discriminatorValue = resolveDiscriminantValue(
            discriminable,
            key,
          );

          if (discriminatorValue && typeof discriminatorValue === "object") {
            throw new Error("Discriminator must be a primitive value");
          }

          if (!discriminatorDef) {
            if (key === discriminator) {
              match = false;
              break;
            } else {
              continue;
            }
          }
          if (discriminatorDef._zod?.def.type !== "literal") {
            break;
          }

          const literalDef = discriminatorDef._zod
            .def as z.core.$ZodLiteralDef<any>;

          if (!Array.from(literalDef.values).includes(discriminatorValue)) {
            match = false;
            break;
          }
        }

        if (match) {
          const dummyFieldNames = Array.from(allNestedRefKeys).filter(
            (key) => !optionDef.shape[key],
          );

          if (dummyFieldNames.length === 0) {
            const coValueSchema = hydrateCoreCoValueSchema(option as any);
            return coValueSchema.getCoValueClass() as typeof CoMap;
          }

          // Add schema-level dummy keys so deep-resolve keys shared by other union branches
          // are recognized without mutating instances at runtime.
          const augmentedShape = {
            ...optionDef.shape,
          } as Record<string, AnyZodOrCoValueSchema>;

          for (const key of dummyFieldNames) {
            augmentedShape[key] = z.optional(z.null());
          }

          const augmentedSchema = hydrateCoreCoValueSchema(
            createCoreCoMapSchema(augmentedShape, optionDef.catchall),
          );

          return augmentedSchema.getCoValueClass() as typeof CoMap;
        }
      }

      throw new Error(
        "co.discriminatedUnion() of collaborative types with no matching discriminator value found",
      );
    };

    return determineSchema;
  } else {
    throw new Error(
      "co.discriminatedUnion() of non-collaborative types is not supported",
    );
  }
}

function isUnionOfCoMapsDeeply(
  schema: CoreCoDiscriminatedUnionSchema<DiscriminableCoValueSchemas>,
): boolean {
  return schema.getDefinition().options.every(isCoMapOrUnionOfCoMapsDeeply);
}

function isCoMapOrUnionOfCoMapsDeeply(
  schema: DiscriminableCoreCoValueSchema,
): boolean {
  if (schema.builtin === "CoMap") {
    return true;
  } else if (schema.builtin === "CoDiscriminatedUnion") {
    return (schema as CoDiscriminatedUnionSchema<any>)
      .getDefinition()
      .options.every(isCoMapOrUnionOfCoMapsDeeply);
  } else {
    return false;
  }
}

export function isUnionOfPrimitivesDeeply(schema: AnyZodOrCoValueSchema) {
  if (schema instanceof z.core.$ZodUnion) {
    return schema._zod.def.options.every(isUnionOfPrimitivesDeeply);
  } else {
    return !isAnyCoValueSchema(schema);
  }
}

function isCoDiscriminatedUnion(
  def: any,
): def is CoreCoDiscriminatedUnionSchema<any> {
  return def.builtin === "CoDiscriminatedUnion";
}

/**
 * Flattens all options from a discriminated union schema, including nested unions.
 * Returns all options in a flat array.
 */
export function getFlattenedUnionOptions(
  schema: CoreCoDiscriminatedUnionSchema<DiscriminableCoValueSchemas>,
): DiscriminableCoreCoValueSchema[] {
  const definition = schema.getDefinition();
  const options = definition.options;
  const availableOptions: DiscriminableCoreCoValueSchema[] = [];

  for (const option of options) {
    if (option.builtin === "CoMap") {
      availableOptions.push(option);
    } else if (option.builtin === "CoDiscriminatedUnion") {
      const nestedOptions = getFlattenedUnionOptions(
        option as CoreCoDiscriminatedUnionSchema<DiscriminableCoValueSchemas>,
      );
      for (const subOption of nestedOptions) {
        if (!availableOptions.includes(subOption)) {
          availableOptions.push(subOption);
        }
      }
    } else {
      throw new Error(
        "Unsupported zod type in co.discriminatedUnion() of collaborative types",
      );
    }
  }

  return availableOptions;
}

/**
 * Gets the discriminator values for a given option and discriminator key
 */
export function getDiscriminatorValuesForOption(
  option: DiscriminableCoreCoValueSchema,
  discriminatorKey: string,
): Set<unknown> | undefined {
  const optionDefinition = option.getDefinition();
  return optionDefinition.discriminatorMap?.[discriminatorKey];
}

export function resolveDiscriminantValue(
  init: unknown,
  discriminatorKey: string,
): JsonValue | undefined {
  if (init == null) {
    return undefined;
  }

  if (init instanceof Map || init instanceof RawCoMap) {
    return init.get(discriminatorKey);
  }

  if (typeof init === "object") {
    const record = init as Record<string, JsonValue>;
    if (discriminatorKey in record) {
      return record[discriminatorKey];
    }
  }

  return undefined;
}
