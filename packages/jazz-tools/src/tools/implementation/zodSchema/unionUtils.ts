import { RawAccount, RawCoList, RawCoMap } from "cojson";
import {
  AnyZodOrCoValueSchema,
  CoDiscriminatedUnionSchema,
  CoMap,
  CoreCoDiscriminatedUnionSchema,
  CoreCoMapSchema,
  DiscriminableCoValueSchemas,
  DiscriminableCoreCoValueSchema,
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

    const availableOptions: DiscriminableCoreCoValueSchema[] = [];

    for (const option of options) {
      if (option.builtin === "CoMap") {
        availableOptions.push(option);
      } else if (option.builtin === "CoDiscriminatedUnion") {
        for (const subOption of (
          option as CoDiscriminatedUnionSchema<any>
        ).getDefinition().options) {
          if (!options.includes(subOption)) {
            options.push(subOption);
          }
        }
      } else {
        throw new Error(
          "Unsupported zod type in co.discriminatedUnion() of collaborative types",
        );
      }
    }

    const determineSchema = (_raw: RawCoMap | RawAccount | RawCoList) => {
      if (_raw instanceof RawCoList) {
        throw new Error(
          "co.discriminatedUnion() of collaborative types is not supported for CoLists",
        );
      }

      for (const option of availableOptions) {
        let match = true;

        for (const key of Object.keys(discriminatorMap)) {
          const discriminatorDef = (option as CoreCoMapSchema).getDefinition()
            .shape[key as string];

          const discriminatorValue = (_raw as RawCoMap).get(key as string);

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

          if (discriminatorDef._zod.def.type !== "literal") {
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
          const coValueSchema = hydrateCoreCoValueSchema(option as any);
          return coValueSchema.getCoValueClass() as typeof CoMap;
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
