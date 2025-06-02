import { RawAccount, RawCoList, RawCoMap } from "cojson";
import { zodSchemaToCoSchema } from "./runtimeConverters/zodSchemaToCoSchema.js";
import { z } from "./zodReExport.js";

export function schemaUnionDiscriminatorFor(
  schema: z.core.$ZodDiscriminatedUnion,
) {
  if (isUnionOfCoMapsDeeply(schema)) {
    if (!schema._zod.disc || schema._zod.disc.size == 0) {
      throw new Error(
        "z.union() of collaborative types is not supported, use z.discriminatedUnion() instead",
      );
    }

    const discriminator = schema._zod.def.discriminator;
    const field = schema._zod.disc.get(discriminator);

    if (!field) {
      throw new Error(
        "z.discriminatedUnion() of collaborative types with non-existent discriminator key is not supported",
      );
    }

    for (const value of field.values) {
      if (typeof value !== "string" && typeof value !== "number") {
        throw new Error(
          "z.discriminatedUnion() of collaborative types with non-string or non-number discriminator value is not supported",
        );
      }
    }

    const availableOptions: z.core.$ZodObject[] = [];

    for (const option of schema._zod.def.options) {
      if (option._zod.def.type === "object") {
        availableOptions.push(option as z.core.$ZodObject);
      } else if (option._zod.def.type === "union") {
        for (const subOption of (option as z.core.$ZodUnion)._zod.def.options) {
          if (subOption._zod.def.type === "object") {
            availableOptions.push(subOption as z.core.$ZodObject);
          }
        }
      } else {
        throw new Error(
          "Unsupported zod type in z.discriminatedUnion() of collaborative types",
        );
      }
    }

    const determineSchema = (_raw: RawCoMap | RawAccount | RawCoList) => {
      if (_raw instanceof RawCoList) {
        throw new Error(
          "z.discriminatedUnion() of collaborative types is not supported for CoLists",
        );
      }

      for (const option of availableOptions) {
        let match = true;

        for (const key of schema._zod.disc.keys()) {
          const discriminatorDef = (option as z.core.$ZodObject)._zod.def.shape[
            key as string
          ];

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

          const literalDef = discriminatorDef._zod.def as z.core.$ZodLiteralDef;

          if (!Array.from(literalDef.values).includes(discriminatorValue)) {
            match = false;
            break;
          }
        }

        if (match) {
          return zodSchemaToCoSchema(option);
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

export function isUnionOfCoMapsDeeply(
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

export function isUnionOfPrimitivesDeeply(schema: z.core.$ZodType) {
  if (schema instanceof z.core.$ZodUnion) {
    return schema._zod.def.options.every(isUnionOfPrimitivesDeeply);
  } else {
    return !("collaborative" in schema);
  }
}
