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
