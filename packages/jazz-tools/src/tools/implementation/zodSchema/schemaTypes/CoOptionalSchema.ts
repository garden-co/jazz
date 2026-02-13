import { CoValueSchemaFromCoreSchema } from "../zodSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";
import { z } from "../zodReExport.js";
import { CoList, CoMap } from "../../../internal.js";

type CoOptionalSchemaDefinition<
  Shape extends CoreCoValueSchema = CoreCoValueSchema,
> = {
  innerType: Shape;
};

export interface CoreCoOptionalSchema<
  Shape extends CoreCoValueSchema = CoreCoValueSchema,
> extends CoreCoValueSchema {
  builtin: "CoOptional";
  innerType: Shape;
  getDefinition: () => CoOptionalSchemaDefinition<Shape>;
}

export class CoOptionalSchema<
  Shape extends CoreCoValueSchema = CoreCoValueSchema,
> implements CoreCoOptionalSchema<Shape>
{
  readonly collaborative = true as const;
  readonly builtin = "CoOptional" as const;
  readonly getDefinition = () => ({
    innerType: this.innerType,
  });
  readonly resolveQuery = true as const;

  #validationSchema: z.ZodType | undefined = undefined;

  constructor(public readonly innerType: Shape) {}

  getValidationSchema = () => {
    if (this.#validationSchema) {
      return this.#validationSchema;
    }

    this.#validationSchema = z.optional(this.innerType.getValidationSchema());
    return this.#validationSchema;
  };

  getCoValueClass(): ReturnType<
    CoValueSchemaFromCoreSchema<Shape>["getCoValueClass"]
  > {
    return (this.innerType as any).getCoValueClass();
  }
}
