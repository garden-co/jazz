import { CoValueSchemaFromCoreSchema } from "../zodSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

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

  constructor(public readonly innerType: Shape) {}

  getCoValueClass(): ReturnType<
    CoValueSchemaFromCoreSchema<Shape>["getCoValueClass"]
  > {
    return (this.innerType as any).getCoValueClass();
  }
}
