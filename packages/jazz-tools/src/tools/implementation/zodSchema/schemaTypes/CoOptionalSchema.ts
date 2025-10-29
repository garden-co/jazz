import { CoValueSchemaFromCoreSchema, ResolveQuery } from "../zodSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";

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
  DefaultResolveQuery extends CoreResolveQuery = Shape["defaultResolveQuery"],
> implements CoreCoOptionalSchema<Shape>
{
  readonly collaborative = true as const;
  readonly builtin = "CoOptional" as const;
  readonly getDefinition = () => ({
    innerType: this.innerType,
  });

  /**
   * The default resolve query to be used when loading instances of this schema.
   * Defaults to `false`, meaning that no resolve query will be used by default.
   * @internal
   */
  public defaultResolveQuery: DefaultResolveQuery;

  constructor(public readonly innerType: Shape) {
    this.defaultResolveQuery = this.innerType
      .defaultResolveQuery as DefaultResolveQuery;
  }

  getCoValueClass(): ReturnType<
    CoValueSchemaFromCoreSchema<Shape>["getCoValueClass"]
  > {
    return (this.innerType as any).getCoValueClass();
  }

  resolved(): CoOptionalSchema<
    Shape,
    DefaultResolveQuery extends false ? true : CoreResolveQuery
  > {
    if (this.defaultResolveQuery) {
      return this as CoOptionalSchema<Shape, true>;
    }
    const copy = new CoOptionalSchema<Shape, true>(this.innerType);
    copy.defaultResolveQuery = true;
    return copy;
  }
}
