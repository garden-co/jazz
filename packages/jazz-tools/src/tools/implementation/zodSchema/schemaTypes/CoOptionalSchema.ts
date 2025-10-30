import { isAnyCoValueSchema } from "../../../internal.js";
import { CoValueSchemaFromCoreSchema, ResolveQuery } from "../zodSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import { DefaultResolveQueryOfSchema } from "../typeConverters/DefaultResolveQueryOfSchema.js";

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
  EagerlyLoaded extends boolean = false,
> implements CoreCoOptionalSchema<Shape>
{
  readonly collaborative = true as const;
  readonly builtin = "CoOptional" as const;
  readonly getDefinition = () => ({
    innerType: this.innerType,
  });

  private isEagerlyLoaded: EagerlyLoaded = false as EagerlyLoaded;
  /**
   * The default resolve query to be used when loading instances of this schema.
   * Defaults to `false`, meaning that no resolve query will be used by default.
   * @internal
   */
  get defaultResolveQuery(): DefaultResolveQuery<this> {
    if (!this.isEagerlyLoaded) {
      return false as DefaultResolveQuery<this>;
    }
    if (
      isAnyCoValueSchema(this.innerType) &&
      this.innerType.defaultResolveQuery
    ) {
      return this.innerType.defaultResolveQuery as DefaultResolveQuery<this>;
    }
    return true as DefaultResolveQuery<this>;
  }

  constructor(public readonly innerType: Shape) {}

  getCoValueClass(): ReturnType<
    CoValueSchemaFromCoreSchema<Shape>["getCoValueClass"]
  > {
    return (this.innerType as any).getCoValueClass();
  }

  resolved(): CoOptionalSchema<Shape, true> {
    if (this.isEagerlyLoaded) {
      return this as CoOptionalSchema<Shape, true>;
    }
    const copy = new CoOptionalSchema<Shape, true>(this.innerType);
    copy.isEagerlyLoaded = true;
    return copy;
  }
}

type DefaultResolveQuery<S> = S extends CoOptionalSchema<
  infer InnerShape,
  infer EagerlyLoaded
>
  ? EagerlyLoaded extends false
    ? false
    : DefaultResolveQueryOfSchema<InnerShape> extends false
      ? true
      : DefaultResolveQueryOfSchema<InnerShape>
  : never;
