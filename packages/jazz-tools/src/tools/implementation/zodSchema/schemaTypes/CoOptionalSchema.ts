import { RawCoValue } from "cojson";
import { CoValueSchemaFromCoreSchema } from "../zodSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";
import { Group, Loaded } from "../../../internal.js";

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
  Inner extends CoreCoValueSchema = CoreCoValueSchema,
> implements CoreCoOptionalSchema<Inner>
{
  readonly collaborative = true as const;
  readonly builtin = "CoOptional" as const;
  readonly getDefinition = () => ({
    innerType: this.innerType,
  });
  readonly resolveQuery = true as const;

  constructor(public readonly innerType: Inner) {}

  fromRaw(raw: RawCoValue): Loaded<Inner> {
    if (
      "fromRaw" in this.innerType &&
      typeof this.innerType.fromRaw === "function"
    ) {
      return this.innerType.fromRaw(raw);
    } else {
      return (this.innerType as any).coValueClass.fromRaw(raw);
    }
  }

  create(init: any, owner: Group): Loaded<Inner> {
    if (
      "create" in this.innerType &&
      typeof this.innerType.create === "function"
    ) {
      return this.innerType.create(init, owner);
    } else {
      return (this.innerType as any).coValueClass.create(init, owner);
    }
  }
}
