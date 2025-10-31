import {
  Account,
  AnonymousJazzAgent,
  CoVector,
  Group,
  InstanceOrPrimitiveOfSchema,
  InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded,
  coOptionalDefiner,
  ResolveQuery,
} from "../../../internal.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";

export interface CoreCoVectorSchema extends CoreCoValueSchema {
  builtin: "CoVector";
  dimensions: number;
}

export function createCoreCoVectorSchema(
  dimensions: number,
): CoreCoVectorSchema {
  return {
    collaborative: true as const,
    builtin: "CoVector" as const,
    dimensions,
    defaultResolveQuery: false,
  };
}

export class CoVectorSchema<EagerlyLoaded extends boolean = false>
  implements CoreCoVectorSchema
{
  readonly collaborative = true as const;
  readonly builtin = "CoVector" as const;

  private isEagerlyLoaded: EagerlyLoaded = false as EagerlyLoaded;
  /**
   * The default resolve query to be used when loading instances of this schema.
   * Defaults to `false`, meaning that no resolve query will be used by default.
   * @internal
   */
  get defaultResolveQuery(): boolean {
    return this.isEagerlyLoaded;
  }

  constructor(
    public dimensions: number,
    private coValueClass: typeof CoVector,
  ) {}

  /**
   * Create a `CoVector` from a given vector.
   */
  create(
    vector: number[] | Float32Array,
    options?: { owner: Group } | Group,
  ): CoVectorInstance;
  /**
   * Create a `CoVector` from a given vector.
   *
   * @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead.
   */
  create(
    vector: number[] | Float32Array,
    options?: { owner: Account | Group } | Account | Group,
  ): CoVectorInstance;
  create(
    vector: number[] | Float32Array,
    options?: { owner: Account | Group } | Account | Group,
  ): CoVectorInstance {
    return this.coValueClass.create(vector, options);
  }

  /**
   * Load a `CoVector` with a given ID.
   */
  load(
    id: string,
    options?: { loadAs: Account | AnonymousJazzAgent },
  ): Promise<MaybeLoadedCoVectorInstance> {
    return this.coValueClass.load(id, options);
  }

  /**
   * Subscribe to a `CoVector`, when you have an ID but don't have a `CoVector` instance yet
   */
  subscribe(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
    listener: (
      value: MaybeLoadedCoVectorInstance,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe(
    id: string,
    listener: (
      value: MaybeLoadedCoVectorInstance,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe(...args: [any, ...any[]]) {
    // @ts-expect-error
    return this.coValueClass.subscribe(...args);
  }

  getCoValueClass(): typeof CoVector {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  resolved(): CoVectorSchema<true> {
    if (this.isEagerlyLoaded) {
      return this as CoVectorSchema<true>;
    }
    const copy = new CoVectorSchema<true>(this.dimensions, this.coValueClass);
    copy.isEagerlyLoaded = true;
    return copy;
  }
}

export type CoVectorInstance = InstanceOrPrimitiveOfSchema<CoVectorSchema>;

export type MaybeLoadedCoVectorInstance =
  InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<CoVectorSchema>;
