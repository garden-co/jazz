import {
  Account,
  AnonymousJazzAgent,
  CoVector,
  Group,
  InstanceOrPrimitiveOfSchema,
  InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded,
  coOptionalDefiner,
  withSchemaPermissions,
} from "../../../internal.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";
import { z } from "../zodReExport.js";
import { coValueValidationSchema } from "./schemaValidators.js";

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
    resolveQuery: true as const,
    getValidationSchema: () => z.any(),
  };
}

export class CoVectorSchema implements CoreCoVectorSchema {
  readonly collaborative = true as const;
  readonly builtin = "CoVector" as const;
  readonly resolveQuery = true as const;

  #validationSchema: z.ZodType | undefined = undefined;
  #permissions: SchemaPermissions | null = null;
  getValidationSchema = () => {
    if (this.#validationSchema) {
      return this.#validationSchema;
    }

    const validationSchema = z.instanceof(Float32Array).or(z.array(z.number()));

    this.#validationSchema = coValueValidationSchema(
      validationSchema,
      CoVector,
    );

    return this.#validationSchema;
  };

  /**
   * Permissions to be used when creating or composing CoValues
   * @internal
   */
  get permissions(): SchemaPermissions {
    return this.#permissions ?? DEFAULT_SCHEMA_PERMISSIONS;
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
    const optionsWithPermissions = withSchemaPermissions(
      options,
      this.permissions,
    );
    return this.coValueClass.create(vector, optionsWithPermissions);
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

  /**
   * Configure permissions to be used when creating or composing CoValues
   */
  withPermissions(
    permissions: Omit<SchemaPermissions, "writer">,
  ): CoVectorSchema {
    const copy = new CoVectorSchema(this.dimensions, this.coValueClass);
    copy.#permissions = permissions;
    return copy;
  }
}

export type CoVectorInstance = InstanceOrPrimitiveOfSchema<CoVectorSchema>;

export type MaybeLoadedCoVectorInstance =
  InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<CoVectorSchema>;
