import {
  Account,
  AnonymousJazzAgent,
  CoVector,
  Group,
  InstanceOrPrimitiveOfSchema,
  InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded,
  RefsToResolve,
  Settled,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  coOptionalDefiner,
  loadCoValueWithoutMe,
  parseCoValueCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  withSchemaPermissions,
} from "../../../internal.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";
import {
  DEFAULT_SCHEMA_PERMISSIONS,
  SchemaPermissions,
} from "../schemaPermissions.js";
import { RawBinaryCoStream } from "cojson";
import { cojsonInternals } from "cojson";

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
  };
}

export class CoVectorSchema implements CoreCoVectorSchema {
  readonly collaborative = true as const;
  readonly builtin = "CoVector" as const;
  readonly resolveQuery = true as const;

  /**
   * Permissions to be used when creating or composing CoValues
   */
  permissions: SchemaPermissions = DEFAULT_SCHEMA_PERMISSIONS;

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
    const { owner } = parseCoValueCreateOptions(optionsWithPermissions);

    const vectorAsFloat32Array =
      vector instanceof Float32Array ? vector : new Float32Array(vector);

    const givenVectorDimensions =
      vectorAsFloat32Array.byteLength / vectorAsFloat32Array.BYTES_PER_ELEMENT;

    if (givenVectorDimensions !== this.dimensions) {
      throw new Error(
        `Vector dimension mismatch! Expected ${this.dimensions} dimensions, got ${givenVectorDimensions}`,
      );
    }

    const coVector = new this.coValueClass({ owner }, this);
    coVector.setVectorData(vectorAsFloat32Array);

    const byteArray = CoVector.toByteArray(vectorAsFloat32Array);

    coVector.$jazz.raw.startBinaryStream({
      mimeType: "application/vector+octet-stream",
      totalSizeBytes: byteArray.byteLength,
    });

    const chunkSize =
      cojsonInternals.TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE;

    // Although most embedding vectors are small
    // (3072-dimensional vector is only 12,288 bytes),
    // we should still chunk the data to avoid transaction size limits
    for (let idx = 0; idx < byteArray.length; idx += chunkSize) {
      coVector.$jazz.raw.pushBinaryStreamChunk(
        byteArray.slice(idx, idx + chunkSize),
      );
    }
    coVector.$jazz.raw.endBinaryStream();

    return coVector;
  }

  fromRaw(raw: RawBinaryCoStream): CoVectorInstance {
    return new this.coValueClass({ fromRaw: raw }, this);
  }

  /**
   * Load a `CoVector` with a given ID.
   */
  async load(
    id: string,
    options?: { loadAs: Account | AnonymousJazzAgent },
  ): Promise<MaybeLoadedCoVectorInstance> {
    const coVector = (await loadCoValueWithoutMe(
      this,
      id,
      options,
    )) as CoVector;

    /**
     * We are only interested in the entire vector. Since most vectors are small (<15kB),
     * we can wait for the stream to be complete before returning the vector
     */
    if (
      !coVector.$isLoaded ||
      !(coVector.$jazz.raw as RawBinaryCoStream).isBinaryStreamEnded()
    ) {
      return new Promise((resolve) => {
        subscribeToCoValueWithoutMe(
          this,
          id,
          options || {},
          (value, unsubscribe) => {
            const vectorValue = value as CoVector;
            if (
              (vectorValue.$jazz.raw as RawBinaryCoStream).isBinaryStreamEnded()
            ) {
              unsubscribe();
              resolve(vectorValue as MaybeLoadedCoVectorInstance);
            }
          },
        );
      }) as Promise<MaybeLoadedCoVectorInstance>;
    }

    coVector.loadVectorData();
    return coVector as MaybeLoadedCoVectorInstance;
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
  subscribe(...args: [any, ...[any]]) {
    const [id, ...restArgs] = args;
    const { options, listener } = parseSubscribeRestArgs<
      CoVector,
      RefsToResolve<CoVector>
    >(restArgs);
    return subscribeToCoValueWithoutMe(this, id, options, listener as any);
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
  withPermissions(permissions: SchemaPermissions): CoVectorSchema {
    const copy = new CoVectorSchema(this.dimensions, this.coValueClass);
    copy.permissions = permissions;
    return copy;
  }
}

export type CoVectorInstance = InstanceOrPrimitiveOfSchema<CoVectorSchema>;

export type MaybeLoadedCoVectorInstance =
  InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<CoVectorSchema>;
