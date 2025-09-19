import {
  Account,
  AnonymousJazzAgent,
  CoVector,
  Group,
  coOptionalDefiner,
} from "../../../internal.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

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
  };
}

export class CoVectorSchema implements CoreCoVectorSchema {
  readonly collaborative = true as const;
  readonly builtin = "CoVector" as const;

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
  ): CoVector;
  /**
   * Create a `CoVector` from a given vector.
   *
   * @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead.
   */
  create(
    vector: number[] | Float32Array,
    options?: { owner: Account | Group } | Account | Group,
  ): CoVector;
  create(
    vector: number[] | Float32Array,
    options?: { owner: Account | Group } | Account | Group,
  ): CoVector {
    return this.coValueClass.create(vector, options);
  }

  /**
   * Load a `CoVector` with a given ID.
   */
  load(
    id: string,
    options?: { loadAs: Account | AnonymousJazzAgent },
  ): Promise<CoVector | null> {
    return this.coValueClass.load(id, options);
  }

  /**
   * Subscribe to a `CoVector`, when you have an ID but don't have a `CoVector` instance yet
   */
  subscribe(
    id: string,
    options: { loadAs: Account | AnonymousJazzAgent },
    listener: (value: CoVector, unsubscribe: () => void) => void,
  ): () => void;
  subscribe(
    id: string,
    listener: (value: CoVector, unsubscribe: () => void) => void,
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

  // Vector operations
  /**
   * Calculate the magnitude of a vector.
   */
  magnitude(vector: Float32Array | CoVector): number {
    return this.coValueClass.magnitude(vector);
  }

  /**
   * Normalize a vector.
   * @returns A new instance of a normalized vector.
   */
  normalize(vector: Float32Array | CoVector): Float32Array {
    return this.coValueClass.normalize(vector);
  }

  /**
   * Calculate the dot product of two vectors.
   */
  dotProduct(
    vectorA: Float32Array | CoVector,
    vectorB: Float32Array | CoVector,
  ): number {
    return this.coValueClass.dotProduct(vectorA, vectorB);
  }

  /**
   * Calculate the cosine similarity between two vectors.
   *
   * @returns A value between `-1` and `1`:
   * - `1` means the vectors are identical
   * - `0` means the vectors are orthogonal (i.e. no similarity)
   * - `-1` means the vectors are opposite direction (perfectly dissimilar)
   */
  cosineSimilarity(
    vectorA: CoVector | Float32Array,
    vectorB: CoVector | Float32Array,
  ): number {
    return this.coValueClass.cosineSimilarity(vectorA, vectorB);
  }
}
