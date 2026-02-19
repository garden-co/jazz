import type { RawBinaryCoStream } from "cojson";
import { cojsonInternals } from "cojson";
import {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  getCoValueOwner,
  Group,
  ID,
  Resolved,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
  Settled,
  Account,
  CoValueJazzApi,
  inspect,
  loadCoValueWithoutMe,
  parseCoValueCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
  CoreCoVectorSchema,
} from "../internal.js";

/**
 * CoVectors are collaborative storages of vectors (floating point arrays).
 *
 * @category CoValues
 */
export class CoVector
  extends Float32Array
  implements Readonly<Float32Array>, CoValue
{
  declare $jazz: CoVectorJazzApi<this>;
  declare $isLoaded: true;

  /** @category Type Helpers */
  declare [TypeSym]: "BinaryCoStream";

  static get [Symbol.species]() {
    return Float32Array;
  }

  protected static requiredDimensionsCount: number | undefined = undefined;
  private declare _isVectorLoaded: boolean;
  private declare _requiredDimensionsCount: number;

  static coValueSchema?: CoreCoVectorSchema;

  constructor(
    options:
      | {
          owner: Account | Group;
        }
      | {
          fromRaw: RawBinaryCoStream;
        },
  ) {
    const dimensionsCount = (new.target as typeof CoVector)
      .requiredDimensionsCount;

    if (dimensionsCount === undefined) {
      throw new Error(
        "Instantiating CoVector without a dimensions count is not allowed. Use co.vector(...).create() instead.",
      );
    }

    // Initialize empty Float32Array buffer with the expected vector length
    // to be filled with the vector data later
    super(dimensionsCount);

    const isFromRaw = "fromRaw" in options;

    const raw: RawBinaryCoStream = isFromRaw
      ? options.fromRaw
      : options.owner.$jazz.raw.createBinaryStream();

    Object.defineProperties(this, {
      [TypeSym]: {
        value: "BinaryCoStream",
        enumerable: false,
        configurable: true,
      },
      $jazz: {
        value: new CoVectorJazzApi(this, raw),
        enumerable: false,
        configurable: true,
      },
      $isLoaded: { value: true, enumerable: false, configurable: true },
      _isVectorLoaded: {
        value: false,
        enumerable: false,
        writable: true,
        configurable: true,
      },
      _requiredDimensionsCount: {
        value: dimensionsCount,
        enumerable: false,
        writable: false,
        configurable: true,
      },
    });

    if (isFromRaw) {
      this.loadVectorData();
    }
  }

  /** @category Internals */
  static fromRaw<V extends CoVector>(
    this: CoValueClass<V> & typeof CoVector,
    raw: RawBinaryCoStream,
  ) {
    return new this({ fromRaw: raw });
  }

  /**
   * Create a new `CoVector` instance with the given vector.
   *
   * @category Creation
   * @deprecated Use `co.vector(...).create` instead.
   */
  static create<S extends CoVector>(
    this: CoValueClass<S> & typeof CoVector,
    vector: number[] | Float32Array,
    options?: { owner?: Account | Group } | Account | Group,
  ) {
    const vectorAsFloat32Array =
      vector instanceof Float32Array ? vector : new Float32Array(vector);

    const givenVectorDimensions =
      vectorAsFloat32Array.byteLength / vectorAsFloat32Array.BYTES_PER_ELEMENT;

    if (
      this.requiredDimensionsCount !== undefined &&
      givenVectorDimensions !== this.requiredDimensionsCount
    ) {
      throw new Error(
        `Vector dimension mismatch! Expected ${this.requiredDimensionsCount} dimensions, got ${
          givenVectorDimensions
        }`,
      );
    }

    const { owner } = parseCoValueCreateOptions(options);
    const coVector = new this({ owner });
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

  private static toByteArray(vector: Float32Array): Uint8Array {
    // zero copy view of the vector bytes
    return new Uint8Array(vector.buffer, vector.byteOffset, vector.byteLength);
  }

  private static fromByteArray(bytesChunks: Uint8Array[]): Float32Array {
    const total = bytesChunks.reduce((acc, c) => acc + c.byteLength, 0);

    if (total % 4 !== 0)
      throw new Error("[INTERNAL] Total byte length must be multiple of 4");

    const u8 = new Uint8Array(total);
    let off = 0;

    for (const c of bytesChunks) {
      u8.set(c, off);
      off += c.byteLength;
    }

    return new Float32Array(u8.buffer, u8.byteOffset, total / 4);
  }

  private loadVectorData(): void {
    if (this._isVectorLoaded === true) {
      return;
    }

    const chunks = this.$jazz.raw.getBinaryChunks();

    if (!chunks) {
      // This should never happen
      throw new Error(`CoVector '${this.$jazz.raw.id}' is not loaded`);
    }

    const vector = CoVector.fromByteArray(chunks.chunks);

    if (vector.length !== this._requiredDimensionsCount) {
      throw new Error(
        `Vector dimension mismatch! CoVector '${this.$jazz.raw.id}' loaded with ${vector.length} dimensions, but the schema requires ${this._requiredDimensionsCount} dimensions`,
      );
    }

    this.setVectorData(vector);

    return;
  }

  private setVectorData(vector: Float32Array): void {
    super.set(vector, 0);
    this._isVectorLoaded = true;
  }

  /**
   * Get a JSON representation of the `CoVector`
   * @category Content
   */
  toJSON(): Array<number> {
    return Array.from(this);
  }

  /** @internal */
  [inspect]() {
    return this.toJSON();
  }

  /**
   * Load a `CoVector`
   *
   * @category Subscription & Loading
   * @deprecated Use `co.vector(...).load` instead.
   */
  static async load<C extends CoVector>(
    this: CoValueClass<C>,
    id: ID<C>,
    options?: {
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Settled<C>> {
    const coVector = await loadCoValueWithoutMe(this, id, options);

    /**
     * We are only interested in the entire vector. Since most vectors are small (<15kB),
     * we can wait for the stream to be complete before returning the vector
     */
    if (!coVector.$isLoaded || !coVector.$jazz.raw.isBinaryStreamEnded()) {
      return new Promise((resolve) => {
        subscribeToCoValueWithoutMe(
          this,
          id,
          options || {},
          (value, unsubscribe) => {
            if (value.$jazz.raw.isBinaryStreamEnded()) {
              unsubscribe();
              resolve(value);
            }
          },
        );
      });
    }

    coVector.loadVectorData();
    return coVector;
  }

  /**
   * Subscribe to a `CoVector`, when you have an ID but don't have a `CoVector` instance yet
   * @category Subscription & Loading
   * @deprecated Use `co.vector(...).subscribe` instead.
   */
  static subscribe<V extends CoVector>(
    this: CoValueClass<V>,
    id: ID<V>,
    listener: (value: Resolved<V, true>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<V extends CoVector>(
    this: CoValueClass<V>,
    id: ID<V>,
    options: SubscribeListenerOptions<V, true>,
    listener: (value: Resolved<V, true>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<V extends CoVector>(
    this: CoValueClass<V>,
    id: ID<V>,
    ...args: SubscribeRestArgs<V, true>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe<V, true>(this, id, options, listener);
  }

  // CoVector mutation method overrides, as CoVectors aren't meant to be mutated
  /**
   * Calling `copyWithin` on a CoVector is forbidden. CoVectors are immutable.
   * @deprecated If you want to change the vector, replace the former instance of CoVector with a new one.
   */
  override copyWithin(target: number, start: number, end?: number): never {
    throw new Error("Cannot mutate a CoVector using `copyWithin`");
  }
  /**
   * Calling `fill` on a CoVector is forbidden. CoVectors are immutable.
   * @deprecated If you want to change the vector, replace the former instance of CoVector with a new one.
   */
  override fill(value: number, start?: number, end?: number): never {
    throw new Error("Cannot mutate a CoVector using `fill`");
  }
  /**
   * Calling `reverse` on a CoVector is forbidden. CoVectors are immutable.
   * @deprecated If you want to change the vector, replace the former instance of CoVector with a new one.
   */
  override reverse(): never {
    throw new Error("Cannot mutate a CoVector using `reverse`");
  }
  /**
   * Calling `set` on a CoVector is forbidden. CoVectors are immutable.
   * @deprecated If you want to change the vector, replace the former instance of CoVector with a new one.
  //  */
  override set(array: ArrayLike<number>, offset?: number): never {
    throw new Error("Cannot mutate a CoVector using `set`");
  }
  /**
   * Calling `sort` on a CoVector is forbidden. CoVectors are immutable.
   * @deprecated If you want to change the vector, replace the former instance of CoVector with a new one.
   */
  override sort(compareFn?: (a: number, b: number) => number): never {
    throw new Error("Cannot mutate a CoVector using `sort`");
  }
}

export class CoVectorJazzApi<V extends CoVector> extends CoValueJazzApi<V> {
  constructor(
    private coVector: V,
    public raw: RawBinaryCoStream,
  ) {
    super(coVector);
  }

  get owner(): Group {
    return getCoValueOwner(this.coVector);
  }

  /**
   * An instance method to subscribe to an existing `CoVector`
   * @category Subscription & Loading
   */
  subscribe<B extends CoVector>(
    this: CoVectorJazzApi<B>,
    listener: (value: Resolved<B, true>) => void,
  ): () => void {
    return subscribeToExistingCoValue(this.coVector, {}, listener);
  }

  /**
   * Wait for the `CoVector` to be uploaded to the other peers.
   *
   * @category Subscription & Loading
   */
  waitForSync(options?: { timeout?: number }) {
    return this.raw.core.waitForSync(options);
  }

  // Vector operations
  /**
   * Calculate the magnitude of this vector.
   */
  magnitude(): number {
    return VectorCalculation.magnitude(this.coVector);
  }

  /**
   * Normalize this vector.
   * @returns A new instance of a normalized vector.
   */
  normalize(): Float32Array {
    return VectorCalculation.normalize(this.coVector);
  }

  /**
   * Calculate the dot product of this vector and another vector.
   */
  dotProduct(otherVector: CoVector | Float32Array | number[]): number {
    return VectorCalculation.dotProduct(this.coVector, otherVector);
  }

  /**
   * Calculate the cosine similarity between this vector and another vector.
   *
   * @returns A value between `-1` and `1`:
   * - `1` means the vectors are identical
   * - `0` means the vectors are orthogonal (i.e. no similarity)
   * - `-1` means the vectors are opposite direction (perfectly dissimilar)
   */
  cosineSimilarity(otherVector: CoVector | Float32Array | number[]): number {
    return VectorCalculation.cosineSimilarity(this.coVector, otherVector);
  }
}

const VectorCalculation = {
  magnitude: (vector: Float32Array | number[]) => {
    let sum = 0;
    for (const v of vector) {
      sum += v * v;
    }
    return Math.sqrt(sum);
  },
  normalize: (vector: Float32Array) => {
    const mag = VectorCalculation.magnitude(vector);

    if (mag === 0) {
      return new Float32Array(vector.length).fill(0);
    }

    return vector.map((v) => v / mag);
  },
  dotProduct: (vectorA: Float32Array, vectorB: Float32Array | number[]) => {
    if (vectorA.length !== vectorB.length) {
      throw new Error(
        `Vector dimensions don't match: ${vectorA.length} vs ${vectorB.length}`,
      );
    }

    return vectorA.reduce((sum, a, i) => sum + a * vectorB[i]!, 0);
  },
  cosineSimilarity: (
    vectorA: Float32Array,
    vectorB: Float32Array | number[],
  ) => {
    const magnitudeA = VectorCalculation.magnitude(vectorA);
    const magnitudeB = VectorCalculation.magnitude(vectorB);

    if (magnitudeA === 0 || magnitudeB === 0) {
      return 0;
    }

    const dotProductAB = VectorCalculation.dotProduct(vectorA, vectorB);
    return dotProductAB / (magnitudeA * magnitudeB);
  },
};
