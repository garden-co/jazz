import type { RawBinaryCoStream } from "cojson";
import { cojsonInternals } from "cojson";
import {
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  getCoValueOwner,
  Group,
  ID,
  RefsToResolve,
  Resolved,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  TypeSym,
} from "../internal.js";
import {
  Account,
  CoValueJazzApi,
  inspect,
  loadCoValueWithoutMe,
  parseCoValueCreateOptions,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  subscribeToExistingCoValue,
} from "../internal.js";

/**
 * CoVectors are collaborative storages of vectors (floating point arrays).
 *
 * @category CoValues
 */
export class CoVector extends Float32Array implements CoValue {
  declare $jazz: CoVectorJazzApi<this>;

  /** @category Type Helpers */
  declare [TypeSym]: "BinaryCoStream";

  static get [Symbol.species]() {
    return Float32Array;
  }

  protected static requiredDimensionsCount: number | undefined = undefined;
  private declare _loadedVector: Float32Array | null;
  private declare _requiredDimensionsCount: number;

  constructor(
    options:
      | {
          owner: Account | Group;
        }
      | {
          fromRaw: RawBinaryCoStream;
        },
  ) {
    super();

    const dimensionsCount = (this.constructor as typeof CoVector)
      .requiredDimensionsCount;

    if (dimensionsCount === undefined) {
      throw new Error(
        "Instantiating CoVector without a dimensions count is not allowed. Use co.vector(...).create() instead.",
      );
    }

    const proxy = new Proxy(this, CoVectorProxyHandler as ProxyHandler<this>);

    let raw: RawBinaryCoStream;

    if ("fromRaw" in options) {
      raw = options.fromRaw;
    } else {
      const rawOwner = options.owner.$jazz.raw;
      raw = rawOwner.createBinaryStream();
    }

    Object.defineProperties(this, {
      [TypeSym]: { value: "BinaryCoStream", enumerable: false },
      $jazz: {
        value: new CoVectorJazzApi(proxy, raw),
        enumerable: false,
      },
      _loadedVector: { value: null, enumerable: false, writable: true },
      _requiredDimensionsCount: {
        value: dimensionsCount,
        enumerable: false,
        writable: false,
      },
    });

    return proxy;
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

    const coVector = new this(parseCoValueCreateOptions(options));
    coVector._loadedVector = vectorAsFloat32Array;

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

  static toByteArray(vector: Float32Array): Uint8Array {
    // zero copy view of the vector bytes
    return new Uint8Array(vector.buffer, vector.byteOffset, vector.byteLength);
  }

  static fromByteArray(bytesChunks: Uint8Array[]): Float32Array {
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

  get vector(): Float32Array {
    if (this._loadedVector !== null) {
      return this._loadedVector;
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

    this._loadedVector = vector;

    return vector;
  }

  /**
   * Get a JSON representation of the `CoVector`
   * @category Content
   */
  toJSON(): Array<number> {
    return Array.from(this.vector);
  }

  valueOf() {
    return this.vector as this;
  }

  /** @internal */
  [inspect]() {
    return this.toJSON();
  }

  [Symbol.toPrimitive]() {
    return this.vector;
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
  ): Promise<CoVector | null> {
    const coVector = await loadCoValueWithoutMe(this, id, options);

    /**
     * We are only interested in the entire vector. Since most vectors are small (<15kB),
     * we can wait for the stream to be complete before returning the vector
     */
    if (!coVector?.$jazz.raw.isBinaryStreamEnded()) {
      return new Promise<CoVector>((resolve) => {
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

    return coVector;
  }

  /**
   * Subscribe to a `CoVector`, when you have an ID but don't have a `CoVector` instance yet
   * @category Subscription & Loading
   * @deprecated Use `co.vector(...).subscribe` instead.
   */
  static subscribe<V extends CoVector, const R extends RefsToResolve<V>>(
    this: CoValueClass<V>,
    id: ID<V>,
    listener: (value: Resolved<V, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<V extends CoVector, const R extends RefsToResolve<V>>(
    this: CoValueClass<V>,
    id: ID<V>,
    options: SubscribeListenerOptions<V, R>,
    listener: (value: Resolved<V, R>, unsubscribe: () => void) => void,
  ): () => void;
  static subscribe<V extends CoVector, const R extends RefsToResolve<V>>(
    this: CoValueClass<V>,
    id: ID<V>,
    ...args: SubscribeRestArgs<V, R>
  ): () => void {
    const { options, listener } = parseSubscribeRestArgs(args);
    return subscribeToCoValueWithoutMe<V, R>(this, id, options, listener);
  }

  // Vector operations
  /**
   * Calculate the magnitude of a vector.
   */
  static magnitude(vector: Float32Array | CoVector): number {
    return VectorCalculation.magnitude(
      vector instanceof CoVector ? vector.vector : vector,
    );
  }

  /**
   * Calculate the magnitude of this vector.
   */
  magnitude(): number {
    return VectorCalculation.magnitude(this.vector);
  }

  /**
   * Normalize a vector.
   * @returns A new instance of a normalized vector.
   */
  static normalize(vector: Float32Array | CoVector): Float32Array {
    return VectorCalculation.normalize(
      vector instanceof CoVector ? vector.vector : vector,
    );
  }

  /**
   * Normalize this vector.
   * @returns A new instance of a normalized vector.
   */
  normalize(): Float32Array {
    return VectorCalculation.normalize(this.vector);
  }

  /**
   * Calculate the dot product of two vectors.
   */
  static dotProduct(
    vectorA: Float32Array | CoVector,
    vectorB: Float32Array | CoVector,
  ): number {
    return VectorCalculation.dotProduct(
      vectorA instanceof CoVector ? vectorA.vector : vectorA,
      vectorB instanceof CoVector ? vectorB.vector : vectorB,
    );
  }

  /**
   * Calculate the dot product of this vector and another vector.
   */
  dotProduct(otherVector: CoVector | Float32Array): number {
    return VectorCalculation.dotProduct(
      this.vector,
      otherVector instanceof CoVector ? otherVector.vector : otherVector,
    );
  }

  /**
   * Calculate the cosine similarity between two vectors.
   *
   * @returns A value between `-1` and `1`:
   * - `1` means the vectors are identical
   * - `0` means the vectors are orthogonal (i.e. no similarity)
   * - `-1` means the vectors are opposite direction (perfectly dissimilar)
   */
  static cosineSimilarity(
    vectorA: CoVector | Float32Array,
    vectorB: CoVector | Float32Array,
  ): number {
    return VectorCalculation.cosineSimilarity(
      vectorA instanceof CoVector ? vectorA.vector : vectorA,
      vectorB instanceof CoVector ? vectorB.vector : vectorB,
    );
  }

  /**
   * Calculate the cosine similarity between this vector and another vector.
   *
   * @returns A value between `-1` and `1`:
   * - `1` means the vectors are identical
   * - `0` means the vectors are orthogonal (i.e. no similarity)
   * - `-1` means the vectors are opposite direction (perfectly dissimilar)
   */
  cosineSimilarity(otherVector: CoVector | Float32Array): number {
    return VectorCalculation.cosineSimilarity(
      this.vector,
      otherVector instanceof CoVector ? otherVector.vector : otherVector,
    );
  }

  /**
   * Check if this vector is equal to another vector.
   */
  equals(otherVector: CoVector | Float32Array): boolean {
    return this.vector.every((value, index) => value === otherVector[index]);
  }

  // CoVector instance properties
  get length(): number {
    return this.vector.length;
  }
  get buffer(): ArrayBuffer {
    return this.vector.buffer as ArrayBuffer;
  }
  get byteOffset(): number {
    return this.vector.byteOffset;
  }
  get byteLength(): number {
    return this.vector.byteLength;
  }
  [Symbol.iterator](): ArrayIterator<number> {
    return this.vector[Symbol.iterator]();
  }

  // CoVector getters & Float32Array-like allowed methods
  override at(index: number): number | undefined {
    return this.vector.at(index);
  }

  override entries() {
    return this.vector.entries();
  }

  override every(
    predicate: (value: number, index: number, array: this) => unknown,
    thisArg?: any,
  ): boolean {
    return this.vector.every(predicate as any, thisArg);
  }

  override filter(
    predicate: (value: number, index: number, array: this) => boolean,
    thisArg?: any,
  ) {
    return this.vector.filter(predicate as any, thisArg);
  }

  override find(
    predicate: (value: number, index: number, array: this) => boolean,
    thisArg?: any,
  ): number | undefined {
    return this.vector.find(predicate as any, thisArg);
  }

  override findIndex(
    predicate: (value: number, index: number, array: this) => boolean,
    thisArg?: any,
  ): number {
    return this.vector.findIndex(predicate as any, thisArg);
  }

  override findLast(
    predicate: (value: number, index: number, array: this) => boolean,
    thisArg?: any,
  ): number | undefined {
    return this.vector.findLast(predicate as any, thisArg);
  }

  override findLastIndex(
    predicate: (value: number, index: number, array: this) => boolean,
    thisArg?: any,
  ): number {
    return this.vector.findLastIndex(predicate as any, thisArg);
  }

  override forEach(
    callbackFn: (value: number, index: number, array: this) => void,
    thisArg?: any,
  ): void {
    return this.vector.forEach(callbackFn as any, thisArg);
  }

  override includes(value: number): boolean {
    return this.vector.includes(value);
  }

  override indexOf(value: number): number {
    return this.vector.indexOf(value);
  }

  override join(value?: string): string {
    return this.vector.join(value);
  }

  override keys(): ArrayIterator<number> {
    return this.vector.keys();
  }

  override lastIndexOf(value: number): number {
    return this.vector.lastIndexOf(value);
  }

  override map(
    callbackfn: (value: number, index: number, array: this) => number,
    thisArg?: any,
  ) {
    return this.vector.map(callbackfn as any, thisArg);
  }

  reduce(
    callbackfn: (
      previousValue: number,
      currentValue: number,
      currentIndex: number,
      array: this,
    ) => number,
    initialValue?: number,
  ): number {
    return (this.vector as any).reduce(
      callbackfn as any,
      initialValue as any,
    ) as number;
  }

  reduceRight(
    callbackfn: (
      previousValue: number,
      currentValue: number,
      currentIndex: number,
      array: this,
    ) => number,
    initialValue?: number,
  ): number {
    return (this.vector as any).reduceRight(
      callbackfn as any,
      initialValue as any,
    ) as number;
  }

  override slice(start?: number, end?: number) {
    return this.vector.slice(start, end);
  }

  override some(
    predicate: (value: number, index: number, array: this) => unknown,
    thisArg?: any,
  ): boolean {
    return this.vector.some(predicate as any, thisArg);
  }

  override toLocaleString(
    locales?: string | string[],
    options?: Intl.NumberFormatOptions,
  ): string {
    if (locales === undefined) {
      return this.vector.toLocaleString();
    }
    return this.vector.toLocaleString(locales, options);
  }

  override toReversed() {
    return this.vector.toReversed();
  }

  override toSorted(compareFn?: (a: number, b: number) => number) {
    return this.vector.toSorted(compareFn);
  }

  override toString(): string {
    return this.vector.toString();
  }

  override values(): ArrayIterator<number> {
    return this.vector.values();
  }

  // CoVector setters & mutators overrides, as CoVectors aren't meant to be mutated
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
   */
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
  /**
   * Calling `subarray` on a CoVector is forbidden. CoVectors are immutable.
   * @deprecated If you want to change the vector, replace the former instance of CoVector with a new one.
   */
  override subarray(begin?: number, end?: number): never {
    throw new Error("Cannot mutate a CoVector using `subarray`");
  }
  /**
   * Calling `with` on a CoVector is forbidden. CoVectors are immutable.
   * @deprecated If you want to change the vector, replace the former instance of CoVector with a new one.
   */
  override with(index: number, value: number): never {
    throw new Error("Cannot mutate a CoVector using `with`");
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
}

const CoVectorProxyHandler: ProxyHandler<CoVector> = {
  get(target, key, receiver) {
    if (typeof key === "string" && !isNaN(+key)) {
      return target.at(Number(key));
    } else {
      return Reflect.get(target, key, receiver);
    }
  },
  set(target, key, value, receiver) {
    if (typeof key === "string" && !isNaN(+key)) {
      throw new Error("Cannot mutate a CoVector.");
    } else {
      return Reflect.set(target, key, value, receiver);
    }
  },
  has(target, key) {
    if (typeof key === "string" && !isNaN(+key)) {
      const length = target.length;
      return Number(key) >= 0 && Number(key) < length;
    } else {
      return Reflect.has(target, key);
    }
  },
  ownKeys(target) {
    const keys = Reflect.ownKeys(target);

    const data = target.vector;
    if (data) {
      // Add numeric indices for all entries in the vector
      const indexKeys = Array.from({ length: data.length }, (_, i) =>
        String(i),
      );
      keys.push(...indexKeys);
    }

    return keys;
  },
  getOwnPropertyDescriptor(target, key) {
    if (typeof key === "string" && !isNaN(+key)) {
      const i = +key;
      if (i >= 0 && i < target.length) {
        return {
          value: target.vector[i],
          enumerable: true,
          configurable: true,
          writable: false, // CoVectors are immutable
        };
      }
    } else if (key in target) {
      return Reflect.getOwnPropertyDescriptor(target, key);
    }
  },
};

const VectorCalculation = {
  magnitude: (vector: Float32Array) => {
    return Math.sqrt(vector.reduce((s, x) => s + x * x, 0));
  },
  normalize: (vector: Float32Array) => {
    const mag = VectorCalculation.magnitude(vector);

    if (mag === 0) {
      return new Float32Array(vector.length).fill(0);
    }

    return vector.map((v) => v / mag);
  },
  dotProduct: (vectorA: Float32Array, vectorB: Float32Array) => {
    if (vectorA.length !== vectorB.length) {
      throw new Error(
        `Vector dimensions don't match: ${vectorA.length} vs ${vectorB.length}`,
      );
    }

    return vectorA.reduce((sum, a, i) => sum + a * vectorB[i]!, 0);
  },
  cosineSimilarity: (vectorA: Float32Array, vectorB: Float32Array) => {
    const magnitudeA = VectorCalculation.magnitude(vectorA);
    const magnitudeB = VectorCalculation.magnitude(vectorB);

    if (magnitudeA === 0 || magnitudeB === 0) {
      return 0;
    }

    const dotProductAB = VectorCalculation.dotProduct(vectorA, vectorB);
    return dotProductAB / (magnitudeA * magnitudeB);
  },
};
