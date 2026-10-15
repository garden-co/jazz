// Ordered collections (Rust BTreeMap/BTreeSet) and structural-key maps.
// Use plain Map/Set when Rust uses HashMap/HashSet or order is irrelevant.
import { compareBytes } from "./bytes.js";

export type Cmp<K> = (a: K, b: K) => number;

export const cmpNum: Cmp<number> = (a, b) => a - b;
export const cmpBig: Cmp<bigint> = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
export const cmpStr: Cmp<string> = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
export const cmpBytes: Cmp<Uint8Array> = compareBytes;
export function cmpTuple(...cmps: Cmp<any>[]): Cmp<any[]> {
  return (a, b) => {
    for (let i = 0; i < cmps.length; i++) {
      const c = cmps[i](a[i], b[i]);
      if (c !== 0) return c;
    }
    return 0;
  };
}
export function cmpBy<T, K>(key: (t: T) => K, cmp: Cmp<K>): Cmp<T> {
  return (a, b) => cmp(key(a), key(b));
}

/** Sorted-array map with binary search; the Rust BTreeMap stand-in. */
export class SortedMap<K, V> implements Iterable<[K, V]> {
  private keys: K[] = [];
  private vals: V[] = [];
  constructor(
    private readonly cmp: Cmp<K>,
    entries?: Iterable<[K, V]>,
  ) {
    if (entries) for (const [k, v] of entries) this.set(k, v);
  }
  private find(k: K): number {
    let lo = 0;
    let hi = this.keys.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.cmp(this.keys[mid], k) < 0) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }
  get size(): number {
    return this.keys.length;
  }
  get(k: K): V | undefined {
    const i = this.find(k);
    return i < this.keys.length && this.cmp(this.keys[i], k) === 0 ? this.vals[i] : undefined;
  }
  has(k: K): boolean {
    const i = this.find(k);
    return i < this.keys.length && this.cmp(this.keys[i], k) === 0;
  }
  set(k: K, v: V): V | undefined {
    const i = this.find(k);
    if (i < this.keys.length && this.cmp(this.keys[i], k) === 0) {
      const old = this.vals[i];
      this.vals[i] = v;
      return old;
    }
    this.keys.splice(i, 0, k);
    this.vals.splice(i, 0, v);
    return undefined;
  }
  getOrInsert(k: K, make: () => V): V {
    const i = this.find(k);
    if (i < this.keys.length && this.cmp(this.keys[i], k) === 0) return this.vals[i];
    const v = make();
    this.keys.splice(i, 0, k);
    this.vals.splice(i, 0, v);
    return v;
  }
  delete(k: K): V | undefined {
    const i = this.find(k);
    if (i < this.keys.length && this.cmp(this.keys[i], k) === 0) {
      const v = this.vals[i];
      this.keys.splice(i, 1);
      this.vals.splice(i, 1);
      return v;
    }
    return undefined;
  }
  clear(): void {
    this.keys = [];
    this.vals = [];
  }
  first(): [K, V] | undefined {
    return this.keys.length ? [this.keys[0], this.vals[0]] : undefined;
  }
  last(): [K, V] | undefined {
    const n = this.keys.length;
    return n ? [this.keys[n - 1], this.vals[n - 1]] : undefined;
  }
  popFirst(): [K, V] | undefined {
    if (!this.keys.length) return undefined;
    return [this.keys.shift()!, this.vals.shift()!];
  }
  /** Entries with lo <= key (< hi when given). */
  *range(lo?: K, hi?: K, hiInclusive = false): IterableIterator<[K, V]> {
    let i = lo === undefined ? 0 : this.find(lo);
    for (; i < this.keys.length; i++) {
      if (hi !== undefined) {
        const c = this.cmp(this.keys[i], hi);
        if (c > 0 || (c === 0 && !hiInclusive)) return;
      }
      yield [this.keys[i], this.vals[i]];
    }
  }
  retain(f: (k: K, v: V) => boolean): void {
    const ks: K[] = [];
    const vs: V[] = [];
    for (let i = 0; i < this.keys.length; i++)
      if (f(this.keys[i], this.vals[i])) {
        ks.push(this.keys[i]);
        vs.push(this.vals[i]);
      }
    this.keys = ks;
    this.vals = vs;
  }
  keysArray(): readonly K[] {
    return this.keys;
  }
  valuesArray(): readonly V[] {
    return this.vals;
  }
  *[Symbol.iterator](): IterableIterator<[K, V]> {
    for (let i = 0; i < this.keys.length; i++) yield [this.keys[i], this.vals[i]];
  }
  clone(): SortedMap<K, V> {
    const m = new SortedMap<K, V>(this.cmp);
    m.keys = this.keys.slice();
    m.vals = this.vals.slice();
    return m;
  }
}

export class SortedSet<K> implements Iterable<K> {
  private readonly map: SortedMap<K, true>;
  constructor(cmp: Cmp<K>, items?: Iterable<K>) {
    this.map = new SortedMap(cmp);
    if (items) for (const k of items) this.map.set(k, true);
  }
  get size(): number {
    return this.map.size;
  }
  has(k: K): boolean {
    return this.map.has(k);
  }
  add(k: K): boolean {
    return this.map.set(k, true) === undefined;
  }
  delete(k: K): boolean {
    return this.map.delete(k) !== undefined;
  }
  first(): K | undefined {
    return this.map.first()?.[0];
  }
  last(): K | undefined {
    return this.map.last()?.[0];
  }
  *range(lo?: K, hi?: K): IterableIterator<K> {
    for (const [k] of this.map.range(lo, hi)) yield k;
  }
  *[Symbol.iterator](): IterableIterator<K> {
    yield* this.map.keysArray();
  }
  toArray(): K[] {
    return [...this.map.keysArray()];
  }
}

/** HashMap keyed by a structural value: stores entries under key(k). */
export class KeyedMap<K, V> implements Iterable<[K, V]> {
  private readonly inner = new Map<string, [K, V]>();
  constructor(private readonly key: (k: K) => string) {}
  get size(): number {
    return this.inner.size;
  }
  get(k: K): V | undefined {
    return this.inner.get(this.key(k))?.[1];
  }
  has(k: K): boolean {
    return this.inner.has(this.key(k));
  }
  set(k: K, v: V): this {
    this.inner.set(this.key(k), [k, v]);
    return this;
  }
  delete(k: K): boolean {
    return this.inner.delete(this.key(k));
  }
  getOrInsert(k: K, make: () => V): V {
    const key = this.key(k);
    let e = this.inner.get(key);
    if (!e) this.inner.set(key, (e = [k, make()]));
    return e[1];
  }
  values(): IterableIterator<V> {
    return [...this.inner.values()].map((e) => e[1])[Symbol.iterator]();
  }
  [Symbol.iterator](): IterableIterator<[K, V]> {
    return this.inner.values();
  }
}

export function groupBy<T, K>(items: Iterable<T>, key: (t: T) => K): Map<K, T[]> {
  const out = new Map<K, T[]>();
  for (const it of items) {
    const k = key(it);
    let g = out.get(k);
    if (!g) out.set(k, (g = []));
    g.push(it);
  }
  return out;
}
