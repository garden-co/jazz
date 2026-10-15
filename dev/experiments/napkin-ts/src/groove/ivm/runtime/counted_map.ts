// Ordered weighted map with order-statistic ranks (#3505): sorted chunks plus
// a Fenwick tree over each chunk's positive-weight count.
import type { Cmp } from "../../../support/collections.js";

const MAX_CHUNK = 256;
const MIN_CHUNK = MAX_CHUNK / 4;

interface Chunk<K> {
  entries: [K, number][];
  positive: number;
}

const lowbit = (i: number) => i & -i;
const countPositive = <K>(entries: [K, number][]) => entries.filter(([, w]) => w > 0).length;

export class CountedMap<K> {
  private chunks: Chunk<K>[] = [];
  private positiveTree: number[] = [0];
  len = 0;

  constructor(private readonly cmp: Cmp<K>) {}

  static fromEntries<K>(cmp: Cmp<K>, entries: Iterable<[K, number]>): CountedMap<K> {
    const map = new CountedMap<K>(cmp);
    for (const [k, w] of entries) map.insert(k, w);
    return map;
  }

  private rebuildPositiveTree(): void {
    const count = this.chunks.length;
    this.positiveTree = new Array(count + 1).fill(0);
    for (let index = 1; index <= count; index++) {
      this.positiveTree[index] += this.chunks[index - 1].positive;
      const parent = index + lowbit(index);
      if (parent <= count) this.positiveTree[parent] += this.positiveTree[index];
    }
  }

  private adjustPositive(chunkIndex: number, gained: boolean, lost: boolean): void {
    if (gained === lost) return;
    for (let index = chunkIndex + 1; index < this.positiveTree.length; index += lowbit(index)) {
      this.positiveTree[index] += gained ? 1 : -1;
    }
  }

  private positiveInChunksBefore(chunkIndex: number): number {
    let total = 0;
    for (let index = chunkIndex; index > 0; index &= index - 1) total += this.positiveTree[index];
    return total;
  }

  private chunkFor(key: K): number {
    let lo = 0;
    let hi = this.chunks.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const entries = this.chunks[mid].entries;
      if (this.cmp(entries[entries.length - 1][0], key) < 0) lo = mid + 1;
      else hi = mid;
    }
    return Math.min(lo, Math.max(this.chunks.length - 1, 0));
  }

  private search(entries: [K, number][], key: K): { found: boolean; index: number } {
    let lo = 0;
    let hi = entries.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      const c = this.cmp(entries[mid][0], key);
      if (c === 0) return { found: true, index: mid };
      if (c < 0) lo = mid + 1;
      else hi = mid;
    }
    return { found: false, index: lo };
  }

  get(key: K): number | undefined {
    const chunk = this.chunks[this.chunkFor(key)];
    if (!chunk) return undefined;
    const { found, index } = this.search(chunk.entries, key);
    return found ? chunk.entries[index][1] : undefined;
  }

  insert(key: K, weight: number): number | undefined {
    if (this.chunks.length === 0) {
      this.chunks.push({ entries: [[key, weight]], positive: weight > 0 ? 1 : 0 });
      this.len = 1;
      this.rebuildPositiveTree();
      return undefined;
    }
    const chunkIndex = this.chunkFor(key);
    const chunk = this.chunks[chunkIndex];
    const { found, index } = this.search(chunk.entries, key);
    if (found) {
      const old = chunk.entries[index][1];
      chunk.entries[index][1] = weight;
      chunk.positive = chunk.positive - (old > 0 ? 1 : 0) + (weight > 0 ? 1 : 0);
      this.adjustPositive(chunkIndex, weight > 0, old > 0);
      return old;
    }
    chunk.entries.splice(index, 0, [key, weight]);
    if (weight > 0) chunk.positive++;
    this.len++;
    if (chunk.entries.length > MAX_CHUNK) {
      this.splitChunk(chunkIndex);
      this.rebuildPositiveTree();
    } else {
      this.adjustPositive(chunkIndex, weight > 0, false);
    }
    return undefined;
  }

  private splitChunk(chunkIndex: number): void {
    const chunk = this.chunks[chunkIndex];
    const tail = chunk.entries.splice(chunk.entries.length >> 1);
    const tailPositive = countPositive(tail);
    chunk.positive -= tailPositive;
    this.chunks.splice(chunkIndex + 1, 0, { entries: tail, positive: tailPositive });
  }

  remove(key: K): number | undefined {
    if (this.chunks.length === 0) return undefined;
    const chunkIndex = this.chunkFor(key);
    const chunk = this.chunks[chunkIndex];
    const { found, index } = this.search(chunk.entries, key);
    if (!found) return undefined;
    const [[, weight]] = chunk.entries.splice(index, 1);
    if (weight > 0) chunk.positive--;
    this.len--;
    if (chunk.entries.length === 0) {
      this.chunks.splice(chunkIndex, 1);
      this.rebuildPositiveTree();
    } else if (chunk.entries.length < MIN_CHUNK && this.chunks.length > 1) {
      this.mergeIntoNeighbour(chunkIndex);
      this.rebuildPositiveTree();
    } else {
      this.adjustPositive(chunkIndex, false, weight > 0);
    }
    return weight;
  }

  private mergeIntoNeighbour(chunkIndex: number): void {
    const left = chunkIndex + 1 < this.chunks.length ? chunkIndex : chunkIndex - 1;
    const [right] = this.chunks.splice(left + 1, 1);
    const merged = this.chunks[left];
    merged.entries.push(...right.entries);
    merged.positive += right.positive;
    if (merged.entries.length > MAX_CHUNK) this.splitChunk(left);
  }

  /** Entries with a positive weight whose key is strictly below `key`. */
  positiveBefore(key: K): number {
    if (this.chunks.length === 0) return 0;
    const chunkIndex = this.chunkFor(key);
    const before = this.positiveInChunksBefore(chunkIndex);
    const chunk = this.chunks[chunkIndex];
    let within = 0;
    while (within < chunk.entries.length && this.cmp(chunk.entries[within][0], key) < 0) within++;
    return (
      before +
      (chunk.positive === chunk.entries.length
        ? within
        : countPositive(chunk.entries.slice(0, within)))
    );
  }

  *[Symbol.iterator](): IterableIterator<[K, number]> {
    for (const chunk of this.chunks) yield* chunk.entries;
  }
}
