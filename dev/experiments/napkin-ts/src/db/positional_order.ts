import { SortedMap, type Cmp } from "../support/collections.js";

const NIL = -1;
const MASK = (1n << 64n) - 1n;

interface TreapNode<K> {
  key: K | undefined;
  left: number;
  right: number;
  parent: number;
  size: number;
  priority: bigint;
}

function splitmix64(state: bigint): bigint {
  let z = state;
  z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & MASK;
  z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & MASK;
  return z ^ (z >> 31n);
}

/** Keyed sequence with O(log n) positional edits (implicit treap + key map). */
export class PositionalOrder<K> {
  private slots: SortedMap<K, number>;
  private nodes: TreapNode<K>[] = [];
  private root = NIL;
  private seed = 0x9e3779b97f4a7c15n;
  private anonymous = 0;

  constructor(private readonly cmp: Cmp<K>) {
    this.slots = new SortedMap(cmp);
  }

  static fromOrdered<K>(cmp: Cmp<K>, keys: Iterable<K>): PositionalOrder<K> {
    const order = new PositionalOrder<K>(cmp);
    for (const key of keys) {
      if (!order.insert(order.len(), key)) throw { duplicate: key };
    }
    return order;
  }

  len(): number {
    return this.nodes.length;
  }

  position(key: K): number | undefined {
    const found = this.slots.get(key);
    if (found === undefined) return undefined;
    let node: number = found;
    let rank = this.size(this.nodes[node].left);
    for (;;) {
      const parent = this.nodes[node].parent;
      if (parent === NIL) return rank;
      if (this.nodes[parent].right === node) rank += this.size(this.nodes[parent].left) + 1;
      node = parent;
    }
  }

  anonymousLen(): number {
    return this.anonymous;
  }

  anonymize(key: K): number | undefined {
    const position = this.position(key);
    if (position === undefined) return undefined;
    const node = this.slots.delete(key)!;
    this.nodes[node].key = undefined;
    this.anonymous++;
    return position;
  }

  get(position: number): K | undefined {
    let node = this.root;
    while (node !== NIL) {
      const cur = this.nodes[node];
      const left = this.size(cur.left);
      if (position < left) node = cur.left;
      else if (position === left) return cur.key;
      else {
        position -= left + 1;
        node = cur.right;
      }
    }
    return undefined;
  }

  at(position: number): K {
    const k = this.get(position);
    if (k === undefined) throw new Error(`position ${position} out of bounds (len ${this.len()})`);
    return k;
  }

  insert(position: number, key: K): boolean {
    if (this.slots.has(key)) return false;
    position = Math.min(position, this.len());
    const node = this.nodes.length;
    if (node >= 0xffffffff) throw new Error("positional order exceeds u32 nodes");
    this.seed = (this.seed + 0x9e3779b97f4a7c15n) & MASK;
    const priority = splitmix64(this.seed);
    this.slots.set(key, node);
    this.nodes.push({ key, left: NIL, right: NIL, parent: NIL, size: 1, priority });
    const [before, after] = this.split(this.root, position);
    const joined = this.merge(before, node);
    this.root = this.merge(joined, after);
    this.nodes[this.root].parent = NIL;
    return true;
  }

  remove(key: K): number | undefined {
    const position = this.position(key);
    if (position === undefined) return undefined;
    const node = this.slots.delete(key)!;
    const [before, rest] = this.split(this.root, position);
    const [, after] = this.split(rest, 1);
    this.root = this.merge(before, after);
    if (this.root !== NIL) this.nodes[this.root].parent = NIL;
    this.release(node);
    return position;
  }

  *iter(): IterableIterator<K | undefined> {
    const stack: number[] = [];
    const descend = (n: number) => {
      while (n !== NIL) {
        stack.push(n);
        n = this.nodes[n].left;
      }
    };
    descend(this.root);
    while (stack.length) {
      const n = stack.pop()!;
      descend(this.nodes[n].right);
      yield this.nodes[n].key;
    }
  }

  keysUnordered(): readonly K[] {
    return this.slots.keysArray();
  }

  equalsArray(other: K[]): boolean {
    if (this.len() !== other.length) return false;
    let i = 0;
    for (const k of this.iter()) if (k === undefined || this.cmp(k, other[i++]) !== 0) return false;
    return true;
  }

  private size(node: number): number {
    return node === NIL ? 0 : this.nodes[node].size;
  }

  private pull(node: number): void {
    const { left, right } = this.nodes[node];
    this.nodes[node].size = 1 + this.size(left) + this.size(right);
    if (left !== NIL) this.nodes[left].parent = node;
    if (right !== NIL) this.nodes[right].parent = node;
  }

  private split(node: number, count: number): [number, number] {
    if (node === NIL) return [NIL, NIL];
    const left = this.nodes[node].left;
    const leftSize = this.size(left);
    if (count <= leftSize) {
      const [before, after] = this.split(left, count);
      this.nodes[node].left = after;
      this.pull(node);
      return [before, node];
    }
    const [before, after] = this.split(this.nodes[node].right, count - leftSize - 1);
    this.nodes[node].right = before;
    this.pull(node);
    return [node, after];
  }

  private merge(left: number, right: number): number {
    if (left === NIL) return right;
    if (right === NIL) return left;
    if (this.nodes[left].priority >= this.nodes[right].priority) {
      this.nodes[left].right = this.merge(this.nodes[left].right, right);
      this.pull(left);
      return left;
    }
    this.nodes[right].left = this.merge(left, this.nodes[right].left);
    this.pull(right);
    return right;
  }

  private release(node: number): void {
    const last = this.nodes.length - 1;
    const moved = this.nodes.pop()!;
    if (node === last) return;
    this.nodes[node] = moved;
    const { parent, left, right } = moved;
    if (parent === NIL) {
      if (this.root === last) this.root = node;
    } else {
      const p = this.nodes[parent];
      if (p.left === last) p.left = node;
      else if (p.right === last) p.right = node;
    }
    if (left !== NIL) this.nodes[left].parent = node;
    if (right !== NIL) this.nodes[right].parent = node;
    if (moved.key !== undefined) this.slots.set(moved.key, node);
  }
}
