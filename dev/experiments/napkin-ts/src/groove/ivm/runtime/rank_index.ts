// In-memory ordered index with exact ranks and path-copying snapshots (AVL).
import type { Cmp } from "../../../support/collections.js";

interface Node<K, V> {
  entry: [K, V];
  left: Link<K, V>;
  right: Link<K, V>;
  height: number;
  count: number;
}
type Link<K, V> = Node<K, V> | undefined;

const height = (link: Link<any, any>) => link?.height ?? 0;
const count = (link: Link<any, any>) => link?.count ?? 0;

function make<K, V>(entry: [K, V], left: Link<K, V>, right: Link<K, V>): Node<K, V> {
  return {
    entry,
    left,
    right,
    height: 1 + Math.max(height(left), height(right)),
    count: 1 + count(left) + count(right),
  };
}

function rotateLeft<K, V>(root: Node<K, V>): Node<K, V> {
  const pivot = root.right!;
  return make(pivot.entry, make(root.entry, root.left, pivot.left), pivot.right);
}

function rotateRight<K, V>(root: Node<K, V>): Node<K, V> {
  const pivot = root.left!;
  return make(pivot.entry, pivot.left, make(root.entry, pivot.right, root.right));
}

function balance<K, V>(node: Node<K, V>): Node<K, V> {
  if (height(node.left) > height(node.right) + 1) {
    const left = node.left!;
    if (height(left.right) > height(left.left))
      node = make(node.entry, rotateLeft(left), node.right);
    return rotateRight(node);
  }
  if (height(node.right) > height(node.left) + 1) {
    const right = node.right!;
    if (height(right.left) > height(right.right))
      node = make(node.entry, node.left, rotateRight(right));
    return rotateLeft(node);
  }
  return node;
}

export class RankIndex<K, V> {
  constructor(
    private readonly cmp: Cmp<K>,
    private root: Link<K, V> = undefined,
  ) {}

  clone(): RankIndex<K, V> {
    return new RankIndex(this.cmp, this.root);
  }

  private insertAt(link: Link<K, V>, entry: [K, V]): Node<K, V> {
    if (!link) return make(entry, undefined, undefined);
    const c = this.cmp(entry[0], link.entry[0]);
    if (c === 0) return { ...link, entry };
    return balance(
      c < 0
        ? make(link.entry, this.insertAt(link.left, entry), link.right)
        : make(link.entry, link.left, this.insertAt(link.right, entry)),
    );
  }

  private popFirst(link: Node<K, V>): [[K, V], Link<K, V>] {
    if (!link.left) return [link.entry, link.right];
    const [entry, left] = this.popFirst(link.left);
    return [entry, balance(make(link.entry, left, link.right))];
  }

  private removeAt(link: Link<K, V>, key: K): Link<K, V> {
    if (!link) return undefined;
    const c = this.cmp(key, link.entry[0]);
    if (c < 0) return balance(make(link.entry, this.removeAt(link.left, key), link.right));
    if (c > 0) return balance(make(link.entry, link.left, this.removeAt(link.right, key)));
    if (!link.left) return link.right;
    if (!link.right) return link.left;
    const [entry, right] = this.popFirst(link.right);
    return balance(make(entry, link.left, right));
  }

  insert(key: K, value: V): void {
    this.root = this.insertAt(this.root, [key, value]);
  }

  remove(key: K): void {
    this.root = this.removeAt(this.root, key);
  }

  contains(key: K): boolean {
    let next = this.root;
    while (next) {
      const c = this.cmp(key, next.entry[0]);
      if (c === 0) return true;
      next = c < 0 ? next.left : next.right;
    }
    return false;
  }

  /** Number of keys strictly before `key`, whether or not it is present. */
  rank(key: K): number {
    let rank = 0;
    let next = this.root;
    while (next) {
      const c = this.cmp(key, next.entry[0]);
      if (c < 0) next = next.left;
      else if (c === 0) return rank + count(next.left);
      else {
        rank += count(next.left) + 1;
        next = next.right;
      }
    }
    return rank;
  }
}
