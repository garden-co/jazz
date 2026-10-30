import type { Side } from "./markdown.js";

/** Past this many cells, the changed part of two values is not diffed and
 * offsets in it snap to its edges. */
const DIFF_CELLS = 1 << 20;

/**
 * Maps an offset in `before` to the same place in `after`, two versions of
 * one value. Offsets outside the changed middle shift with it; inside, they
 * follow the characters both versions keep. At a place where text was
 * inserted, `"before"` stays ahead of the insert and `"after"` moves past
 * it.
 */
export function mapOffset(before: string, after: string, offset: number, side: Side): number {
  const { start, endBefore, endAfter } = changedMiddle(before, after);
  if (offset < start || (offset === start && side === "before")) return offset;
  if (offset > endBefore || (offset === endBefore && side === "after")) {
    return offset + endAfter - endBefore;
  }
  const a = before.slice(start, endBefore);
  const b = after.slice(start, endAfter);
  if (a.length * b.length > DIFF_CELLS) return side === "before" ? start : endAfter;
  return start + mapThroughDiff(a, b, offset - start, side);
}

/** The middle where `before` and `after` differ, between their common
 * start and end. */
export function changedMiddle(before: string, after: string) {
  let start = 0;
  const shorter = Math.min(before.length, after.length);
  while (start < shorter && before.charCodeAt(start) === after.charCodeAt(start)) start++;
  let end = 0;
  while (
    end < shorter - start &&
    before.charCodeAt(before.length - 1 - end) === after.charCodeAt(after.length - 1 - end)
  ) {
    end++;
  }
  return { start, endBefore: before.length - end, endAfter: after.length - end };
}

/** Maps `offset` in `a` into `b` through their longest common
 * subsequence. */
function mapThroughDiff(a: string, b: string, offset: number, side: Side): number {
  const n = a.length;
  const m = b.length;
  // lcs[i][j]: the longest common subsequence of a[i..] and b[j..].
  const lcs = new Uint32Array((n + 1) * (m + 1));
  const at = (i: number, j: number) => i * (m + 1) + j;
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      lcs[at(i, j)] =
        a.charCodeAt(i) === b.charCodeAt(j)
          ? lcs[at(i + 1, j + 1)]! + 1
          : Math.max(lcs[at(i + 1, j)]!, lcs[at(i, j + 1)]!);
    }
  }
  // Walk the alignment up to `offset`: the last kept pair before it and
  // the first at or after it.
  let i = 0;
  let j = 0;
  let afterKept = 0;
  while (i < n && j < m) {
    if (a.charCodeAt(i) === b.charCodeAt(j) && lcs[at(i, j)] === lcs[at(i + 1, j + 1)]! + 1) {
      if (i >= offset) {
        // A kept character at or past the offset: inserts before it lie
        // between `afterKept` and `j`.
        return side === "before" ? afterKept : j;
      }
      i++;
      j++;
      afterKept = j;
    } else if (lcs[at(i + 1, j)]! >= lcs[at(i, j + 1)]!) {
      i++;
    } else {
      j++;
    }
  }
  return side === "before" ? afterKept : m;
}
