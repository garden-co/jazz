export type TextSplice = { at: number; delete: number; insert: string };

const isHighSurrogate = (code: number) => code >= 0xd800 && code <= 0xdbff;
const isLowSurrogate = (code: number) => code >= 0xdc00 && code <= 0xdfff;

/**
 * The single splice that turns `base` into `next`, in UTF-16 code units (the
 * coordinates Jazz uses for string diffs by default). The edited range never
 * splits a surrogate pair, which Jazz would reject.
 *
 * Sending a splice rather than the whole string lets two people type in the
 * same block at once: Jazz merges their splices instead of letting the last
 * full write win.
 */
export function textSplice(base: string, next: string): TextSplice | null {
  if (base === next) return null;
  let start = 0;
  const shorter = Math.min(base.length, next.length);
  while (start < shorter && base.charCodeAt(start) === next.charCodeAt(start)) start++;
  if (start > 0 && isHighSurrogate(base.charCodeAt(start - 1))) start--;

  let baseEnd = base.length;
  let nextEnd = next.length;
  while (
    baseEnd > start &&
    nextEnd > start &&
    base.charCodeAt(baseEnd - 1) === next.charCodeAt(nextEnd - 1)
  ) {
    baseEnd--;
    nextEnd--;
  }
  if (baseEnd < base.length && isLowSurrogate(base.charCodeAt(baseEnd))) {
    baseEnd++;
    nextEnd++;
  }
  return { at: start, delete: baseEnd - start, insert: next.slice(start, nextEnd) };
}

/** Apply a splice locally; used by tests to check `textSplice` round-trips. */
export function applyTextSplice(base: string, splice: TextSplice): string {
  return base.slice(0, splice.at) + splice.insert + base.slice(splice.at + splice.delete);
}
