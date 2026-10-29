// Text merge columns (`stops.publicDescription`, `stopNotes.body`) are edited
// in a form, so the save carries the form's edits over to the current value.

type TextSplice = { at: number; delete: number; insert: string };

const isHighSurrogate = (code: number) => code >= 0xd800 && code <= 0xdbff;
const isLowSurrogate = (code: number) => code >= 0xdc00 && code <= 0xdfff;

/**
 * The single splice that turns `base` into `next`, in UTF-16 code units. The
 * edited range never splits a surrogate pair.
 */
function textSplice(base: string, next: string): TextSplice | null {
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

function applyTextSplice(base: string, splice: TextSplice): string {
  return base.slice(0, splice.at) + splice.insert + base.slice(splice.at + splice.delete);
}

/**
 * Carry the edits made in a form since `base` (the value when editing began)
 * over to `remote`, the value Jazz holds now. A bandmate's edit that arrived
 * while the form was open then survives the save: the server merges a write
 * against the version the writer held, so writing the form's text as is would
 * undo it.
 *
 * Both sides are one splice away from `base`. Where they touch different
 * parts, both apply. Where they overlap, the local side wins the span both
 * touched, as the server does for the later of two overlapping writes.
 */
export function rebaseText(base: string, local: string, remote: string): string {
  const mine = textSplice(base, local);
  const theirs = textSplice(base, remote);
  if (!mine) return remote;
  if (!theirs) return local;
  const mineEnd = mine.at + mine.delete;
  const theirsEnd = theirs.at + theirs.delete;
  if (mineEnd <= theirs.at && !(mine.delete === 0 && theirs.delete === 0 && mine.at === theirs.at))
    return applyTextSplice(remote, mine);
  if (theirsEnd <= mine.at) {
    const shift = theirs.insert.length - theirs.delete;
    return applyTextSplice(remote, { ...mine, at: mine.at + shift });
  }
  // Overlap: the local text of the span either side touched, between the
  // remote text before and after it.
  const from = Math.min(mine.at, theirs.at);
  const after = base.length - Math.max(mineEnd, theirsEnd);
  return (
    remote.slice(0, from) +
    local.slice(from, local.length - after) +
    remote.slice(remote.length - after)
  );
}
