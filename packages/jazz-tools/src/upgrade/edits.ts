/** Position-based text edits, applied back to front so earlier offsets stay valid. */
export interface TextEdit {
  start: number;
  end: number;
  text: string;
}

export function applyEdits(text: string, edits: TextEdit[]): string {
  const sorted = [...edits].sort((a, b) => b.start - a.start || b.end - a.end);
  let out = text;
  let last = Infinity;
  for (const edit of sorted) {
    if (edit.end > last) throw new Error(`overlapping edits at ${edit.start}-${edit.end}`);
    out = out.slice(0, edit.start) + edit.text + out.slice(edit.end);
    last = edit.start;
  }
  return out;
}

/** A module specifier for `target` from `fromFile`'s directory, in the style of `like`. */
export function specifierLike(like: string, newBasename: string, keepExtension = true): string {
  const slash = like.lastIndexOf("/");
  const dir = slash === -1 ? "" : like.slice(0, slash + 1);
  const base = slash === -1 ? like : like.slice(slash + 1);
  const ext = /\.(js|ts|mjs|mts|jsx|tsx)$/.exec(base)?.[0] ?? "";
  return `${dir}${newBasename}${keepExtension ? ext : ""}`;
}
