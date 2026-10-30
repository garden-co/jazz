/**
 * Stateless anchors and ranges into text values, resolved by the runtime's
 * `jazz-text` code: a position or span found again in a later value by the
 * text around it, with no history. Positions count UTF-16 code units, like
 * JavaScript strings. Anchors and ranges are plain objects, so they can be
 * stored in columns or sent as presence; tools and LLMs can author them
 * from the current value alone by leaving out `version` and `position`.
 */
export type TextSide = "before" | "after";

/** A position in a text value (W3C text quote selector, plus a side). Plain
 * JSON, so it can be stored in a `s.json()` column as is. */
export type TextAnchor = {
  /** The value's version that `position` counts in, if known. */
  version?: number;
  /** Where to look first. */
  position?: number;
  /** Which neighbour the anchor sticks to when text is inserted at it. */
  side: TextSide;
  before: string;
  after: string;
};

/** A span of a text value: the text it covered and the text around it. */
export type TextRange = {
  version?: number;
  start?: number;
  end?: number;
  before: string;
  matched: string;
  after: string;
};

/** Where an anchor landed: `confidence` falls as its text changed, and
 * `ambiguous` says another place matched about as well. */
export type ResolvedTextAnchor = {
  position: number;
  confidence: number;
  ambiguous: boolean;
};

export type ResolvedTextRange = {
  start: number;
  end: number;
  confidence: number;
  ambiguous: boolean;
};

export interface TextAnchors {
  captureAnchor(text: string, position: number, side: TextSide, version?: number): TextAnchor;
  resolveAnchor(anchor: TextAnchor, text: string, version?: number): ResolvedTextAnchor;
  captureRange(text: string, start: number, end: number, version?: number): TextRange;
  resolveRange(range: TextRange, text: string, version?: number): ResolvedTextRange;
}

/** The functions a runtime module (WASM or Node) exports for text anchors. */
export interface TextAnchorModule {
  captureTextAnchor(
    text: string,
    version: number | undefined,
    position: number,
    side: unknown,
  ): unknown;
  resolveTextAnchor(anchor: unknown, text: string, version?: number): unknown;
  captureTextRange(text: string, version: number | undefined, start: number, end: number): unknown;
  resolveTextRange(range: unknown, text: string, version?: number): unknown;
}

export function textAnchorsFrom(module: TextAnchorModule): TextAnchors {
  return {
    captureAnchor: (text, position, side, version) =>
      module.captureTextAnchor(text, version, position, side) as TextAnchor,
    resolveAnchor: (anchor, text, version) =>
      module.resolveTextAnchor(anchor, text, version) as ResolvedTextAnchor,
    captureRange: (text, start, end, version) =>
      module.captureTextRange(text, version, start, end) as TextRange,
    resolveRange: (range, text, version) =>
      module.resolveTextRange(range, text, version) as ResolvedTextRange,
  };
}
