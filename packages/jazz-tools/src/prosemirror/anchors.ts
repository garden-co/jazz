/**
 * Anchors and ranges for an editor's positions, in the markdown value's
 * terms, so that other clients (and other editors) can find them in their
 * own version of the value. `anchors` is `db.textAnchors()`.
 */
import type { EditorState } from "prosemirror-state";
import type { TextAnchor, TextAnchors, TextRange } from "../runtime/text-anchors.js";
import type { Side } from "./markdown.js";
import { docPosition, markdownOf, textPosition } from "./sync.js";

/** The anchor for document position `pos`, such as a cursor. */
export function captureAnchor(
  state: EditorState,
  anchors: TextAnchors,
  pos: number,
  side: Side = "after",
  version?: number,
): TextAnchor {
  return anchors.captureAnchor(
    markdownOf(state).text,
    textPosition(state, pos, side),
    side,
    version,
  );
}

/** Where `anchor` lands in the editor's document. */
export function resolveAnchor(
  state: EditorState,
  anchors: TextAnchors,
  anchor: TextAnchor,
  version?: number,
): { pos: number; confidence: number; ambiguous: boolean } {
  const found = anchors.resolveAnchor(anchor, markdownOf(state).text, version);
  return { ...found, pos: docPosition(state, found.position, anchor.side) };
}

/** The range for `from..to`, such as a selection or a comment. */
export function captureRange(
  state: EditorState,
  anchors: TextAnchors,
  from: number,
  to: number,
  version?: number,
): TextRange {
  const start = textPosition(state, Math.min(from, to), "after");
  const end = Math.max(start, textPosition(state, Math.max(from, to), "before"));
  return anchors.captureRange(markdownOf(state).text, start, end, version);
}

/** Where `range` lands in the editor's document. */
export function resolveRange(
  state: EditorState,
  anchors: TextAnchors,
  range: TextRange,
  version?: number,
): { from: number; to: number; confidence: number; ambiguous: boolean } {
  const found = anchors.resolveRange(range, markdownOf(state).text, version);
  const from = docPosition(state, found.start, "after");
  return {
    from,
    to: Math.max(from, docPosition(state, found.end, "before")),
    confidence: found.confidence,
    ambiguous: found.ambiguous,
  };
}
