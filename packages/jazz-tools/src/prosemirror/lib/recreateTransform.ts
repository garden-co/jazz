import type { Node } from "prosemirror-model";
import { ReplaceStep, Transform } from "prosemirror-transform";

/**
 * Given two ProseMirror documents, produce a Transform containing the steps
 * needed to convert fromDoc into toDoc.
 *
 * Uses ProseMirror's built-in Fragment diffing to locate the changed region
 * and creates a ReplaceStep to reconcile it.
 */
export function recreateTransform(fromDoc: Node, toDoc: Node): Transform {
  const tr = new Transform(fromDoc);

  if (fromDoc.eq(toDoc)) {
    return tr;
  }

  let start = toDoc.content.findDiffStart(fromDoc.content);
  if (start === null) {
    return tr;
  }

  const diffEnd = toDoc.content.findDiffEnd(fromDoc.content);
  if (diffEnd === null) {
    return tr;
  }

  let { a: endA, b: endB } = diffEnd;

  // When start overshoots the end positions, we have overlapping boundaries.
  // Resolve by choosing the side with the shallowest depth.
  const overlap = start - Math.min(endA, endB);
  if (overlap > 0) {
    if (
      fromDoc.resolve(start - overlap).depth <
      toDoc.resolve(endA + overlap).depth
    ) {
      start -= overlap;
    } else {
      endA += overlap;
      endB += overlap;
    }
  }

  tr.step(new ReplaceStep(start, endB, toDoc.slice(start, endA)));

  return tr;
}
