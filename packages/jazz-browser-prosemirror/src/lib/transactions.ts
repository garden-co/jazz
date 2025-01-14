import { CoRichText, Marks } from "jazz-tools";
import { Node as ProsemirrorNode } from "prosemirror-model";
import {
  Transaction as ProsemirrorTransaction,
  TextSelection,
} from "prosemirror-state";
import {
  AddMarkStep,
  RemoveMarkStep,
  ReplaceStep,
} from "prosemirror-transform";
import { MARK_TYPE_LOOKUP, MarkType } from "./types.js";

/**
 * Helper function to safely get the document state for a step.
 * Returns undefined if the document state is not found.
 */
function getDocumentStateForStep(
  tr: ProsemirrorTransaction,
  step: ReplaceStep | AddMarkStep | RemoveMarkStep,
) {
  const doc = tr.docs[tr.steps.indexOf(step)];
  if (!doc) {
    console.warn("Document state not found for step", step);
    return undefined;
  }
  return doc;
}

/**
 * Handles paragraph split operations, creating new paragraph marks.
 */
function handleParagraphSplit(text: CoRichText, start: number) {
  const matchingMarks =
    text.marks?.filter(
      (m): m is Exclude<typeof m, null> =>
        !!m &&
        m.tag === "paragraph" &&
        ((m.startAfter && text.idxAfter(m.startAfter)) || 0) < start &&
        ((m.endBefore && text.idxBefore(m.endBefore)) || Infinity) > start,
    ) || [];

  let lastSeenEnd = start;
  for (const matchingMark of matchingMarks) {
    const originalEnd = text.idxAfter(matchingMark.endAfter)!; // TODO: non-tight case
    if (originalEnd > lastSeenEnd) {
      lastSeenEnd = originalEnd;
    }
    matchingMark.endBefore = text.posBefore(start + 1)!;
    matchingMark.endAfter = text.posAfter(start)!;
  }

  text.insertMark(start, lastSeenEnd, Marks.Paragraph, {
    tag: "paragraph",
  });
}

/**
 * Handles text insertion operations from a ReplaceStep.
 * This includes regular insertions and paragraph splits.
 */
function handleTextInsertion(
  text: CoRichText,
  step: ReplaceStep,
  doc: ProsemirrorNode,
  start: number,
) {
  // Handle text insertions at the start of the document
  if (step.from === 0) {
    text.insertAfter(0, step.slice.content.firstChild?.firstChild?.text || "");
  } else if (step.slice.content.firstChild?.text) {
    // ProseMirror positions include nodes and are 1-based
    // We need to convert to our text-only 0-based positions
    const resolvedPos = doc.resolve(step.from);
    const parentOffset = resolvedPos.parentOffset;

    // When no explicit position is provided, ProseMirror sets from=1
    // In this case, we should insert at the current selection
    const insertPos = step.from === 1 ? text.length : parentOffset;
    text.insertAfter(insertPos, step.slice.content.firstChild.text);
  } else {
    // this is a split operation
    const splitNodeType = step.slice.content.firstChild?.type.name;
    if (splitNodeType === "paragraph") {
      handleParagraphSplit(text, start);
    } else {
      console.warn("Unknown node type to split", splitNodeType);
    }
  }
}

/**
 * Handles mark addition operations from an AddMarkStep.
 */
function handleAddMark(text: CoRichText, step: AddMarkStep) {
  const markType = step.mark.type.name as MarkType;
  const markInfo = MARK_TYPE_LOOKUP[markType];

  if (markInfo) {
    const { mark, tag } = markInfo;
    text.insertMark(step.from - 1, step.to - 1, mark, {
      tag,
    });
  } else {
    console.warn("Unsupported mark type", step.mark);
  }
}

/**
 * Handles mark removal operations from a RemoveMarkStep.
 */
function handleRemoveMark(
  text: CoRichText,
  step: RemoveMarkStep,
  tr: ProsemirrorTransaction,
) {
  const markType = step.mark.type.name as MarkType;
  const markInfo = MARK_TYPE_LOOKUP[markType];

  if (markInfo) {
    const { mark } = markInfo;
    // Calculate positions using the document state
    const doc = getDocumentStateForStep(tr, step);
    if (!doc) return;

    const fromPos = doc.resolve(step.from);
    const toPos = doc.resolve(step.to);
    const from = fromPos.parentOffset;
    const to = toPos.parentOffset;
    text.removeMark(from, to, mark, { tag: markType });
  } else {
    console.warn("Unsupported mark type", step.mark);
  }
}

/**
 * Applies ProseMirror transactions to the underlying CoRichText document.
 * Handles text operations (insert, delete) and mark operations (add).
 *
 * @param text - The CoRichText document to modify
 * @param tr - The ProseMirror transaction to apply
 *
 * Supported operations:
 * - ReplaceStep: Text insertions and deletions
 * - AddMarkStep: Adding strong (bold) and em (italic) marks
 * - RemoveMarkStep: Removing strong (bold) and em (italic) marks
 * - Paragraph splits: Creating new paragraph marks when Enter is pressed
 *
 * Prosemirror uses before from and before to for it's mark ranges
 */
export function applyTrToRichText(
  text: CoRichText,
  tr: ProsemirrorTransaction,
) {
  for (const step of tr.steps) {
    if (step instanceof ReplaceStep) {
      const doc = getDocumentStateForStep(tr, step);
      if (!doc) continue;

      const resolvedStart = doc.resolve(step.from);
      const resolvedEnd = doc.resolve(step.to);

      // Calculate text positions by measuring content between start of doc
      // and our target position. This handles nested node structures.
      const selectionToStart = TextSelection.between(
        doc.resolve(0),
        resolvedStart,
      );

      const start = selectionToStart
        .content()
        .content.textBetween(0, selectionToStart.content().content.size).length;

      const selectionToEnd = TextSelection.between(doc.resolve(0), resolvedEnd);
      const end = selectionToEnd
        .content()
        .content.textBetween(0, selectionToEnd.content().content.size).length;

      // Text insertions
      if (start === end) {
        handleTextInsertion(text, step, doc, start);
      } else {
        text.deleteRange({ from: start, to: end });
      }
    } else if (step instanceof AddMarkStep) {
      handleAddMark(text, step);
    } else if (step instanceof RemoveMarkStep) {
      handleRemoveMark(text, step, tr);
    } else {
      console.warn("Unsupported step type", step);
    }
  }
}
