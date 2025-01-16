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
 * Handles text insertion in the document.
 * @param text The CoRichText instance to modify
 * @param pos The position to insert at (Jazz coordinates)
 * @param content The text content to insert
 */
export function handleTextInsertion(
  text: CoRichText,
  pos: number,
  content: string,
) {
  // For position 0, insert before. For all other positions, insert after the previous position
  if (pos === 0) {
    text.insertBefore(0, content);
  } else if (pos === text.length) {
    text.insertAfter(text.length - 1, content);
  } else {
    text.insertAfter(pos, content);
  }

  // Handle paragraph marks after insertion
  handleParagraphMerge(text, pos);
}

/**
 * Handles text deletion in the document.
 * @param text The CoRichText instance to modify
 * @param from The start position (Jazz coordinates)
 * @param to The end position (Jazz coordinates)
 */
export function handleTextDeletion(text: CoRichText, from: number, to: number) {
  // Delete the range
  text.deleteRange({
    from: Math.max(0, from),
    to,
  });

  // Handle paragraph merging after deletion
  handleParagraphMerge(text, from);
}

/**
 * Handles adding a mark to a range of text.
 * @param text The CoRichText instance to modify
 * @param from The start position (Jazz coordinates)
 * @param to The end position (Jazz coordinates)
 * @param markType The type of mark to add
 */
function handleAddMark(
  text: CoRichText,
  from: number,
  to: number,
  markType: string,
) {
  if (markType === "strong") {
    text.insertMark(from, to, Marks.Strong, { tag: "strong" });
  } else if (markType === "em") {
    text.insertMark(from, to, Marks.Em, { tag: "em" });
  } else if (markType === "paragraph") {
    text.insertMark(from, to, Marks.Paragraph, {
      tag: "paragraph",
    });
  } else {
    console.warn("Unsupported mark type", markType);
  }
}

/**
 * Handles removing a mark from a range of text.
 * @param text The CoRichText instance to modify
 * @param from The start position (Jazz coordinates)
 * @param to The end position (Jazz coordinates)
 * @param markType The type of mark to remove
 */
function handleRemoveMark(
  text: CoRichText,
  from: number,
  to: number,
  markType: string,
) {
  if (markType === "strong") {
    text.removeMark(from, to, Marks.Strong, { tag: "strong" });
  } else if (markType === "em") {
    text.removeMark(from, to, Marks.Em, { tag: "em" });
  } else if (markType === "paragraph") {
    text.removeMark(from, to, Marks.Paragraph, {
      tag: "paragraph",
    });
  }
}

/**
 * Converts a ProseMirror position to a Jazz position.
 * @param doc The ProseMirror document
 * @param pos The ProseMirror position
 * @returns The Jazz position (0-based)
 */
function pmPosToJazzPos(doc: Node, pos: number): number {
  return doc.textBetween(0, pos, "\n", " ").length;
}

/**
 * Applies a ProseMirror transaction to a CoRichText instance.
 * Handles text insertion, deletion, and mark changes.
 * @param text The CoRichText instance to modify
 * @param tr The ProseMirror transaction to apply
 */
export function applyTrToRichText(text: CoRichText, tr: Transaction) {
  tr.steps.forEach((step) => {
    if (step instanceof ReplaceStep) {
      const { from, to } = step;
      const content = step.slice.content.textBetween(
        0,
        step.slice.content.size,
        "\n", // Use newline as block separator
        " ", // Use space as leaf node separator
      );

      // Convert ProseMirror positions to Jazz positions
      const jazzFrom = pmPosToJazzPos(tr.before, from);
      const jazzTo = pmPosToJazzPos(tr.before, to);

      // Handle deletion if needed
      if (to > from) {
        handleTextDeletion(text, jazzFrom, jazzTo);
      }

      // Handle insertion
      if (content) {
        handleTextInsertion(text, jazzFrom, content);
      }

      // Handle paragraph split if needed
      if (step.slice?.content?.firstChild?.type.name === "paragraph") {
        handleParagraphSplit(text, jazzFrom - 1);
      }
    } else if (step instanceof AddMarkStep) {
      const { from, to, mark } = step;
      // Convert ProseMirror positions to Jazz positions
      const jazzFrom = pmPosToJazzPos(tr.before, from);
      const jazzTo = pmPosToJazzPos(tr.before, to);
      handleAddMark(text, jazzFrom, jazzTo, mark.type.name);
    } else if (step instanceof RemoveMarkStep) {
      const { from, to, mark } = step;
      // Convert ProseMirror positions to Jazz positions
      const jazzFrom = pmPosToJazzPos(tr.before, from);
      const jazzTo = pmPosToJazzPos(tr.before, to);
      handleRemoveMark(text, jazzFrom, jazzTo, mark.type.name);
    }
  });
}
