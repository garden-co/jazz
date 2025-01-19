import { CoRichText, Marks } from "jazz-tools";
import { Node } from "prosemirror-model";
import { Transaction } from "prosemirror-state";
import {
  AddMarkStep,
  RemoveMarkStep,
  ReplaceStep,
} from "prosemirror-transform";

/**
 * Handles merging paragraphs after text deletion.
 * @param text The CoRichText instance to modify
 * @param pos The 0-based position where text was deleted (Jazz coordinates)
 */
export function handleParagraphMerge(text: CoRichText, pos: number) {
  // Remove all existing paragraph marks first to prevent duplicates
  const existingMarks = text
    .resolveMarks()
    .filter((m) => m.sourceMark.tag === "paragraph");

  for (const mark of existingMarks) {
    text.removeMark(mark.startBefore, mark.endAfter, Marks.Paragraph, {
      tag: "paragraph",
    });
  }

  // Create a single paragraph mark spanning the entire content
  if (text.length > 0) {
    text.insertMark(0, text.length - 1, Marks.Paragraph, { tag: "paragraph" });
  }
}

/**
 * Handles splitting a paragraph at a specific position.
 * @param text The CoRichText instance to modify
 * @param pos The position to split at (Jazz coordinates)
 */
export function handleParagraphSplit(text: CoRichText, pos: number) {
  // Remove all existing paragraph marks first to prevent duplicates
  const existingMarks = text
    .resolveMarks()
    .filter((m) => m.sourceMark.tag === "paragraph");

  for (const mark of existingMarks) {
    text.removeMark(mark.startBefore, mark.endAfter, Marks.Paragraph, {
      tag: "paragraph",
    });
  }

  // Create two new paragraph marks
  // First paragraph: from start to split position
  if (pos > 0) {
    text.insertMark(0, pos - 1, Marks.Paragraph, { tag: "paragraph" });
  }

  // Second paragraph: from split position to end
  if (pos < text.length) {
    text.insertMark(pos, text.length - 1, Marks.Paragraph, {
      tag: "paragraph",
    });
  }
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
    text.insertAfter(pos - 1, content);
  }

  // If the content contains newlines, we need to split paragraphs
  if (content.includes("\n")) {
    handleParagraphSplit(text, pos);
  } else {
    // Otherwise just ensure the content is within a paragraph
    const paragraphMarks = text
      .resolveMarks()
      .filter((m) => m.sourceMark.tag === "paragraph");

    if (paragraphMarks.length === 0 && text.length > 0) {
      text.insertMark(0, text.length - 1, Marks.Paragraph, {
        tag: "paragraph",
      });
    }
  }
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
  // Get the text up to this position, preserving newlines
  return doc.textBetween(0, pos + 1, "\n", " ").length;
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
      // Convert ProseMirror positions to Jazz positions
      const jazzFrom = pmPosToJazzPos(tr.before, from);
      const jazzTo = pmPosToJazzPos(tr.before, to);

      // Handle deletion if needed
      if (to > from) {
        handleTextDeletion(text, jazzFrom, jazzTo);
      }

      // Get any content to insert
      const content = step.slice.content.textBetween(
        0,
        step.slice.content.size,
        "\n", // Use newline as block separator
        " ", // Use space as leaf node separator
      );

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
