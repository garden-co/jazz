import { CoRichText, CoRichTextDebug, Marks } from "jazz-tools";
import { Node } from "prosemirror-model";
import { Transaction } from "prosemirror-state";
import {
  AddMarkStep,
  RemoveMarkStep,
  ReplaceStep,
} from "prosemirror-transform";
import { debugDoc } from "./utils/debug.js";

/**
 * Handles merging paragraphs after text deletion.
 * @param text The CoRichText instance to modify
 * @param pos The 0-based position where text was deleted
 */
export function handleParagraphMerge(text: CoRichText, pos: number) {
  // First, save all marks
  const existingMarks = text.resolveMarks();
  const otherMarks = existingMarks.filter(
    (m) => m.sourceMark.tag !== "paragraph",
  );

  // Normalize the text content by removing newlines
  const content = text.toString();
  if (content.includes("\n")) {
    const newContent = content.replace(/\n/g, "");

    // Remove all existing marks first
    for (const mark of existingMarks) {
      if (mark.sourceMark.tag === "strong") {
        text.removeMark(mark.startBefore, mark.endBefore, Marks.Strong, {
          tag: "strong",
        });
      } else if (mark.sourceMark.tag === "em") {
        text.removeMark(mark.startBefore, mark.endBefore, Marks.Em, {
          tag: "em",
        });
      } else if (mark.sourceMark.tag === "paragraph") {
        text.removeMark(mark.startBefore, mark.endAfter, Marks.Paragraph, {
          tag: "paragraph",
        });
      }
    }

    // Replace content
    text.deleteRange({ from: 0, to: text.length });
    text.insertBefore(0, newContent);

    // Reapply non-paragraph marks with adjusted positions
    for (const mark of otherMarks) {
      // If the mark was in the first paragraph (before the newline), keep its position
      // If it was in the second paragraph (after the newline), adjust its position by -1 to account for removed newline
      const isSecondParagraph = mark.startBefore > pos;
      const startBefore = isSecondParagraph
        ? mark.startBefore - 1
        : mark.startBefore;
      const endBefore = isSecondParagraph ? mark.endBefore - 1 : mark.endBefore;

      if (mark.sourceMark.tag === "strong") {
        text.insertMark(startBefore - 1, endBefore - 1, Marks.Strong, {
          tag: "strong",
        });
      } else if (mark.sourceMark.tag === "em") {
        text.insertMark(startBefore - 1, endBefore - 1, Marks.Em, {
          tag: "em",
        });
      }
    }

    // Create a single paragraph mark spanning the entire content
    if (text.length > 0) {
      text.insertMark(0, text.length - 1, Marks.Paragraph, {
        tag: "paragraph",
      });
    }
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
  const hasNewline = text.toString().includes("\n");

  // Delete the range
  text.deleteRange({
    from: Math.max(0, from - 1), // Remove 1 to start deletion after the character before
    to,
  });

  // If we had a newline, we need to merge paragraphs
  if (hasNewline) {
    handleParagraphMerge(text, from);
  }
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
  console.log("Starting transaction application");
  console.log("Transaction doc before:", {
    size: tr.before.nodeSize,
    content: tr.before.textContent,
  });

  // Debug the entire document
  debugDoc(tr.doc, "After insertion");

  tr.steps.forEach((step, i) => {
    console.log(`\nProcessing step ${i}:`, {
      type: step.constructor.name,
      step,
    });

    if (step instanceof ReplaceStep) {
      const { from, to } = step;
      console.log("ReplaceStep details:", {
        from,
        to,
        slice: {
          size: step.slice.size,
          content: step.slice.content.textBetween(
            0,
            step.slice.content.size,
            "\n",
            " ",
          ),
        },
      });

      // Convert ProseMirror positions to Jazz positions
      const jazzFrom = pmPosToJazzPos(tr.before, from);
      const jazzTo = pmPosToJazzPos(tr.before, to);
      console.log("Converted positions:", { jazzFrom, jazzTo });

      // Handle deletion if needed
      if (to > from) {
        console.log(
          `Handling deletion: "${text.toString()}" from ${jazzFrom} to ${jazzTo} (length ${text.length})`,
        );
        handleTextDeletion(text, jazzFrom, jazzTo);
      }

      // Get any content to insert
      const content = step.slice.content.textBetween(
        0,
        step.slice.content.size,
        "\n",
        " ",
      );

      // Handle insertion
      if (content) {
        console.log("Handling insertion:", { content });
        handleTextInsertion(text, jazzFrom, content);
      }

      // Handle paragraph split if needed
      if (step.slice?.content?.firstChild?.type.name === "paragraph") {
        console.log("Handling paragraph split");
        handleParagraphSplit(text, jazzFrom - 1);
      }
    } else if (step instanceof AddMarkStep) {
      console.log("AddMarkStep:", {
        from: step.from,
        to: step.to,
        markType: step.mark.type.name,
      });
      const { from, to, mark } = step;
      const jazzFrom = pmPosToJazzPos(tr.before, from);
      const jazzTo = pmPosToJazzPos(tr.before, to);
      handleAddMark(text, jazzFrom, jazzTo, mark.type.name);
    } else if (step instanceof RemoveMarkStep) {
      console.log("RemoveMarkStep:", {
        from: step.from,
        to: step.to,
        markType: step.mark.type.name,
      });
      const { from, to, mark } = step;
      const jazzFrom = pmPosToJazzPos(tr.before, from);
      const jazzTo = pmPosToJazzPos(tr.before, to);
      handleRemoveMark(text, jazzFrom, jazzTo, mark.type.name);
    }
  });

  CoRichTextDebug.log(text, { label: "After transaction" });

  console.log("\nTransaction complete. Final text:", {
    content: text.toString(),
    marks: text.resolveMarks(),
  });
}
