import { CoRichText, Marks, TreeLeaf, TreeNode } from "jazz-tools";
import {
  Mark as ProsemirrorMark,
  Node as ProsemirrorNode,
} from "prosemirror-model";
import { schema } from "prosemirror-schema-basic";
import {
  Transaction as ProsemirrorTransaction,
  TextSelection,
} from "prosemirror-state";
import {
  AddMarkStep,
  RemoveMarkStep,
  ReplaceStep,
} from "prosemirror-transform";

// Supported mark types
const MARK_TYPE_LOOKUP = {
  strong: {
    mark: Marks.Strong,
    tag: "strong",
  },
  em: {
    mark: Marks.Em,
    tag: "em",
  },
} as const;

// Supported node types
const NODE_TYPE_LOOKUP = {
  paragraph: {
    mark: Marks.Paragraph,
    tag: "paragraph",
  },
} as const;

export type MarkType = keyof typeof MARK_TYPE_LOOKUP;
export type NodeType = keyof typeof NODE_TYPE_LOOKUP;

/**
 * Converts a CoRichText document to a ProseMirror document node.
 * Currently supports basic inline marks (strong, em) within paragraphs.
 *
 * @param text - The CoRichText document to convert
 * @returns A ProseMirror document node, or undefined if conversion fails
 */
export function richTextToProsemirrorDoc(
  text: CoRichText,
): ProsemirrorNode | undefined {
  if (!text) {
    return;
  }

  const asString = text.toString();

  return schema.node("doc", {}, [
    schema.node(
      "paragraph",
      {},
      asString.length === 0
        ? []
        : text
            .toTree(Object.keys(MARK_TYPE_LOOKUP) as MarkType[])
            .children.map((child) => {
              if (
                child.type === "node" &&
                child.tag in NODE_TYPE_LOOKUP &&
                child.children.length > 0
              ) {
                if (!child.children[0]) {
                  throw new Error("Node children must be non-empty");
                }
                // Nodes are treated differently, passing their children directly
                return collectInlineMarks(asString, child.children[0], []);
              }
              // Marks are collected recursively, leaf nodes are plain text
              return collectInlineMarks(asString, child, []);
            }),
    ),
  ]);
}

/**
 * Recursively collects inline marks from a CoRichText tree node.
 * Handles leaf nodes (plain text) and mark nodes (strong, em).
 *
 * @param fullString - The complete document text
 * @param node - Current tree node being processed
 * @param currentMarks - Accumulated marks from parent nodes
 * @returns A ProseMirror text node with appropriate marks
 */
export function collectInlineMarks(
  fullString: string,
  node: TreeNode | TreeLeaf,
  currentMarks: ProsemirrorMark[],
): ProsemirrorNode {
  if (node.type === "leaf") {
    return schema.text(fullString.slice(node.start, node.end), currentMarks);
  } else {
    if (!(node.tag in MARK_TYPE_LOOKUP)) {
      throw new Error(`Unsupported tag '${node.tag}'`);
    }
    if (!node.children[0]) {
      throw new Error("Node children must be non-empty");
    }
    const schemaMark = schema.mark(node.tag);
    return collectInlineMarks(
      fullString,
      node.children[0],
      currentMarks.concat(schemaMark),
    );
  }
}

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
 * Handles mark addition operations from an AddMarkStep.
 */
function handleAddMark(text: CoRichText, step: AddMarkStep) {
  const markType = step.mark.type.name as MarkType;
  const markInfo = MARK_TYPE_LOOKUP[markType];

  if (markInfo) {
    const { mark, tag } = markInfo;
    text.insertMark(step.from - 1, step.to - 2, mark, {
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
