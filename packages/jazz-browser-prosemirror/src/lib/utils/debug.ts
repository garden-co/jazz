import { Mark, Node } from "prosemirror-model";
import { Transaction } from "prosemirror-state";

/**
 * Creates a visual tree representation of a ProseMirror document with position indexes
 * @param node The ProseMirror node to visualize
 * @param pos Current position in the document
 * @param depth Current indentation depth
 * @param isLast Whether this is the last child of its parent
 * @returns A tuple of [visualization string, next position]
 */
function visualizeDocWithPos(
  node: Node,
  pos: number = 0,
  depth: number = 0,
  isLast: boolean = true,
): [string, number] {
  const indent = "  ".repeat(depth);
  const prefix = depth === 0 ? "" : isLast ? "└─ " : "├─ ";

  // Calculate positions
  const startPos = pos;
  const endPos = pos + node.nodeSize;

  // Build the node representation
  let result = `${indent}${prefix}${node.type.name} (${startPos}:${endPos})`;

  // Add marks if present
  if (node.marks.length > 0) {
    const markStr = node.marks.map((mark: Mark) => mark.type.name).join(", ");
    result += ` [${markStr}]`;
  }

  // Add text content if it's a text node
  if (node.isText) {
    // Replace newlines with visible symbol
    const visibleText = (node.text || "").replace(/\n/g, "↵");
    result += `: "${visibleText}"`;
  }

  result += "\n";

  // Recursively process child nodes
  let currentPos = pos + 1; // Add 1 to account for the opening token
  const children = node.content.content;

  children.forEach((child: Node, index: number) => {
    const [childStr, nextPos] = visualizeDocWithPos(
      child,
      currentPos,
      depth + 1,
      index === children.length - 1,
    );
    result += childStr;
    currentPos = nextPos;
  });

  return [result, endPos];
}

/**
 * Creates a visual tree representation of a ProseMirror document with positions
 * @param node The ProseMirror node to visualize
 * @returns A string representation of the document tree
 */
export function visualizeDoc(node: Node): string {
  return visualizeDocWithPos(node)[0];
}

/**
 * Logs a visual representation of a ProseMirror document to the console
 * @param doc The ProseMirror document to visualize
 * @param label Optional label for the visualization
 */
export function debugDoc(doc: Node, label?: string): void {
  console.group(`Document Structure${label ? ` (${label})` : ""}`);
  console.log(visualizeDoc(doc));
  console.groupEnd();
}

/**
 * Creates a compact debug string for a position in the document
 * @param doc The ProseMirror document
 * @param pos The position to debug
 * @returns A string describing the position context
 */
export function debugPosition(doc: Node, pos: number): string {
  const resolvedPos = doc.resolve(pos);
  const parentNode = resolvedPos.parent;
  const parentOffset = resolvedPos.parentOffset;

  let context = "";
  if (parentNode.isText) {
    const text = parentNode.text || "";
    const start = Math.max(0, parentOffset - 10);
    const end = Math.min(text.length, parentOffset + 10);
    context = text.slice(start, end);
    if (start > 0) context = "..." + context;
    if (end < text.length) context = context + "...";
    context = `"${context}"`;
    const pointer = " ".repeat(start > 0 ? 13 : 10) + "^";
    context += `\n${pointer}`;
  }

  return `Position ${pos}: ${resolvedPos.depth} levels deep, offset ${parentOffset} in ${parentNode.type.name}\n${context}`;
}

const colors = {
  add: "#4caf50",
  remove: "#f44336",
  structure: "#2196f3",
  meta: "#9c27b0",
};

export function debugTransaction(tr: Transaction) {
  // Skip if no steps
  if (!tr.steps.length && !tr.docChanged) {
    console.log("%c No changes in transaction", "color: gray");
    return;
  }

  console.group("📝 Transaction");

  // Log document changes
  if (tr.docChanged) {
    console.log(
      "%c Document changed",
      `color: ${colors.structure}; font-weight: bold`,
    );
    console.group("Changes");

    // Show before/after
    console.log("Before:", formatDoc(tr.before));
    console.log("After:", formatDoc(tr.doc));

    // Log each step
    tr.steps.forEach((step, i) => {
      console.group(`Step ${i + 1}`);
      console.log("Type:", step.toJSON().stepType);
      console.log("JSON:", step.toJSON());
      console.groupEnd();
    });

    console.groupEnd();
  }

  // Log selection changes
  if (tr.selectionSet) {
    console.log("%c Selection changed", "color: #ff9800; font-weight: bold");
    // console.log("From:", tr.before.selection.toJSON());
    console.log("To:", tr.selection.toJSON());
  }

  // Log stored marks changes
  if (tr.storedMarksSet) {
    console.log("%c Stored marks changed", "color: #9c27b0; font-weight: bold");
    console.log("Marks:", tr.storedMarks);
  }

  console.groupEnd();
}

// Helper to format document nodes
function formatDoc(doc: Node): string {
  return JSON.stringify(doc.toJSON(), null, 2);
}

// Usage example:
/*
import { debugTransaction } from './utils/debug'

// In your plugin:
new Plugin({
  view(editorView) {
    return {
      update: (view, prevState) => {
        const tr = view.state.tr
        if (tr.docChanged) {
          debugTransaction(tr)
        }
      }
    }
  }
})
*/
