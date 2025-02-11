import { CoRichText, TreeLeaf, TreeNode } from "jazz-tools";
import {
  Mark as ProsemirrorMark,
  Node as ProsemirrorNode,
} from "prosemirror-model";
import { schema } from "prosemirror-schema-basic";
import { MARK_TYPE_LOOKUP, NODE_TYPE_LOOKUP } from "./types.js";

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
    return schema.text(
      fullString.slice(node.start, node.end + 1), // +1 to include the end character as slice is end-exclusive and end is inclusive
      currentMarks,
    );
  } else {
    if (!node.children[0]) {
      throw new Error("Node children must be non-empty");
    }

    // Skip paragraph nodes in mark collection since they're handled at a higher level
    if (node.tag === "paragraph") {
      return collectInlineMarks(fullString, node.children[0], currentMarks);
    }

    if (!(node.tag in MARK_TYPE_LOOKUP)) {
      throw new Error(`Unsupported tag '${node.tag}'`);
    }

    const schemaMark = schema.mark(node.tag);
    const newMarks = currentMarks.concat(schemaMark);

    // If this node has multiple children, combine their text since they share the same marks
    if (node.children.length > 1) {
      const text = node.children
        .map((child) => fullString.slice(child.start, child.end + 1)) // +1 to include the end character as slice is end-exclusive and end is inclusive
        .join("");
      return schema.text(text, newMarks);
    }

    return collectInlineMarks(fullString, node.children[0], newMarks);
  }
}

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

  // Include both mark and node types in precedence list, with paragraph first
  const tagPrecedence = [
    ...Object.keys(NODE_TYPE_LOOKUP),
    ...Object.keys(MARK_TYPE_LOOKUP),
  ];

  const tree = text.toTree(tagPrecedence);

  // Process each child of the root node
  const content =
    asString.length === 0
      ? []
      : tree.children.map((child) => {
          if (child.type === "node" && child.tag === "paragraph") {
            // For paragraph nodes, process their children
            if (!child.children[0]) {
              throw new Error("Node children must be non-empty");
            }
            return collectInlineMarks(asString, child.children[0], []);
          }
          // For other nodes (marks), collect them recursively
          return collectInlineMarks(asString, child, []);
        });

  // Create the document with a single paragraph containing all content
  return schema.node("doc", {}, [schema.node("paragraph", {}, content)]);
}
