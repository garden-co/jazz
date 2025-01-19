import { CoRichText, TreeLeaf, TreeNode } from "jazz-tools";
import {
  Mark as ProsemirrorMark,
  Node as ProsemirrorNode,
} from "prosemirror-model";
import { schema } from "prosemirror-schema-basic";
import { MARK_TYPE_LOOKUP } from "./types.js";

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
        : text.toTree(Object.keys(MARK_TYPE_LOOKUP)).children.map((child) => {
            if (
              child.type === "node" &&
              child.tag === "paragraph" &&
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
