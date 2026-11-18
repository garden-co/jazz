import { replaceVersionTokens } from "./index";

type MdastNode = { type: string; value?: string; url?: string; children?: MdastNode[] };

function walk(node: MdastNode, visit: (node: MdastNode) => void) {
  visit(node);
  for (const child of node.children ?? []) walk(child, visit);
}

/** Replaces `%jazz.version%` in page text, inline code and link URLs. */
export function remarkJazzVersion() {
  return (tree: MdastNode) => {
    walk(tree, (node) => {
      if ((node.type === "text" || node.type === "inlineCode") && node.value?.includes("%jazz."))
        node.value = replaceVersionTokens(node.value);
      if (node.type === "link" && node.url?.includes("%jazz."))
        node.url = replaceVersionTokens(node.url);
    });
  };
}
