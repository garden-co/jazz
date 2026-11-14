import { loadBundleSizes, replaceBundleSizeTokens } from "./index";

type MdastNode = { type: string; value?: string; children?: MdastNode[] };

function walk(node: MdastNode, visit: (node: MdastNode) => void) {
  visit(node);
  for (const child of node.children ?? []) walk(child, visit);
}

/**
 * Replaces `%bundle.thin%`, `%bundle.wasm%` and `%bundle.wasmRaw%` in page text
 * with the measured sizes, so pages, partials and the Markdown export for
 * agents all show the same rounded numbers.
 */
export function remarkBundleSizes() {
  return async (tree: MdastNode) => {
    let found = false;
    walk(tree, (node) => {
      if (node.type === "text" && node.value?.includes("%bundle.")) found = true;
    });
    if (!found) return;
    const sizes = await loadBundleSizes();
    walk(tree, (node) => {
      if (node.type === "text" && node.value?.includes("%bundle."))
        node.value = replaceBundleSizeTokens(node.value, sizes);
    });
  };
}
