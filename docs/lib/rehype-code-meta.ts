import { parseCodeBlockAttributes } from "fumadocs-core/mdx-plugins";

type HastNode = {
  type: string;
  tagName?: string;
  value?: string;
  properties?: Record<string, unknown>;
  data?: { meta?: string | null };
  children?: HastNode[];
};

function textOf(node: HastNode): string {
  if (node.type === "text") return node.value ?? "";
  return (node.children ?? []).map(textOf).join("");
}

function languageOf(code: HastNode): string {
  const className = code.properties?.className;
  const classes = Array.isArray(className) ? className : [className];
  for (const name of classes) {
    if (typeof name === "string" && name.startsWith("language-")) {
      return name.slice("language-".length);
    }
  }
  return "plaintext";
}

function visit(node: HastNode) {
  for (const child of node.children ?? []) visit(child);
  if (node.type !== "element" || node.tagName !== "pre") return;

  const code = node.children?.find((child) => child.type === "element" && child.tagName === "code");
  if (!code) return;

  const { attributes } = parseCodeBlockAttributes(code.data?.meta ?? "", ["title", "custom"]);
  // Code blocks keep a trailing newline from the fence; the block itself adds none.
  node.properties = {
    ...node.properties,
    code: textOf(code).replace(/\n$/, ""),
    language: languageOf(code),
    ...(typeof attributes.title === "string" ? { title: attributes.title } : {}),
    ...(typeof attributes.custom === "string" ? { custom: attributes.custom } : {}),
  };
}

/**
 * Docs pages render fenced code with Astryx `CodeBlock`, which highlights
 * client-side from a plain string. With Shiki (`rehypeCode`) off for the docs
 * collection, this copies each fence's source, language and meta (`title`,
 * `custom`) onto its `<pre>` so the MDX `pre` component gets them as props.
 */
export function rehypeCodeMeta() {
  return (tree: HastNode) => visit(tree);
}
