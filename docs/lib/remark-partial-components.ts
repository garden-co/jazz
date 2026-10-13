type EsTreeNode = {
  type: string;
  source?: { value?: unknown };
  specifiers?: { local: { name: string } }[];
};

type MdastNode = {
  type: string;
  name?: string | null;
  attributes?: { type: string; name?: string; value?: unknown }[];
  data?: { estree?: { body?: EsTreeNode[] } };
  children?: MdastNode[];
};

function walk(node: MdastNode, visit: (node: MdastNode) => void) {
  visit(node);
  for (const child of node.children ?? []) walk(child, visit);
}

/** `props.components` as the ESTree program MDX expects on an attribute. */
function propsComponentsEstree() {
  return {
    type: "Program",
    sourceType: "module",
    body: [
      {
        type: "ExpressionStatement",
        expression: {
          type: "MemberExpression",
          object: { type: "Identifier", name: "props" },
          property: { type: "Identifier", name: "components" },
          computed: false,
          optional: false,
        },
      },
    ],
  };
}

/**
 * Passes the page's MDX components into imported `.mdx` partials. A partial
 * renders with its own (empty) component map unless it is handed one, so its
 * tables and code blocks would otherwise skip the docs' Astryx components.
 * Rewrites `<Partial />` to `<Partial components={props.components} />` at
 * compile time, leaving the source (and the Markdown export) unchanged.
 */
export function remarkPartialComponents() {
  return (tree: MdastNode) => {
    const partials = new Set<string>();
    walk(tree, (node) => {
      if (node.type !== "mdxjsEsm") return;
      for (const statement of node.data?.estree?.body ?? []) {
        const source = statement.source?.value;
        if (statement.type !== "ImportDeclaration" || typeof source !== "string") continue;
        if (!source.endsWith(".mdx")) continue;
        for (const specifier of statement.specifiers ?? []) partials.add(specifier.local.name);
      }
    });
    if (partials.size === 0) return;

    walk(tree, (node) => {
      if (node.type !== "mdxJsxFlowElement" && node.type !== "mdxJsxTextElement") return;
      if (!node.name || !partials.has(node.name)) return;
      const attributes = (node.attributes ??= []);
      if (attributes.some((a) => a.type === "mdxJsxAttribute" && a.name === "components")) return;
      attributes.push({
        type: "mdxJsxAttribute",
        name: "components",
        value: {
          type: "mdxJsxAttributeValueExpression",
          value: "props.components",
          data: { estree: propsComponentsEstree() },
        },
      });
    });
  };
}
