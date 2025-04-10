import createMDX from "@next/mdx";
import { transformerNotationDiff } from "@shikijs/transformers";
import { transformerTwoslash } from "@shikijs/twoslash";
import withToc from "@stefanprobst/rehype-extract-toc";
import { valueToEstree } from "estree-util-value-to-estree";
import GithubSlugger from "github-slugger";
import { headingRank } from "hast-util-heading-rank";
import { toString } from "hast-util-to-string";
import { createHighlighter } from "shiki";
import { SKIP, visit } from "unist-util-visit";
import { jazzDark } from "./themes/jazzDark.mjs";
import { jazzLight } from "./themes/jazzLight.mjs";

/** @type {import('next').NextConfig} */
const nextConfig = {
  // Configure `pageExtensions`` to include MDX files
  pageExtensions: ["js", "jsx", "mdx", "ts", "tsx"],
  transpilePackages: ["gcmp-design-system"],
};

const withMDX = createMDX({
  // Add markdown plugins here, as desired
  options: {
    remarkPlugins: [highlightPlugin, remarkHtmlToJsx],
    rehypePlugins: [
      // Add id to heading elements, and indicate which frameworks to show the heading for
      // This is a modified version of rehype-slug
      withSlugAndHeadingsFrameworkVisibility,

      // Create table of contents array
      withToc,

      // Return the table of contents and framework visibility data when importing a .mdx file
      // This is a modified version of withTocExport from @stefanprobst/rehype-extract-toc
      withTocAndFrameworkHeadingsVisibilityExport,
    ],
  },
});

const config = {
  ...withMDX(nextConfig),
  output: "standalone",
  redirects: async () => {
    return [
      {
        source: "/docs",
        destination: "/docs/react",
        permanent: false,
      },
    ];
  },
};

const highlighterPromise = createHighlighter({
  langs: ["typescript", "bash", "tsx", "json", "svelte", "vue"],
  themes: [jazzLight, jazzDark],
});

function highlightPlugin() {
  return async function transformer(tree) {
    const highlighter = await highlighterPromise;

    visit(tree, "code", visitor);

    function visitor(node) {
      let error = "";
      const html = highlighter.codeToHtml(node.value, {
        lang: node.lang,
        meta: { __raw: node.lang + " " + node.meta },
        themes: {
          light: "jazz-light",
          dark: "jazz-dark",
        },

        transformers: [
          transformerTwoslash({
            explicitTrigger: true,
            throws: process.env.NODE_ENV === "production",
            onTwoslashError:
              process.env.NODE_ENV !== "production"
                ? (e) => {
                    console.error(e);
                    error = e;
                  }
                : undefined,
          }),
          transformerNotationDiff(),
        ],
      });

      node.type = "html";
      node.value = error
        ? `<div style="color: red;">${error}</div>` + html
        : html;
      node.children = [];
      return SKIP;
    }
  };
}

function remarkHtmlToJsx() {
  async function transform(...args) {
    // Async import since these packages are all in ESM
    const { visit, SKIP } = await import("unist-util-visit");
    const { mdxFromMarkdown } = await import("mdast-util-mdx");
    const { fromMarkdown } = await import("mdast-util-from-markdown");
    const { mdxjs } = await import("micromark-extension-mdxjs");

    // This is a horror show, but it's the only way I could get the raw HTML into MDX.
    const [ast] = args;
    visit(ast, "html", (node) => {
      const escapedHtml = JSON.stringify(node.value);
      const jsx = `<CodeWithInterpolation highlightedCode={${escapedHtml}}/>`;
      const rawHtmlNode = fromMarkdown(jsx, {
        extensions: [mdxjs()],
        mdastExtensions: [mdxFromMarkdown()],
      }).children[0];

      Object.assign(node, rawHtmlNode);

      return SKIP;
    });
  }

  return transform;
}

const slugs = new GithubSlugger();

export function withSlugAndHeadingsFrameworkVisibility() {
  return function (tree, vfile) {
    slugs.reset();
    vfile.data.headingsFrameworkVisibility = {};

    visit(tree, "element", function (node) {
      if (headingRank(node) && !node.properties.id) {
        const lastChild = node.children?.[node.children.length - 1];
        if (!lastChild || lastChild.type !== "text") return;

        const match = lastChild.value.match(
          /\s*\[\!framework=([a-zA-Z0-9,_-]+)\]\s*$/,
        );
        if (match) {
          const frameworks = match[1];

          lastChild.value = lastChild.value.replace(
            /\s*\[\!framework=[a-zA-Z0-9,_-]+\]\s*$/,
            "",
          );

          node.properties.id = slugs.slug(lastChild.value);
          vfile.data.headingsFrameworkVisibility[node.properties.id] =
            frameworks.split(",");
        } else {
          node.properties.id = slugs.slug(toString(node));
        }
      }
    });
  };
}

export function withTocAndFrameworkHeadingsVisibilityExport() {
  return function transformer(tree, vfile) {
    if (vfile.data.toc == null) return;

    tree.children.unshift({
      type: "mdxjsEsm",
      data: {
        estree: {
          type: "Program",
          sourceType: "module",
          body: [
            {
              type: "ExportNamedDeclaration",
              source: null,
              specifiers: [],
              declaration: {
                type: "VariableDeclaration",
                kind: "const",
                declarations: [
                  {
                    type: "VariableDeclarator",
                    id: {
                      type: "Identifier",
                      name: "headingsFrameworkVisibility",
                    },
                    init: valueToEstree(vfile.data.headingsFrameworkVisibility),
                  },
                  {
                    type: "VariableDeclarator",
                    id: { type: "Identifier", name: "tableOfContents" },
                    init: valueToEstree(vfile.data.toc),
                  },
                ],
              },
            },
          ],
        },
      },
    });
  };
}

export default config;
