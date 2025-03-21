import createMDX from "@next/mdx";
import withToc from "@stefanprobst/rehype-extract-toc";
import withTocExport from "@stefanprobst/rehype-extract-toc/mdx";
import rehypeSlug from "rehype-slug";
import { createHighlighter } from "shiki";
import { transformerNotationDiff, transformerRemoveLineBreak } from '@shikijs/transformers'
import { transformerTwoslash } from "@shikijs/twoslash";
import { SKIP, visit } from "unist-util-visit";
import { fileURLToPath } from 'url';
import { dirname, resolve as resolvePath } from "path";

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
    rehypePlugins: [rehypeSlug, withToc, withTocExport],
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
  themes: ["min-light", "tokyo-night"],
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const vfsRoot = resolvePath(__dirname, "../..");

console.log(vfsRoot);

function highlightPlugin() {
  return async function transformer(tree) {
    const highlighter = await highlighterPromise;

    visit(tree, "code", visitor);

    function visitor(node) {
      const html = highlighter.codeToHtml(node.value, {
        lang: node.lang,
        meta: { __raw: node.lang + " " + node.meta },
        themes: {
          light: "min-light",
          dark: "tokyo-night",
        },
        transformers: [
          transformerTwoslash({
            explicitTrigger: true,
            twoslashOptions: {
              vfsRoot: vfsRoot,
              compilerOptions: {
                baseUrl: "./packages",
              }
            }
          }),
          transformerNotationDiff(),
          transformerRemoveLineBreak(),
        ],
      });

      node.type = "html";
      node.value = html;
      node.children = [];
      return SKIP;
    }
  };
}

function escape(s) {
  return s.replace(/[^0-9A-Za-z ]/g, (c) => "&#" + c.charCodeAt(0) + ";");
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

export default config;
