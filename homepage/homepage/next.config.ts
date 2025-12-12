// @ts-check

import createMDX from "@next/mdx";
import withToc from "@stefanprobst/rehype-extract-toc";
import { remarkHtmlToJsx } from "./remark-plugins/html-to-jsx.mjs";
import { highlightPlugin } from "./remark-plugins/highlight-plugin.mjs";
import { codeSnippets } from './remark-plugins/code-snippets.ts';
import { withSlugAndHeadingsFrameworkVisibility } from "./rehype-plugins/with-slug-and-framework-visibility.mjs";
import { withTocAndFrameworkHeadingsVisibilityExport } from "./rehype-plugins/with-toc-and-framework-visibility-export.mjs";
import { redirects } from "./content/docs/301redirects.js"; // 301s from nav change made October 2025.

// Keep in sync with content/framework.ts
const frameworks = [
  "react",
  "react-native",
  "react-native-expo",
  "svelte",
  "vanilla",
];

/** @type {import('next').NextConfig} */
const nextConfig = {
  // Configure `pageExtensions`` to include MDX files
  pageExtensions: ["js", "jsx", "mdx", "ts", "tsx"],
  transpilePackages: ["@garden-co/design-system"],
};

const withMDX = createMDX({
  options: {
    remarkPlugins: [[codeSnippets, { dir: "./content/docs/code-snippets" }], highlightPlugin, remarkHtmlToJsx],
    rehypePlugins: [
      withSlugAndHeadingsFrameworkVisibility,
      withToc, // Create table of contents array
      withTocAndFrameworkHeadingsVisibilityExport,
    ],
  },
});

const config = {
  ...withMDX(nextConfig),
  output: "standalone",
  redirects: async () => {
    // Check if the first segment after /docs/ is not a valid framework
    const frameworkPattern = frameworks.map((f) => `${f}(?:/|$)`).join("|");

    return [
      ...redirects(), // 301s from nav change made October 2025.
      {
        source: "/docs",
        destination: "/docs/react",
        permanent: false,
      },
      {
        source: `/docs/:slug((?!${frameworkPattern}).*)`,
        destination: "/docs/react/:slug*",
        permanent: false,
      },
      {
        source: "/cloud",
        destination: "/",
        permanent: true,
      },
    ];
  },
};

export default config;
