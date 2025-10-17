// @ts-check

import createMDX from "@next/mdx";
import withToc from "@stefanprobst/rehype-extract-toc";
import { remarkHtmlToJsx } from "./remark-plugins/html-to-jsx.mjs";
import { highlightPlugin } from "./remark-plugins/highlight-plugin.mjs";
import { withSlugAndHeadingsFrameworkVisibility } from "./rehype-plugins/with-slug-and-framework-visibility.mjs";
import { withTocAndFrameworkHeadingsVisibilityExport } from "./rehype-plugins/with-toc-and-framework-visibility-export.mjs";

/** @type {import('next').NextConfig} */
const nextConfig = {
  // Configure `pageExtensions`` to include MDX files
  pageExtensions: ["js", "jsx", "mdx", "ts", "tsx"],
  transpilePackages: ["@garden-co/design-system"],
};

const withMDX = createMDX({
  options: {
    remarkPlugins: [highlightPlugin, remarkHtmlToJsx],
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
    return [
      {
        source: "/docs",
        destination: "/docs/react",
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
