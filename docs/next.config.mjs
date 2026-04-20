import path from "node:path";
import { fileURLToPath } from "node:url";

const defaultPageExtensions = ["mdx", "md", "jsx", "js", "tsx", "ts"];
const isDev = process.env.NODE_ENV === "development";
const docsDir = path.dirname(fileURLToPath(import.meta.url));
const loaderOptions = {
  configPath: "source.config.ts",
  outDir: ".source",
  absoluteCompiledConfigPath: path.join(docsDir, ".source/source.config.mjs"),
  isDev,
};

/** @type {import('next').NextConfig} */
const config = {
  reactStrictMode: true,
  pageExtensions: defaultPageExtensions,
  turbopack: {
    rules: {
      "*.{md,mdx}": {
        loaders: [
          {
            loader: "fumadocs-mdx/loader-mdx",
            options: loaderOptions,
          },
        ],
        as: "*.js",
      },
      "*.json": {
        loaders: [
          {
            loader: "fumadocs-mdx/loader-meta",
            options: loaderOptions,
          },
        ],
        as: "*.json",
      },
      "*.yaml": {
        loaders: [
          {
            loader: "fumadocs-mdx/loader-meta",
            options: loaderOptions,
          },
        ],
        as: "*.js",
      },
    },
  },
  webpack: (webpackConfig, options) => {
    webpackConfig.resolve ||= {};
    webpackConfig.module ||= {};
    webpackConfig.module.rules ||= [];
    webpackConfig.module.rules.push(
      {
        test: /\.mdx?(\?.+?)?$/,
        use: [
          options.defaultLoaders.babel,
          {
            loader: "fumadocs-mdx/loader-mdx",
            options: loaderOptions,
          },
        ],
      },
      {
        test: /\.(json|yaml)(\?.+?)?$/,
        enforce: "pre",
        use: [
          {
            loader: "fumadocs-mdx/loader-meta",
            options: loaderOptions,
          },
        ],
      },
    );

    return webpackConfig;
  },
  async rewrites() {
    return [
      {
        source: "/docs/:path*.mdx",
        destination: "/llms.mdx/docs/:path*",
      },
    ];
  },
};

export default config;
