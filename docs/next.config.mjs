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
  async redirects() {
    // The examples & benchmarks page subsumes the old perf timeline. Old
    // ?benchmark=… links land on the page; the query is ignored.
    // Pages moved by the server-first docs restructure keep their old URLs
    // working. Temporary until the new structure settles.
    return [
      { source: "/perf-timeline", destination: "/examples", permanent: false },
      {
        source: "/docs/tooling/agent-skills",
        destination: "/docs/setup/run-locally",
        permanent: false,
      },
      { source: "/docs/faq", destination: "/docs/how-jazz-works#faq", permanent: false },
      {
        source: "/docs/setup/deployment/jazz-cloud",
        destination: "/docs/setup/deployment",
        permanent: false,
      },
      {
        source: "/docs/auth/authentication",
        destination: "/docs/setup/auth/authentication",
        permanent: false,
      },
      {
        source: "/docs/auth/lifecycle",
        destination: "/docs/setup/auth/lifecycle",
        permanent: false,
      },
      {
        source: "/docs/auth/local-first-auth",
        destination: "/docs/setup/web-client/local-first-auth",
        permanent: false,
      },
      { source: "/docs/auth/permissions", destination: "/docs/schemas/policies", permanent: false },
      { source: "/docs/auth/sessions", destination: "/docs/setup/auth/sessions", permanent: false },
      {
        source: "/docs/concepts/branches",
        destination: "/docs/recipes/drafts-and-branches",
        permanent: false,
      },
      {
        source: "/docs/concepts/how-sync-works",
        destination: "/docs/recipes/local-first/how-sync-works",
        permanent: false,
      },
      {
        source: "/docs/concepts/local-first-data-model",
        destination: "/docs/recipes/local-first/local-first-data-model",
        permanent: false,
      },
      {
        source: "/docs/getting-started/client-setup",
        destination: "/docs/setup/web-client/local-first-setup",
        permanent: false,
      },
      {
        source: "/docs/getting-started/server-setup",
        destination: "/docs/setup/backend/trusted",
        permanent: false,
      },
      {
        source: "/docs/install/client",
        destination: "/docs/setup/web-client/local-first-install",
        permanent: false,
      },
      {
        source: "/docs/install/typescript-server",
        destination: "/docs/setup/backend/install",
        permanent: false,
      },
      { source: "/docs/quickstart", destination: "/docs/setup/run-locally", permanent: false },
      { source: "/docs/recipes/testing", destination: "/docs/tooling/testing", permanent: false },
      {
        source: "/docs/reference/agent-skills",
        destination: "/docs/setup/run-locally",
        permanent: false,
      },
      {
        source: "/docs/reference/inspector",
        destination: "/docs/tooling/inspector",
        permanent: false,
      },
    ];
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
