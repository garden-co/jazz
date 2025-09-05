import { defineConfig } from "tsup";

const cfg = {
  splitting: true,
  sourcemap: true,
  clean: true,
  treeshake: false,
  dts: false,
  format: ["esm" as const],
  external: ["jazz-tools"],
};

export default defineConfig([
  {
    ...cfg,
    entry: {
      index: "src/index.ts",
      testing: "src/testing.ts",
    },
    outDir: "dist",
  },
  {
    ...cfg,
    entry: {
      index: "src/browser/index.ts",
    },
    outDir: "dist/browser",
  },
  {
    ...cfg,
    entry: {
      index: "src/media/index.ts",
      "index.browser": "src/media/index.browser.ts",
      "index.native": "src/media/index.native.ts",
      "index.server": "src/media/index.server.ts",
    },
    outDir: "dist/media",
  },
  {
    ...cfg,
    entry: {
      index: "src/expo/index.ts",
      testing: "src/expo/testing.ts",
      crypto: "src/expo/crypto.ts",
    },
    outDir: "dist/expo",
  },
  {
    ...cfg,
    entry: {
      index: "src/inspector/index.tsx",
    },
    outDir: "dist/inspector",
    esbuildOptions: (options) => {
      options.banner = {
        js: '"use client";',
      };
    },
  },
  {
    ...cfg,
    entry: {
      "register-custom-element": "src/inspector/register-custom-element.ts",
    },
    // This is a custom element meant to be used on non-react apps
    noExternal: ["react", "react-dom", "react-dom/client", "react/jsx-runtime"],
    outDir: "dist/inspector",
  },
  {
    ...cfg,
    entry: {
      index: "src/prosemirror/index.ts",
    },
    outDir: "dist/prosemirror",
  },
  {
    ...cfg,
    entry: {
      ssr: "src/react/ssr.ts",
    },
    outDir: "dist/react",
  },
  {
    ...cfg,
    entry: {
      index: "src/react/index.ts",
      testing: "src/react/testing.tsx",
    },
    outDir: "dist/react",
    esbuildOptions: (options) => {
      options.banner = {
        js: '"use client";',
      };
    },
  },
  {
    ...cfg,
    entry: {
      index: "src/react-core/index.ts",
      testing: "src/react-core/testing.tsx",
    },
    outDir: "dist/react-core",
    esbuildOptions: (options) => {
      options.banner = {
        js: '"use client";',
      };
    },
  },
  {
    ...cfg,
    entry: {
      index: "src/react-native/index.ts",
      testing: "src/react-native/testing.ts",
      crypto: "src/react-native/crypto.ts",
    },
    outDir: "dist/react-native",
  },
  {
    ...cfg,
    entry: {
      index: "src/react-native-core/index.ts",
      testing: "src/react-native-core/testing.tsx",
      crypto: "src/react-native-core/crypto/index.ts",
    },
    outDir: "dist/react-native-core",
  },
  {
    ...cfg,
    entry: {
      index: "src/tiptap/index.ts",
    },
    outDir: "dist/tiptap",
  },
  {
    ...cfg,
    entry: {
      index: "src/worker/index.ts",
    },
    outDir: "dist/worker",
  },
  {
    ...cfg,
    entry: {
      client: "src/better-auth/auth/client.ts",
      server: "src/better-auth/auth/server.ts",
      react: "src/better-auth/auth/react.tsx",
    },
    outDir: "dist/better-auth/auth",
  },
]);
