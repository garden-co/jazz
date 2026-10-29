import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { expect, it } from "vitest";

it("tree-shakes unused transaction classes from a schema-only public import", async () => {
  const result = await build({
    stdin: {
      contents: `
        import { table, schema, getCollectedSchema } from "jazz-tools";
        table("tasks", { title: schema.string() });
        export const collected = getCollectedSchema();
      `,
      resolveDir: fileURLToPath(new URL(".", import.meta.url)),
    },
    bundle: true,
    format: "esm",
    platform: "node",
    write: false,
    // This checks TypeScript tree shaking, without building or loading native bindings.
    packages: "external",
    external: ["./native-artifact-fingerprint-napi.js", "./native-artifact-fingerprint-wasm.js"],
    plugins: [
      {
        name: "local-jazz-tools",
        setup(build) {
          build.onResolve({ filter: /^jazz-tools$/ }, () => ({
            path: fileURLToPath(new URL("./index.ts", import.meta.url)),
          }));
        },
      },
    ],
  });

  // Computed Symbol method keys keep these entire classes alive even when the
  // consumer only defines a schema. Repeated CLI schema imports then pay for
  // transforming the otherwise unused transaction implementation and dependencies.
  const retainedClasses = result.outputFiles[0]!.text.matchAll(
    /class\s+_?(Db|Transaction|JazzClient)\d*\b/g,
  );
  expect(Array.from(retainedClasses, (match) => match[1])).toEqual([]);
});
