// Resolve the app schema's `import { schema } from "jazz-tools"` to the DSL
// module alone, as a dedicated `jazz-tools/schema` entry point would. Every
// other import of jazz-tools (e.g. the lazy full-Jazz loader) is untouched.
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dsl = path.join(root, "node_modules/jazz-tools/dist/schema-namespace.js");

export function dslEntryPlugin() {
  return {
    name: "jazz-lite-dsl-entry",
    enforce: "pre",
    resolveId(source, importer) {
      if (source === "jazz-tools" && importer && importer.endsWith(path.join("app", "schema.ts")))
        return dsl;
      return null;
    },
  };
}
