// Bundle-size profiling config: the app's own vite config plus a treemap,
// raw per-module sizes, and the module graph (static vs dynamic imports).
import base from "./vite.config.ts";
import { visualizer } from "rollup-plugin-visualizer";
import { writeFileSync } from "node:fs";
const out = process.env.VIZ_OUT ?? "bundle";
const graph = {
  name: "dump-module-graph",
  generateBundle() {
    const g: Record<string, unknown> = {};
    for (const id of this.getModuleIds()) {
      const m = this.getModuleInfo(id)!;
      g[id] = { entry: m.isEntry, imports: m.importedIds, dynamic: m.dynamicallyImportedIds };
    }
    writeFileSync(out + ".graph.json", JSON.stringify(g));
  },
};
export default {
  ...base,
  plugins: [
    ...(base.plugins ?? []),
    graph,
    visualizer({ filename: out + ".json", template: "raw-data", gzipSize: true, brotliSize: true }),
    visualizer({ filename: out + ".html", template: "treemap", gzipSize: true, brotliSize: true }),
  ],
};
