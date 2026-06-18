import path from "node:path";
import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

const solidRoot = path.resolve(__dirname, "src/solid");
const entryFile = path.resolve(solidRoot, "index.ts");

function shouldExternalizeModule(moduleId: string, parentModuleId: string | undefined): boolean {
  if (moduleId === "solid-js" || moduleId.startsWith("solid-js/")) {
    return true;
  }
  if (!parentModuleId) {
    return false;
  }

  // Files from "jazz-tools" that do not belong to the solid folder, like the runtime,
  // should not be bundled here. They are already bundled in the `pnpm build` step.
  const modulePath = path.resolve(path.dirname(parentModuleId), moduleId);
  return !isPathInside(modulePath, solidRoot);
}

export default defineConfig(({ mode }) => {
  const ssr = mode === "solid-ssr";

  // On package.json script "build:solid",
  // First build pass is "solid-dom", second build pass is "solid-ssr"
  const isFirstBuildPass = !ssr;

  return {
    plugins: [solid({ ssr })],
    build: {
      ssr: ssr ? entryFile : undefined,
      outDir: "dist/solid",
      emptyOutDir: isFirstBuildPass, // First run (DOM) cleans output. Second run (SSR) appends server bundle.
      sourcemap: true,
      lib: ssr
        ? undefined
        : {
            entry: entryFile,
            formats: ["es"] as const,
            fileName: () => "index.js",
          },
      rolldownOptions: {
        external: shouldExternalizeModule,
        output: {
          format: "es" as const,
          preserveModules: true,
          preserveModulesRoot: "src/solid",
          entryFileNames: ssr
            ? (chunkInfo) => (chunkInfo.name === "index" ? "index.ssr.js" : "[name].ssr.js")
            : "[name].js",
        },
      },
    },
  };
});

export function normalizePath(input: string): string {
  let value = path.resolve(input).replaceAll("\\", "/");
  const root = path.parse(value).root.replaceAll("\\", "/");

  if (value !== root) {
    value = value.replace(/\/+$/, "");
  }
  // Windows is case Insensitive
  if (process.platform === "win32") {
    value = value.toLowerCase();
  }
  return value;
}

function isPathInside(child: string, parent: string): boolean {
  const normChild = normalizePath(child);
  const normParent = normalizePath(parent);

  return normChild === normParent || normChild.startsWith(`${normParent}/`);
}
