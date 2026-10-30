import { resolve } from "node:path";
import { defineConfig, type UserConfig } from "vite";
import react from "@vitejs/plugin-react-swc";

const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
  throw new Error("sealed correctness consumer is missing its admitted WASM package");

const sealedWasmAlias: Record<string, string> = sealedWasmPackage
  ? { "jazz-wasm": resolve(sealedWasmPackage, "jazz_wasm.js") }
  : {};

export default defineConfig(({ mode }): UserConfig => {
  if (mode === "embedded") {
    if (process.env.JAZZ_TEST_SEALED_INSPECTOR_DIST === "1")
      throw new Error(
        "cannot rebuild the embedded inspector while test consumers share its assets",
      );
    return {
      plugins: [react()],
      resolve: { alias: sealedWasmAlias },
      base: "./",
      worker: { format: "es" },
      build: {
        outDir: "dist-embedded",
        emptyOutDir: true,
        rollupOptions: { input: { index: resolve(__dirname, "embedded.html") } },
      },
    };
  }

  // The standalone "web" build (the default).
  return {
    plugins: [react()],
    resolve: { alias: sealedWasmAlias },
    base: "/",
    publicDir: "public",
    worker: { format: "es" },
    build: {
      outDir: "dist",
      emptyOutDir: true,
    },
  };
});
