import path from "path";
import react from "@vitejs/plugin-react-swc";
import depsExternal from "rollup-plugin-node-externals";
import { defineConfig } from "vite";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react(), depsExternal()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    lib: {
      entry: path.resolve(__dirname, "src/app.tsx"),
      name: "JazzInspector",
      // the proper extensions will be added
      fileName: "app",
      formats: ["es"],
    },
  },
});
