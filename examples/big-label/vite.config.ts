import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { defineConfig } from "vite";

export default defineConfig({ plugins: [react(), jazzPlugin()] });
