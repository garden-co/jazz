import { jazzPlugin } from "jazz-tools/dev/vite";
import { defineConfig } from "vite";
import { LOCAL_APP_ID } from "./server/config";

// The Hono server (server/index.ts) runs Jazz and Better Auth. In development
// it listens on 3001 and Vite proxies to it, so the browser sees one origin.
const backend = "http://127.0.0.1:3001";

/** The browser and the server name the same Jazz app; production must set it. */
function jazzAppId(mode: string) {
  const configured = process.env.VITE_JAZZ_APP_ID;
  if (configured) return configured;
  if (mode === "production") throw new Error("VITE_JAZZ_APP_ID must be configured in production");
  return LOCAL_APP_ID;
}

export default defineConfig(({ mode }) => ({
  plugins: [jazzPlugin({ server: false })],
  define: { "import.meta.env.VITE_JAZZ_APP_ID": JSON.stringify(jazzAppId(mode)) },
  worker: { format: "es" },
  server: {
    host: "127.0.0.1",
    port: 3000,
    strictPort: true,
    proxy: {
      "/api": { target: backend },
      "/jazz": { target: backend, ws: true },
    },
  },
}));
