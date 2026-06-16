import { randomBytes } from "node:crypto";
import tailwindcss from "@tailwindcss/vite";
import { sveltekit } from "@sveltejs/kit/vite";
import { jazzSvelteKit } from "jazz-tools/dev/sveltekit";
import { defineConfig } from "vite";

// The server-side `asBackend()` Db needs a backend secret, which the managed
// runtime's local server only has if we give it one. Generate a fresh 32-byte
// secret per dev start (the same seed format as a client auth secret) so the
// example is zero-config — no secret to commit or set in `.env`. In production
// you provide a real, stable `BACKEND_SECRET`.
const backendSecret = process.env.BACKEND_SECRET ?? randomBytes(32).toString("base64url");

export default defineConfig({
  plugins: [tailwindcss(), sveltekit(), jazzSvelteKit({ server: { backendSecret } })],
  server: {
    fs: {
      allow: ["../.."],
    },
  },
});
