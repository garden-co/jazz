import { defineConfig } from "@neon/config/v1";

// Managed Better Auth and Realtime on the project's branch; the browser origins
// Realtime accepts WebSockets from.
export default defineConfig({
  auth: true,
  preview: { realtime: { allowedOrigins: ["http://127.0.0.1:4332", "http://localhost:3000"] } },
});
