import { defineConfig } from "@neon/config/v1";

// Realtime on the project's branch; the browser origins it accepts WebSockets from.
export default defineConfig({
  preview: { realtime: { allowedOrigins: ["http://127.0.0.1:4320", "http://localhost:5173"] } },
});
