import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { WORKOS_CLIENT_ID } from "./constants.js";

export default defineConfig({
  plugins: [
    react(),
    jazzPlugin({
      server: {
        jwksUrl: `https://api.workos.com/sso/jwks/${WORKOS_CLIENT_ID}`,
      },
    }),
  ],
});
