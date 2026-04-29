import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { withJazz } from "jazz-tools/dev/expo";

const require = createRequire(import.meta.url);
const { getDefaultConfig } = require("expo/metro-config");

const __filename = fileURLToPath(import.meta.url);
const projectRoot = path.dirname(__filename);

const config = getDefaultConfig(projectRoot);

// pnpm uses symlinks for hoisted packages
config.resolver.unstable_enableSymlinks = true;

// Start Jazz dev server and inject EXPO_PUBLIC_JAZZ_* env vars for Metro to inline.
// `server.jwksUrl` points the Jazz dev server at the BetterAuth Hono server's
// JWKS endpoint so it can verify the JWTs the client presents on the WS handshake.
// Without this, the dev server can't authenticate the client and silently
// withholds query results.
// Metro supports async config, so top-level await is fine here.
await withJazz(
  {},
  {
    schemaDir: projectRoot,
    server: {
      jwksUrl: `${process.env.APP_ORIGIN ?? "http://localhost:3001"}/api/auth/jwks`,
    },
  },
);

export default config;
