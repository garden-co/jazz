import { existsSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { fileURLToPath } from "node:url";

// fileURLToPath handles Windows drive letters correctly; the raw URL.pathname
// would yield "/C:/..." on Windows which fs APIs reject.
const envPath = fileURLToPath(new URL("../.env.local", import.meta.url));

if (existsSync(envPath)) {
  process.exit(0);
}

const secret = randomBytes(32).toString("base64url");

writeFileSync(
  envPath,
  ["NEXT_PUBLIC_ENABLE_BETTERAUTH=0", `BETTER_AUTH_SECRET=${secret}`, ""].join("\n"),
  "utf8",
);

console.log("Created .env.local");
