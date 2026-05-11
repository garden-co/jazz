import { existsSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = join(__dirname, "..", ".env");

if (!existsSync(envPath)) {
  const secret = randomBytes(32).toString("hex");
  writeFileSync(envPath, `BETTER_AUTH_SECRET=${secret}\nAPP_ORIGIN=http://localhost:3001\n`);
  console.log("No .env detected. Generated .env with a random BETTER_AUTH_SECRET");
}
