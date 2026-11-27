// Convex Auth's manual setup (labs.convex.dev/auth/setup/manual): a JWT key
// pair and SITE_URL on the deployment. Runs once per local deployment.
import { execFileSync } from "node:child_process";
import { exportJWK, exportPKCS8, generateKeyPair } from "jose";

const convex = (...args) => execFileSync("npx", ["convex", ...args], { encoding: "utf8" });
const siteUrl = process.argv[2] ?? "http://127.0.0.1:4402";

if (!convex("env", "list").includes("JWKS")) {
  const keys = await generateKeyPair("RS256", { extractable: true });
  const privateKey = await exportPKCS8(keys.privateKey);
  const jwks = JSON.stringify({ keys: [{ use: "sig", ...(await exportJWK(keys.publicKey)) }] });
  convex("env", "set", "JWT_PRIVATE_KEY", "--", privateKey.trimEnd().replace(/\n/g, " "));
  convex("env", "set", "JWKS", jwks);
}
convex("env", "set", "SITE_URL", siteUrl);
