import { assertBuildConfiguration } from "./build-config.mjs";

/**
 * A server secret from the environment. There are no fallbacks: deployments
 * configure both secrets, and `pnpm dev` generates local ones.
 */
export function serverSecret(name: "BACKEND_SECRET" | "BETTER_AUTH_SECRET"): string {
  assertBuildConfiguration();
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not configured`);
  return value;
}
