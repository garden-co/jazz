import { assertBuildConfiguration, usesLocalDefaults } from "./build-config.mjs";

export function serverSecret(name: "BACKEND_SECRET" | "BETTER_AUTH_SECRET", localFallback: string) {
  const config = assertBuildConfiguration();
  const configured = process.env[name];
  if (configured) return configured;
  if (!usesLocalDefaults(config))
    throw new Error(`${name} must be configured for nonlocal PosterShop`);
  return localFallback;
}
