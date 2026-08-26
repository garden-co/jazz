/**
 * Development may use deterministic fixture values so the reference app is
 * runnable without a secret manager. Production must always inject its own
 * secret; a bundled fallback would silently collapse the trust boundary.
 */
export function serverSecret(name: "BACKEND_SECRET" | "BETTER_AUTH_SECRET", devFallback: string) {
  const configured = process.env[name];
  if (configured) return configured;
  if (process.env.NODE_ENV === "production") {
    throw new Error(`${name} must be configured in production`);
  }
  return devFallback;
}
