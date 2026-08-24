/**
 * Return a server-only secret, allowing an intentionally convenient fallback
 * only while running this copyable example in development.
 *
 * A committed fallback is never an acceptable production credential: anyone
 * who can read the example source can impersonate that server capability.
 */
export function serverSecret(name: string, developmentFallback: string): string {
  const configured = process.env[name];
  if (configured) return configured;
  if (process.env.NODE_ENV === "production") {
    throw new Error(`${name} must be configured in production.`);
  }
  return developmentFallback;
}
