/**
 * Whether this process runs on Vercel's serverless hosts: no lasting disk,
 * sockets only through `upgradeWebSocket`. Only `vercel dev` (VERCEL_ENV
 * development) is a local machine; a VERCEL flag without VERCEL_ENV counts
 * as a host, the safer default for refusing local disk.
 */
export function onVercelHost(env: Record<string, string | undefined> = process.env): boolean {
  return !!env.VERCEL && env.VERCEL_ENV !== "development";
}
