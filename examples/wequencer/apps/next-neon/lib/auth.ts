import "server-only";
import { createNeonAuth } from "@neon/auth/next/server";

let auth: ReturnType<typeof createNeonAuth> | undefined;
/**
 * Managed Better Auth, mounted on the app's origin (as in Neon's Next.js
 * example). Lazy: createNeonAuth checks the cookie secret when constructed,
 * so `next build` needs none.
 */
export function getAuth() {
  return (auth ??= createNeonAuth({
    baseUrl: process.env.NEON_AUTH_BASE_URL!,
    cookies: { secret: process.env.NEON_AUTH_COOKIE_SECRET! },
  }));
}

/** The signed-in user, or null. Every Server Action and Route Handler calls it itself. */
export async function getUser() {
  const { data, error } = await getAuth().getSession();
  if (error) console.warn("[auth] getSession failed", error.code);
  return data?.user ?? null;
}
