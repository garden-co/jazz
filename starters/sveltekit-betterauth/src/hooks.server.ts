import { auth } from "$lib/auth";
import { svelteKitHandler } from "better-auth/svelte-kit";
import { getSessionCookie } from "better-auth/cookies";
import { building } from "$app/environment";
import { redirect, type Handle } from "@sveltejs/kit";

export const handle: Handle = async ({ event, resolve }) => {
  const path = event.url.pathname;

  // Don't interfere with auth API routes — svelteKitHandler owns them.
  if (!path.startsWith("/api/auth")) {
    const sessionCookie = getSessionCookie(event.request);
    if (path === "/" && sessionCookie) {
      throw redirect(303, "/dashboard");
    }
    if (path.startsWith("/dashboard") && !sessionCookie) {
      throw redirect(303, "/");
    }
  }

  return svelteKitHandler({ event, resolve, auth, building });
};
