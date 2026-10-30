import { auth } from "$lib/auth";
import { svelteKitHandler } from "better-auth/svelte-kit";
import { isAPIError } from "better-auth/api";
import { building } from "$app/environment";
import { redirect, type Handle } from "@sveltejs/kit";

export const handle: Handle = async ({ event, resolve }) => {
  const path = event.url.pathname;

  // Don't interfere with auth API routes — Better Auth's
  // svelteKitHandler owns them.
  if (!path.startsWith("/api/auth")) {
    const needsSession = path === "/" || path.startsWith("/dashboard");
    if (needsSession) {
      const session = await auth.api
        .getSession({
          headers: event.request.headers,
        })
        .catch((error) => {
          if (isAPIError(error) && error.status === "UNAUTHORIZED") return null;
          throw error;
        });
      if (path === "/" && session) {
        throw redirect(303, "/dashboard");
      }
      if (path.startsWith("/dashboard") && !session) {
        throw redirect(303, "/");
      }
    }
  }

  return svelteKitHandler({ event, resolve, auth, building });
};
