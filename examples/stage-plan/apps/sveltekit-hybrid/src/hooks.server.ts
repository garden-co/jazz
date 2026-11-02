import type { Handle } from "@sveltejs/kit";
import { building } from "$app/environment";
import { svelteKitHandler } from "better-auth/svelte-kit";
import { auth } from "$lib/server/auth";
import { jazz } from "$lib/server/jazz";

export const handle: Handle = async ({ event, resolve }) => {
  if (building) return resolve(event);
  // Sync and sign-in for the browser, on this app's own origin.
  const { pathname } = event.url;
  if (pathname === "/jazz" || pathname.startsWith("/jazz/")) {
    return (await jazz()).fetch(event.request);
  }
  // Better Auth answers /api/auth/*; everything else is the app.
  return svelteKitHandler({ event, resolve, auth: auth(), building });
};
