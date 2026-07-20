import { createAuthClient } from "better-auth/vue";

export default defineNuxtRouteMiddleware(async (to) => {
  if (import.meta.server) return;
  if (!to.path.startsWith("/dashboard")) return;

  const authClient = createAuthClient();
  const { data } = await authClient.getSession();
  if (!data?.session) return navigateTo("/");
});
