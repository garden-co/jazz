import { auth } from "~/server/utils/auth";

export default defineEventHandler(async (event) => {
  const path = getRequestURL(event).pathname;
  if (!path.startsWith("/dashboard")) return;

  const session = await auth.api.getSession({ headers: event.headers });
  if (!session) {
    await sendRedirect(event, "/");
  }
});
