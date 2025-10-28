import { auth } from "@/lib/auth";
import { toNextJsHandler } from "better-auth/next-js";

export const { GET, POST } = (() => {
  if (!process.env.NEXT_PUBLIC_AUTH_BASE_URL) {
    return toNextJsHandler(auth.handler);
  } else {
    return { GET: undefined, POST: undefined };
  }
})();
