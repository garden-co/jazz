// Next 16 calls middleware "proxy"; Convex Auth's Next.js middleware runs here.
// It serves /api/auth (sign-in sets the session cookies) and refreshes them.
import { convexAuthNextjsMiddleware } from "@convex-dev/auth/nextjs/server";

export default convexAuthNextjsMiddleware();

export const config = {
  matcher: ["/((?!.*\\..*|_next).*)", "/", "/(api|trpc)(.*)"],
};
