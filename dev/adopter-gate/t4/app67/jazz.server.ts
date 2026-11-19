import { betterAuth } from "better-auth";
import { nextCookies } from "better-auth/next-js";
import { acceptDeviceAccounts, createJazz, perBranch, vercel } from "jazz-tools/backend";
import { jazzAdapter } from "jazz-tools/better-auth-adapter";
import { app } from "./jazz.generated";
import permissions from "./jazz.permissions.json" with { type: "json" };
import { authOptions } from "./auth-options";

// Synthetic eval data: the team comes from the email's local part (red-… / blue-…).
function teamFor(email: string): string | null {
  const local = email.split("@")[0] ?? "";
  if (local.startsWith("red")) return "red";
  if (local.startsWith("blue")) return "blue";
  return null;
}

export const jazz = createJazz({
  app,
  permissions,
  store: vercel({
    // T4_NAMESPACE: the adopter gate's per-run preview core (set only on its preview deployments).
    namespace: ({ environment, branch }) =>
      process.env.T4_NAMESPACE ||
      (environment === "production" ? "production" : perBranch(branch) || "cli-previews"),
  }),
  publicUrl: [
    process.env.VERCEL_PROJECT_PRODUCTION_URL,
    process.env.VERCEL_BRANCH_URL,
    process.env.VERCEL_URL,
  ]
    .filter(Boolean)
    .map((host) => `https://${host}/jazz`),
  authenticate: async (request) => {
    const session = await auth.api.getSession({ headers: request.headers });
    if (!session) return acceptDeviceAccounts(request);
    const team = teamFor(session.user.email);
    return {
      userId: session.user.id,
      expiresAt: Math.floor(session.session.expiresAt.getTime() / 1000),
      claims: team ? { team } : {},
    };
  },
});

export const auth = betterAuth({
  ...authOptions,
  database: jazzAdapter({ db: async () => jazz.db, schema: app.wasmSchema }),
  plugins: [nextCookies()],
});
