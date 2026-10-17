/**
 * One Jazz environment for the browser client, the account manager and the
 * auth backend. If they disagree, sign-in succeeds but the two sides read
 * and write different environments, so the library stays empty.
 */
export const jazzEnv: "dev" | "prod" =
  process.env.NEXT_PUBLIC_JAZZ_ENV === "prod" || process.env.NEXT_PUBLIC_JAZZ_ENV === "dev"
    ? process.env.NEXT_PUBLIC_JAZZ_ENV
    : process.env.NODE_ENV === "production"
      ? "prod"
      : "dev";
