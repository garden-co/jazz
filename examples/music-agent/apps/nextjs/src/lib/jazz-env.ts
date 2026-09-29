/**
 * The Jazz environment the browser and the server both use. They must agree:
 * the server seeds the workspace that the browser then reads.
 */
export const jazzEnv: "dev" | "prod" =
  process.env.NEXT_PUBLIC_JAZZ_ENV === "prod" ||
  (process.env.NEXT_PUBLIC_JAZZ_ENV !== "dev" && process.env.NODE_ENV === "production")
    ? "prod"
    : "dev";
