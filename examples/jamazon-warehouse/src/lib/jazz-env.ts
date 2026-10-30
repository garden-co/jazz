/**
 * The Jazz environment the browser and the server both use. They must agree:
 * the server's backend client seeds and staffs warehouses in this environment,
 * and every operator's browser then reads them from the same one.
 * NEXT_PUBLIC_JAZZ_ENV wins; otherwise a production build is "prod" and
 * everything else is "dev".
 */
export const jazzEnv: "dev" | "prod" =
  process.env.NEXT_PUBLIC_JAZZ_ENV === "prod" ||
  (process.env.NEXT_PUBLIC_JAZZ_ENV !== "dev" && process.env.NODE_ENV === "production")
    ? "prod"
    : "dev";
