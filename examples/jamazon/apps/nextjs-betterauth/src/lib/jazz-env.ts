/**
 * The Jazz environment the browser and the backend share. Both sides must
 * agree, or the backend writes to a different environment than the browser
 * reads and the store stays empty. Set NEXT_PUBLIC_JAZZ_ENV to override.
 */
export const jazzEnv: string =
  process.env.NEXT_PUBLIC_JAZZ_ENV || (process.env.NODE_ENV === "production" ? "prod" : "dev");
