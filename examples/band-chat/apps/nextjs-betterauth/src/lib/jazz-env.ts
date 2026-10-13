/**
 * The Jazz environment shared by the browser client and the backend session.
 * Both sides must agree, or they read and write different data and the UI
 * stays empty after a successful sign-in.
 */
export const JAZZ_ENV =
  process.env.NEXT_PUBLIC_JAZZ_ENV || (process.env.NODE_ENV === "production" ? "prod" : "dev");
