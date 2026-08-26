const LOCAL_ORIGIN = "http://127.0.0.1:3000";
const LOCAL_APP_ID = "poster-shop-local";
const LOCAL_SERVER_URL = "http://127.0.0.1:4200";

/** Only the complete checked-in local tuple may use deterministic dev secrets. */
function usesExplicitLocalDefaults(): boolean {
  return (
    (process.env.NEXT_PUBLIC_APP_ORIGIN ?? LOCAL_ORIGIN) === LOCAL_ORIGIN &&
    (process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? LOCAL_APP_ID) === LOCAL_APP_ID &&
    (process.env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? LOCAL_SERVER_URL) === LOCAL_SERVER_URL
  );
}

export function serverSecret(name: "BACKEND_SECRET" | "BETTER_AUTH_SECRET", localFallback: string) {
  const configured = process.env[name];
  if (configured) return configured;
  if (!usesExplicitLocalDefaults()) {
    throw new Error(
      `${name} must be configured whenever NEXT_PUBLIC_APP_ORIGIN, NEXT_PUBLIC_JAZZ_APP_ID, or NEXT_PUBLIC_JAZZ_SERVER_URL is nonlocal`,
    );
  }
  return localFallback;
}
