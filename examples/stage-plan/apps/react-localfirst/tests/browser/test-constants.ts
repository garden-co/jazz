/** Shared constants for browser tests. No Node.js imports. */
export const APP_ID = "019d6a51-3c1e-7b42-9f0e-5a7c2e8b4d61";

/** The app's backend: Jazz in the test server, on the page's own origin. */
export function serverUrl(): string {
  return `${location.origin}/jazz`;
}
