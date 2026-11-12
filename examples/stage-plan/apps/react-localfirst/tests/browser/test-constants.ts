/** Shared constants for browser tests. No Node.js imports. */
/** What a client here is scoped by: its endpoint, the backend's own URL. */
export const SCOPE = `${globalThis.location?.origin}/jazz`;

/** The app's backend: Jazz in the test server, on the page's own origin. */
export function serverUrl(): string {
  return `${location.origin}/jazz`;
}
