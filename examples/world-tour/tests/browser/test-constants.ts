/** The app's backend: Jazz in the test server, on the page's own origin. */
export const SERVER_URL = `${globalThis.location?.origin}/jazz`;
/** What a client here is scoped by: its endpoint, the backend's own URL. */
export const SCOPE = `${globalThis.location?.origin}/jazz`;
