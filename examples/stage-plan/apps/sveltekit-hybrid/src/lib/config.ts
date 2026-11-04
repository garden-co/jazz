/** The Jazz app id a local run uses when PUBLIC_JAZZ_APP_ID is not set. */
export const LOCAL_APP_ID = "00000000-0000-0000-0000-00000005a6e1";
export const LOCAL_ORIGIN = "http://localhost:5173";

/** Accounts this app's backend signs in carry this issuer. */
export const backendIssuer = (appId: string) => `jazz:app:${appId}`;
