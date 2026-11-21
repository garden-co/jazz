import { ConvexReactClient } from "convex/react";

/**
 * An anonymous Convex Auth account, like Jazz's device accounts: no sign-up,
 * the browser profile holds the session. Crew join shows through invite links.
 */
export const convex = new ConvexReactClient(import.meta.env.VITE_CONVEX_URL as string);
