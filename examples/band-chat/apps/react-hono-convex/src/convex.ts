import { ConvexReactClient } from "convex/react";

/** BandChat's Convex client: queries, mutations and auth over one websocket. */
export const convex = new ConvexReactClient(import.meta.env.VITE_CONVEX_URL as string);
