"use client";

import { ConvexAuthNextjsProvider } from "@convex-dev/auth/nextjs";
import { useAuthActions } from "@convex-dev/auth/react";
import { ConvexReactClient } from "convex/react";

// Convex Auth's Next.js integration: the session lives in cookies (set by
// middleware.ts through /api/auth), and the client authenticates its Convex
// websocket with it.
const convex = new ConvexReactClient(process.env.NEXT_PUBLIC_CONVEX_URL!);

export function ConvexClientProvider({ children }: { children: React.ReactNode }) {
  return <ConvexAuthNextjsProvider client={convex}>{children}</ConvexAuthNextjsProvider>;
}

/** Signs out of Convex Auth. */
export function useGracefulSignOut() {
  const { signOut } = useAuthActions();
  return signOut;
}
