"use client";

import { AuthProvider } from "jazz-tools/better-auth/auth/react";
import { betterAuthClient } from "@/lib/auth-client";

export function App() {
  return (
    /* Other providers (e.g. your Jazz Provider) */
    <AuthProvider betterAuthClient={betterAuthClient}>
      {/* your app */}
    </AuthProvider>
  );
}
