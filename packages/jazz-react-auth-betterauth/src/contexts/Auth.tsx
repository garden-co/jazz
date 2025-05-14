"use client";

import type { ClientOptions } from "better-auth/client";
import { createContext, useContext } from "react";
import { useBetterAuth } from "../index.js";

const AuthContext = createContext<ReturnType<typeof useBetterAuth> | null>(
  null,
);

export function AuthProvider({
  children,
  options,
}: {
  children: React.ReactNode;
  options?: Parameters<typeof useBetterAuth>[0];
}) {
  const auth = useBetterAuth(options);
  return <AuthContext.Provider value={auth}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
