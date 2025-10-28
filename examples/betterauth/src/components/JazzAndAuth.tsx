"use client";

import { AuthProvider } from "jazz-tools/better-auth/auth/react";
import { JazzReactProvider } from "jazz-tools/react";
import { betterAuthClient } from "@/lib/auth-client";
import { type ReactNode, lazy } from "react";

const JazzDevTools =
  process.env.NODE_ENV === "production"
    ? () => null
    : lazy(() =>
        import("jazz-tools/inspector").then((res) => ({
          default: res.JazzInspector,
        })),
      );

export function JazzAndAuth({ children }: { children: ReactNode }) {
  return (
    <JazzReactProvider
      sync={{
        peer: "wss://cloud.jazz.tools/?key=betterauth-example@garden.co",
      }}
    >
      <AuthProvider betterAuthClient={betterAuthClient}>
        {children}
      </AuthProvider>
      <JazzDevTools />
    </JazzReactProvider>
  );
}
