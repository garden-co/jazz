"use client";

import { AuthProvider } from "jazz-react-auth-betterauth";
import { JazzReactProvider } from "jazz-tools/react";
import { lazy, type ReactNode } from "react";

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
      <AuthProvider
        options={{
          baseURL: process.env.NEXT_PUBLIC_AUTH_BASE_URL,
        }}
      >
        {children}
      </AuthProvider>
      <JazzDevTools />
    </JazzReactProvider>
  );
}
