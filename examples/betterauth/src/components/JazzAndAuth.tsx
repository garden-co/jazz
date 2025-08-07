"use client";

import { JazzReactProviderWithBetterAuth } from "jazz-tools/better-auth/auth/react";
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
    <JazzReactProviderWithBetterAuth
      sync={{
        peer: "wss://cloud.jazz.tools/?key=betterauth-example@garden.co",
      }}
      betterAuth={{}}
    >
      {children}
      <JazzDevTools />
    </JazzReactProviderWithBetterAuth>
  );
}
