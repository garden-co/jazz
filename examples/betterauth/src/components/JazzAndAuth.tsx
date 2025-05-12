"use client";

import { emailOTPClient, magicLinkClient } from "better-auth/client/plugins";
import { JazzProvider } from "jazz-react";
import { AuthProvider } from "jazz-react-auth-betterauth";
import { type ReactNode, lazy } from "react";

const JazzDevTools =
  process.env.NODE_ENV === "production"
    ? () => null
    : lazy(() =>
        import("jazz-inspector").then((res) => ({
          default: res.JazzInspector,
        })),
      );

export function JazzAndAuth({ children }: { children: ReactNode }) {
  return (
    <JazzProvider
      sync={{
        peer: "wss://cloud.jazz.tools/?key=betterauth-example@garden.co",
      }}
    >
      <>
        <AuthProvider
          options={{
            baseURL: process.env.NEXT_PUBLIC_AUTH_BASE_URL,
            plugins: [magicLinkClient(), emailOTPClient()],
          }}
        >
          {children}
        </AuthProvider>
        <JazzDevTools />
      </>
    </JazzProvider>
  );
}
