"use client";

import { AuthProvider } from "@/contexts/Auth";
import { JazzProvider } from "jazz-react";
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
      sync={{ peer: "wss://cloud.jazz.tools/?key=dashboard@garden.co" }}
    >
      <>
        <AuthProvider>{children}</AuthProvider>
        <JazzDevTools />
      </>
    </JazzProvider>
  );
}
