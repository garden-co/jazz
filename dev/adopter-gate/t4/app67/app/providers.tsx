"use client";
import { JazzProvider } from "jazz-tools/react";
import { AuthForm } from "./auth-form";

export function Providers({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      config={{ serverUrl: "/jazz", initial: "device" }}
      signedOut={
        <div data-testid="signed-out">
          <p>Signed out.</p>
          <AuthForm />
        </div>
      }
    >
      {children}
    </JazzProvider>
  );
}
