"use client";
import { JazzProvider as Provider, betterAuth } from "jazz-tools/react";
import { authClient } from "@/lib/auth-client";
import { SignInForm } from "./sign-in-form";

export function JazzProvider({ children }: React.PropsWithChildren) {
  return (
    <Provider
      appId={process.env.NEXT_PUBLIC_JAZZ_APP_ID!}
      serverUrl={process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!}
      auth={betterAuth(authClient)}
      signedOut={
        <main className="page-center">
          <SignInForm />
        </main>
      }
    >
      {children}
    </Provider>
  );
}
