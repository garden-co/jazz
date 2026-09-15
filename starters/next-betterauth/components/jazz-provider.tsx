"use client";
import { useEffect } from "react";
import { usePathname, useRouter } from "next/navigation";
import { JazzProvider as Provider, betterAuth } from "jazz-tools/react";
import { authClient } from "@/lib/auth-client";
import { SignInForm } from "./sign-in-form";

function SignedOut() {
  const pathname = usePathname();
  const router = useRouter();
  useEffect(() => {
    if (pathname !== "/") router.replace("/");
  }, [pathname, router]);
  return (
    <main className="page-center">
      <SignInForm />
    </main>
  );
}

export function JazzProvider({ children }: React.PropsWithChildren) {
  return (
    <Provider
      appId={process.env.NEXT_PUBLIC_JAZZ_APP_ID!}
      serverUrl={process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!}
      auth={betterAuth(authClient)}
      signedOut={<SignedOut />}
    >
      {children}
    </Provider>
  );
}
