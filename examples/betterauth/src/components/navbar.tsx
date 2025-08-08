"use client";

import { useAuth } from "jazz-react-auth-betterauth";
import { Account } from "jazz-tools";
import { useAccount, useIsAuthenticated } from "jazz-tools/react";
import Image from "next/image";
import Link from "next/link";
import { useCallback } from "react";
import { Button } from "@/components/ui/button";

export function Navbar() {
  const { authClient } = useAuth();
  const { logOut } = useAccount(Account, { resolve: { profile: {} } });

  const isAuthenticated = useIsAuthenticated();

  const signOut = useCallback(() => {
    authClient.signOut().catch(console.error).finally(logOut);
  }, [logOut, authClient]);

  return (
    <header className="absolute p-4 top-0 left-0 w-full z-10 flex justify-between">
      <nav className="flex gap-4">
        <Link href="/">
          <Image
            src="/jazz-logo.svg"
            alt="Jazz logo"
            width={96}
            height={96}
            priority
          />
        </Link>
        {isAuthenticated && (
          <Button asChild variant="link">
            <Link href="/settings">Settings</Link>
          </Button>
        )}
      </nav>
      <nav className="flex gap-4">
        {isAuthenticated ? (
          <Button onClick={signOut}>Sign out</Button>
        ) : (
          <>
            <Button asChild variant="secondary">
              <Link href="/auth/sign-in">Sign in</Link>
            </Button>
            <Button asChild>
              <Link href="/auth/sign-up">Sign up</Link>
            </Button>
          </>
        )}
      </nav>
    </header>
  );
}
