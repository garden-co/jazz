"use client";

import { Button } from "@/components/Button";
import { useAccount, useIsAuthenticated } from "jazz-react";
import { useAuth } from "jazz-react-auth-betterauth";
import Image from "next/image";
import { useCallback } from "react";

export default function Home() {
  const { authClient, account, state } = useAuth();
  const hasCredentials = state !== "anonymous";
  const { me, logOut } = useAccount({ resolve: { profile: {} } });
  const isAuthenticated = useIsAuthenticated();
  const signOut = useCallback(() => {
    authClient.signOut().catch(console.error).finally(logOut);
  }, [logOut, authClient]);
  console.log("me", me);
  console.log("account", account);
  console.log("state", state);
  console.log("hasCredentials", hasCredentials);
  console.log("isAuthenticated", isAuthenticated);

  return (
    <>
      <header className="absolute p-4 top-0 left-0 w-full z-10 flex items-center justify-between gap-4">
        <div className="float-start flex gap-4">
          {me && hasCredentials && isAuthenticated && (
            <>
              <Button onClick={signOut}>Sign out</Button>
              <Button href="/settings">Settings</Button>
            </>
          )}
        </div>
        <div className="float-end flex gap-4">
          {!hasCredentials && !isAuthenticated && (
            <>
              <Button href="/sign-in" variant="secondary">
                Sign in
              </Button>
              <Button href="/sign-up">Sign up</Button>
            </>
          )}
        </div>
      </header>
      <div className="grid grid-rows-[20px_1fr_20px] items-center justify-items-center min-h-screen p-8 pb-20 gap-16 sm:p-20 font-[family-name:var(--font-geist-sans)]">
        <main className="flex flex-col gap-[32px] row-start-2 items-center sm:items-start">
          <Image
            src="/jazz-logo.svg"
            alt="Jazz logo"
            width={180}
            height={38}
            priority
          />
          <p className="text-sm/6 text-center sm:text-left font-[family-name:var(--font-geist-mono)]">
            {me && hasCredentials && isAuthenticated && (
              <>
                {"Signed in as "}
                <span className="bg-black/[.05] dark:bg-white/[.06] px-1 py-0.5 rounded font-[family-name:var(--font-geist-mono)] font-semibold">
                  {me.profile.name}
                </span>
                .
              </>
            )}
            {!hasCredentials && !isAuthenticated && <>Not signed in.</>}
            {!hasCredentials && isAuthenticated && (
              <>Not connected to the authentication server.</>
            )}
            {hasCredentials && !isAuthenticated && (
              <>Authenticated, but not logged in. Try refreshing.</>
            )}
          </p>

          <div className="flex gap-4 items-center flex-col sm:flex-row">
            <a
              className="rounded-full border border-solid border-transparent transition-colors flex items-center justify-center bg-foreground text-background gap-2 hover:bg-[#383838] dark:hover:bg-[#ccc] font-medium text-sm sm:text-base h-10 sm:h-12 px-4 sm:px-5 sm:w-auto"
              href="https://jazz.tools/"
              target="_blank"
              rel="noopener noreferrer"
            >
              <Image src="/jazz.svg" alt="Jazz logo" width={20} height={20} />
              Start building
            </a>
            <a
              className="rounded-full border border-solid border-black/[.08] dark:border-white/[.145] transition-colors flex items-center justify-center gap-2 hover:bg-[#f2f2f2] dark:hover:bg-[#1a1a1a] hover:border-transparent font-medium text-sm sm:text-base h-10 sm:h-12 px-4 sm:px-5 w-full sm:w-auto"
              href="https://jazz.tools/docs"
              target="_blank"
              rel="noopener noreferrer"
            >
              <Image
                aria-hidden
                src="/file.svg"
                alt="File icon"
                width={16}
                height={16}
              />
              Read the docs
            </a>
          </div>
        </main>
        <footer className="row-start-3 flex gap-[24px] flex-wrap items-center justify-center">
          <a
            className="flex items-center gap-2 hover:underline hover:underline-offset-4"
            href="https://jazz.tools/api-reference"
            target="_blank"
            rel="noopener noreferrer"
          >
            <Image
              aria-hidden
              src="/file.svg"
              alt="File icon"
              width={16}
              height={16}
            />
            API reference
          </a>
          <a
            className="flex items-center gap-2 hover:underline hover:underline-offset-4"
            href="https://jazz.tools/examples"
            target="_blank"
            rel="noopener noreferrer"
          >
            <Image
              aria-hidden
              src="/window.svg"
              alt="Window icon"
              width={16}
              height={16}
            />
            Examples
          </a>
          <a
            className="flex items-center gap-2 hover:underline hover:underline-offset-4"
            href="https://jazz.tools/status"
            target="_blank"
            rel="noopener noreferrer"
          >
            <Image
              aria-hidden
              src="/globe.svg"
              alt="Globe icon"
              width={16}
              height={16}
            />
            Status
          </a>
          <a
            className="flex items-center gap-2 hover:underline hover:underline-offset-4"
            href="https://jazz.tools/showcase"
            target="_blank"
            rel="noopener noreferrer"
          >
            <Image
              aria-hidden
              src="/wrench.svg"
              alt="Wrench icon"
              width={16}
              height={16}
            />
            Built with Jazz
          </a>
        </footer>
      </div>
    </>
  );
}
