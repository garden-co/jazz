"use client";

import { Button } from "@/components/Button";
import { useAuth } from "@/contexts/Auth";
import { useAccount, useIsAuthenticated } from "jazz-react";
import type { AuthCredentials } from "jazz-tools";
import Image from "next/image";
import { useCallback, useEffect, useState } from "react";

export default function Home() {
  const auth = useAuth();
  const [user, setUser] = useState<AuthCredentials | undefined>(undefined);
  useEffect(() => {
    auth.authClient.useSession.subscribe(({ data }) => {
      if (data?.user.encryptedCredentials) {
        auth.authClient.jazzPlugin
          .decryptCredentials()
          .then((x) => {
            setUser(x.data === null ? undefined : x.data);
          })
          .catch((error) => {
            console.error("Error decrypting credentials:", error);
          });
      } else if (data && !data.user.encryptedCredentials) {
        auth.signIn().then(() => {
          auth.authClient.jazzPlugin
            .decryptCredentials()
            .then((x) => {
              setUser(x.data === null ? undefined : x.data);
            })
            .catch((error) => {
              console.error("Error decrypting credentials:", error);
            });
        });
      }
    });
  }, [user]);
  const { me, logOut } = useAccount({ resolve: { profile: true } });
  const isAuthenticated = useIsAuthenticated();
  const signOut = useCallback(() => {
    auth.authClient.signOut({
      fetchOptions: {
        onSuccess: () => {
          logOut();
        },
      },
    });
  }, [logOut, auth]);
  console.log("me", JSON.stringify(me));
  console.log("user", JSON.stringify(user));
  console.log("isAuthenticated", JSON.stringify(isAuthenticated));

  return (
    <>
      <header className="absolute p-4 top-0 left-0 w-full z-10 flex items-center justify-between gap-4">
        <div className="float-start">
          {me && user && isAuthenticated && (
            <Button className="float-start" onClick={signOut}>
              Sign out
            </Button>
          )}
        </div>
        <div className="float-end flex gap-4">
          {!user && !isAuthenticated && (
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
          {/* <Image
					className="dark:invert"
					src="/next.svg"
					alt="Next.js logo"
					width={180}
					height={38}
					priority
				/> */}
          <p className="text-sm/6 text-center sm:text-left font-[family-name:var(--font-geist-mono)]">
            {me && user && isAuthenticated && (
              <>
                {"Signed in as "}
                <span className="bg-black/[.05] dark:bg-white/[.06] px-1 py-0.5 rounded font-[family-name:var(--font-geist-mono)] font-semibold">
                  {me.profile.name}
                </span>
              </>
            )}
            {!user && !isAuthenticated && <>{"Not signed in"}</>}
            {!user && isAuthenticated && (
              <>{"Could not connect to the authentication server"}</>
            )}
            .
          </p>
          {/* <ol className="list-inside list-decimal text-sm/6 text-center sm:text-left font-[family-name:var(--font-geist-mono)]">
						<li className="mb-2 tracking-[-.01em]">
							Get started by editing{" "}
							<code className="bg-black/[.05] dark:bg-white/[.06] px-1 py-0.5 rounded font-[family-name:var(--font-geist-mono)] font-semibold">
								src/app/page.tsx
							</code>
							.
						</li>
						<li className="tracking-[-.01em]">
							Save and see your changes instantly.
						</li>
					</ol> */}

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
