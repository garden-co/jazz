"use client";

import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { TodoWidget } from "@/src/components/todo-widget";
import { authClient } from "@/src/lib/auth-client";
import { BrowserAuthSecretStore } from "jazz-tools";
import { LocalFirstProvider } from "./local-first-provider";
import { BetterAuthProvider } from "./better-auth-provider";

const ENABLE_BETTERAUTH = process.env.NEXT_PUBLIC_ENABLE_BETTERAUTH === "1";
const Provider = ENABLE_BETTERAUTH ? BetterAuthProvider : LocalFirstProvider;

function HeaderActions() {
  const router = useRouter();
  const { data: authSession } = authClient.useSession();

  if (!ENABLE_BETTERAUTH) return null;

  if (authSession?.session) {
    async function handleSignOut() {
      await authClient.signOut();
      await BrowserAuthSecretStore.clearSecret();
      router.push("/");
    }

    return (
      <>
        <span className="user-email">{authSession.user.email}</span>
        <button type="button" className="btn-secondary" onClick={handleSignOut}>
          Sign out
        </button>
      </>
    );
  }

  return (
    <>
      <Link href="/signup" className="btn-secondary">
        Sign up to access from any device
      </Link>
      <Link href="/signin" className="link">
        Sign in
      </Link>
    </>
  );
}

export default function Page() {
  return (
    <Provider>
      <main className="dashboard">
        <header>
          <Image src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
          <HeaderActions />
        </header>
        <TodoWidget />
      </main>
    </Provider>
  );
}
