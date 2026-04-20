"use client";

import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { TodoWidget } from "@/components/todo-widget";
import { AuthBackup } from "@/components/auth-backup";
import { authClient } from "@/lib/auth-client";
import { BrowserAuthSecretStore } from "jazz-tools";

function HeaderActions() {
  const router = useRouter();
  const { data: authSession } = authClient.useSession();

  if (authSession?.session) {
    async function handleSignOut() {
      await authClient.signOut();
      await BrowserAuthSecretStore.clearSecret();
      router.push("/");
    }

    return (
      <div className="auth-nav">
        <p>Hello, {authSession.user.name}</p>
        <button type="button" className="btn-secondary" onClick={handleSignOut}>
          Sign out
        </button>
      </div>
    );
  }

  return (
    <div className="auth-nav">
      <p>
        <Link href="/signup" className="link">
          Sign up
        </Link>
        {" or "}
        <Link href="/signin" className="link">
          Sign in
        </Link>
      </p>
    </div>
  );
}

export default function Page() {
  const { data: authSession } = authClient.useSession();
  const authenticated = Boolean(authSession?.session);

  return (
    <main className="dashboard">
      <header>
        <Image
          src="/jazz.svg"
          alt="Jazz"
          className="wordmark"
          width={80}
          height={24}
          style={{ width: "100%", height: "auto" }}
          loading="eager"
        />
        <HeaderActions />
      </header>
      <TodoWidget />
      {!authenticated && <AuthBackup />}
    </main>
  );
}
