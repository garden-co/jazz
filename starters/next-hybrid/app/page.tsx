"use client";

import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { TodoWidget } from "@/components/todo-widget";
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
        <span className="user-email">{authSession.user.email}</span>
        <button type="button" className="btn-secondary" onClick={handleSignOut}>
          Sign out
        </button>
      </div>
    );
  }

  return (
    <div className="auth-nav">
      <p>
        <Link href="/signup" className="btn-secondary">
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
    </main>
  );
}
