"use client";

import Image from "next/image";
import { authClient } from "@/lib/auth-client";
import { TodoWidget } from "@/components/todo-widget";
import { useAuthActions } from "@/components/jazz-provider";

export default function DashboardPage() {
  const actions = useAuthActions();
  const { data: session } = authClient.useSession();
  if (!session) return null;

  async function handleSignOut() {
    try {
      await actions.signOut();
      window.location.assign("/");
    } catch {
      // Shared auth state exposes logout failures and retry.
    }
  }

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
        <div className="auth-nav">
          <p>Hello, {session.user.name}</p>
          <button type="button" onClick={handleSignOut}>
            Sign out
          </button>
        </div>
      </header>
      <TodoWidget />
    </main>
  );
}
