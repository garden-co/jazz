"use client";

import Image from "next/image";
import { authClient } from "@/lib/auth-client";
import { TodoWidget } from "@/components/todo-widget";
import { useJazzLifecycle } from "@/components/jazz-provider";

export default function DashboardPage() {
  const lifecycle = useJazzLifecycle();
  const { data: session } = authClient.useSession();
  if (!session) return null;

  async function handleSignOut() {
    try {
      await lifecycle.transition(async (accounts) => {
        await authClient.signOut();
        accounts.logout();
      });
      window.location.assign("/");
    } catch (cause) {
      lifecycle.reportFailure(cause);
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
