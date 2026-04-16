"use client";

import Image from "next/image";
import { authClient } from "@/lib/auth-client";
import { TodoWidget } from "@/components/todo-widget";

export default function DashboardPage() {
  const { data: session } = authClient.useSession();
  if (!session) return null;

  async function handleSignOut() {
    await authClient.signOut();
    window.location.assign("/");
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
        <p>Hello, {session.user.name}</p>
        <button type="button" onClick={handleSignOut}>
          Sign out
        </button>
      </header>
      <TodoWidget />
    </main>
  );
}
