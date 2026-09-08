"use client";

import Image from "next/image";
import { authClient } from "@/lib/auth-client";
import { TodoWidget } from "@/components/todo-widget";
import { useJazzAuth } from "jazz-tools/react";

export default function DashboardPage() {
  const { logout } = useJazzAuth();
  const { data: session } = authClient.useSession();
  if (!session) return null;

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
          <button type="button" onClick={() => void logout()}>
            Sign out
          </button>
        </div>
      </header>
      <TodoWidget />
    </main>
  );
}
