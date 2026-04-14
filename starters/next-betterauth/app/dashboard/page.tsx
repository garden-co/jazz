"use client";

import { useRouter } from "next/navigation";
import Image from "next/image";
import { authClient } from "@/src/lib/auth-client";
import { TodoWidget } from "@/src/components/todo-widget";

export default function DashboardPage() {
  const router = useRouter();
  const { data: session, isPending } = authClient.useSession();

  if (isPending || !session) return null;

  async function handleSignOut() {
    await authClient.signOut();
    router.push("/");
  }

  return (
    <main className="dashboard">
      <header>
        <Image src="/jazz.svg" alt="Jazz" className="wordmark" width={80} height={24} />
        <p>Hello, {session.user.name}</p>
        <button type="button" onClick={handleSignOut}>
          Sign out
        </button>
      </header>
      <TodoWidget />
    </main>
  );
}
