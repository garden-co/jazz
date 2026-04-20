"use client";

import Image from "next/image";
import { TodoWidget } from "@/components/todo-widget";
import { AuthBackup } from "@/components/auth-backup";

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
      </header>
      <TodoWidget />
      <AuthBackup />
    </main>
  );
}
