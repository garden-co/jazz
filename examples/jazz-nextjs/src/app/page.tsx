"use client";

import { JazzAccount } from "@/schema";
import { useAccount } from "jazz-tools/react";
import { TodoList } from "./TodoList";
import { useState } from "react";

export default function Home() {
  const me = useAccount(JazzAccount);
  const [copied, setCopied] = useState(false);

  if (!me.$isLoaded) {
    return null;
  }

  const handleCopyUrl = async () => {
    if (typeof window === "undefined") {
      return;
    }

    const accountUrl = `${window.location.origin}/account/${me.$jazz.id}`;

    await navigator.clipboard.writeText(accountUrl);
    setCopied(true);
  };

  return (
    <div className="relative flex h-screen flex-col items-center justify-center gap-4">
      <button
        onClick={handleCopyUrl}
        className="absolute right-4 top-4 rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white shadow hover:bg-blue-500 focus:outline-none focus:ring-2 focus:ring-blue-400 focus:ring-offset-2"
      >
        {copied ? "Copied!" : "Copy todo list URL"}
      </button>
      <TodoList id={me.$jazz.id} />
    </div>
  );
}
