"use client";
import { PosterShopApp } from "@/src/App";
import { useEffect, useState } from "react";
import { authClient } from "@/src/lib/auth-client";
export default function Dashboard() {
  const { data: session } = authClient.useSession();
  const [bootstrap, setBootstrap] = useState<"loading" | "ready" | "failed">("loading");
  useEffect(() => {
    if (!session) return;
    let cancelled = false;
    void fetch("/api/bootstrap", { method: "POST", credentials: "same-origin" }).then(
      (response) => {
        if (!cancelled) setBootstrap(response.ok ? "ready" : "failed");
      },
    );
    return () => {
      cancelled = true;
    };
  }, [session?.user.id]);
  if (!session || bootstrap === "loading") return <main>Preparing your poster studio…</main>;
  if (bootstrap === "failed") return <main>Could not prepare your poster studio.</main>;
  return <PosterShopApp />;
}
