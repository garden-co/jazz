"use client";

import { Banner, Button, Spinner } from "@astryxdesign/core";
import { useEffect, useState } from "react";
import { PosterShopApp } from "@/src/App";
import { bootstrapPersonalCanvas, joinCanvasWithInvite } from "@/src/lib/account-enrollment";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";

export default function Dashboard() {
  const { data: session, isPending } = authClient.useSession();
  const [bootstrap, setBootstrap] = useState<"loading" | "ready" | "failed">("loading");
  const [attempt, setAttempt] = useState(0);
  const [joinedCanvasId, setJoinedCanvasId] = useState<string | null>(null);
  useEffect(() => {
    if (!session) return;
    let cancelled = false;
    setBootstrap("loading");
    const invite = new URLSearchParams(window.location.search).get("join");
    void (async () => {
      const token = await getJwtFromBetterAuth();
      if (!token) return false;
      const bootstrapped = await bootstrapPersonalCanvas(token);
      if (!bootstrapped.ok) return false;
      if (invite) {
        const joined = await joinCanvasWithInvite(token, invite);
        if (!joined.ok) return false;
        const { canvasId } = (await joined.json()) as { canvasId: string };
        if (!cancelled) setJoinedCanvasId(canvasId);
        window.history.replaceState(null, "", "/dashboard");
      }
      return true;
    })()
      .catch(() => false)
      .then((ok) => {
        if (!cancelled) setBootstrap(ok ? "ready" : "failed");
      });
    return () => {
      cancelled = true;
    };
  }, [session?.user.id, attempt]);
  useEffect(() => {
    // Signed-out visitors (for example from an invite link) sign in first and
    // come back with the same query string.
    if (!isPending && !session) window.location.assign(`/${window.location.search}`);
  }, [isPending, session]);
  if (!session || bootstrap === "loading")
    return (
      <main className="centered-page">
        <Spinner label="Preparing your poster studio" />
      </main>
    );
  if (bootstrap === "failed")
    return (
      <main className="centered-page">
        <Banner
          status="error"
          title="Could not prepare your poster studio"
          description="The server did not finish setting up your first poster. Your work is safe; try again."
          endContent={<Button label="Try again" onClick={() => setAttempt(attempt + 1)} />}
        />
      </main>
    );
  return <PosterShopApp initialCanvasId={joinedCanvasId} />;
}
