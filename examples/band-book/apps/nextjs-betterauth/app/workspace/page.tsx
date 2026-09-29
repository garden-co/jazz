"use client";

import { Suspense, useCallback, useEffect, useState } from "react";
import { Button } from "@astryxdesign/core";
import { BandBookApp } from "@/src/components/BandBookApp";
import { StatusScreen } from "@/components/status-screen";
import { authClient, getJwtFromBetterAuth } from "@/src/lib/auth-client";
import { bootstrapWorkspace } from "@/src/lib/account-enrollment";

type Bootstrap =
  | { state: "loading" }
  | { state: "ready"; workspaceId: string }
  | { state: "failed"; message: string };

export default function WorkspacePage() {
  const { data: session, isPending } = authClient.useSession();
  const [bootstrap, setBootstrap] = useState<Bootstrap>({ state: "loading" });

  const run = useCallback(async () => {
    setBootstrap({ state: "loading" });
    try {
      const jwt = await getJwtFromBetterAuth();
      if (!jwt) throw new Error("Your session has no Jazz token. Sign in again.");
      const response = await bootstrapWorkspace(jwt);
      if (!response.ok) throw new Error(`The server answered ${response.status}.`);
      const { workspaceId } = (await response.json()) as { workspaceId: string };
      setBootstrap({ state: "ready", workspaceId });
    } catch (cause) {
      setBootstrap({
        state: "failed",
        message: cause instanceof Error ? cause.message : String(cause),
      });
    }
  }, []);

  useEffect(() => {
    if (!isPending && !session) window.location.assign("/");
    if (session) void run();
  }, [isPending, session?.user.id, run]);

  if (bootstrap.state === "failed")
    return (
      <StatusScreen
        label="Could not set up your band"
        error={bootstrap.message}
        action={<Button label="Try again" onClick={() => void run()} />}
      />
    );
  if (!session || bootstrap.state === "loading")
    return <StatusScreen label="Setting up your band" />;
  return (
    <Suspense fallback={<StatusScreen label="Opening BandBook" />}>
      <BandBookApp homeWorkspaceId={bootstrap.workspaceId} />
    </Suspense>
  );
}
