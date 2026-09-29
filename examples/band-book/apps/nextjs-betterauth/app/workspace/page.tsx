"use client";

import { Suspense, useCallback, useEffect, useState } from "react";
import { Button } from "@astryxdesign/core";
import { BandBookApp } from "@/src/components/BandBookApp";
import { StatusScreen } from "@/components/status-screen";
import { useSession } from "jazz-tools/react";
import { bootstrapWorkspace } from "@/src/lib/server-calls";

type Bootstrap =
  | { state: "loading" }
  | { state: "ready"; workspaceId: string }
  | { state: "failed"; message: string };

export default function WorkspacePage() {
  // The Jazz provider renders this page only once the account's client is ready.
  const account = useSession()?.user.account;
  const [bootstrap, setBootstrap] = useState<Bootstrap>({ state: "loading" });

  const run = useCallback(async () => {
    setBootstrap({ state: "loading" });
    try {
      const response = await bootstrapWorkspace();
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
    if (account) void run();
  }, [account, run]);

  if (bootstrap.state === "failed")
    return (
      <StatusScreen
        label="Could not set up your band"
        error={bootstrap.message}
        action={<Button label="Try again" onClick={() => void run()} />}
      />
    );
  if (bootstrap.state === "loading") return <StatusScreen label="Setting up your band" />;
  return (
    <Suspense fallback={<StatusScreen label="Opening BandBook" />}>
      <BandBookApp homeWorkspaceId={bootstrap.workspaceId} />
    </Suspense>
  );
}
