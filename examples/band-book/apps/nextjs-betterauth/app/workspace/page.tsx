"use client";

import { Suspense, useCallback, useEffect, useState } from "react";
import { BandBookApp } from "@/src/components/BandBookApp";
import { StatusScreen } from "@/components/status-screen";
import { useSession } from "jazz-tools/react";
import { ensureHomeWorkspace, rememberedHomeWorkspace } from "@/src/lib/home-workspace";

type Bootstrap =
  | { state: "pending" }
  | { state: "ready"; workspaceId: string }
  | { state: "failed"; message: string };

export default function WorkspacePage() {
  // The Jazz provider renders this page only once the account's client is ready.
  const account = useSession()?.user.account;
  const [bootstrap, setBootstrap] = useState<Bootstrap>(() => {
    const remembered = account ? rememberedHomeWorkspace(account) : null;
    return remembered ? { state: "ready", workspaceId: remembered } : { state: "pending" };
  });

  // The demo workspace is created by the server once per account. The app
  // renders from local data meanwhile; this only picks the home workspace and
  // reports a failure when there is nothing else to show.
  const run = useCallback((account: string) => {
    let cancelled = false;
    ensureHomeWorkspace(account).then(
      (workspaceId) => !cancelled && setBootstrap({ state: "ready", workspaceId }),
      (cause: unknown) =>
        !cancelled &&
        setBootstrap({
          state: "failed",
          message: cause instanceof Error ? cause.message : String(cause),
        }),
    );
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (account) return run(account);
  }, [account, run]);

  return (
    <Suspense fallback={<StatusScreen label="Opening BandBook" />}>
      <BandBookApp
        homeWorkspaceId={bootstrap.state === "ready" ? bootstrap.workspaceId : null}
        settingUp={bootstrap.state === "pending"}
        setupError={
          bootstrap.state === "failed" && account
            ? {
                message: bootstrap.message,
                retry: () => {
                  setBootstrap({ state: "pending" });
                  run(account);
                },
              }
            : null
        }
      />
    </Suspense>
  );
}
