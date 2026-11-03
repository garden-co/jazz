"use client";

import { Suspense, useCallback, useEffect, useState } from "react";
import { BandBookApp } from "@/src/components/BandBookApp";
import { StatusScreen } from "@/components/status-screen";
import { useDb, useSession } from "jazz-tools/react";
import {
  confirmHomeWorkspace,
  ensureHomeWorkspace,
  rememberedHomeWorkspace,
} from "@/src/lib/home-workspace";

type Bootstrap =
  | { state: "pending" }
  | { state: "ready"; workspaceId: string }
  | { state: "failed"; message: string };

export default function WorkspacePage() {
  // The Jazz provider renders this page only once the account's client is ready.
  const session = useSession();
  const account = session?.user.account;
  const db = useDb();
  const [bootstrap, setBootstrap] = useState<Bootstrap>(() => {
    const remembered = account ? rememberedHomeWorkspace(account) : null;
    return remembered ? { state: "ready", workspaceId: remembered } : { state: "pending" };
  });

  // The demo workspace is created by the server once per account. The app
  // renders from local data meanwhile; this only picks the home workspace and
  // reports a failure when there is nothing else to show.
  const run = useCallback(
    (account: string) => {
      let cancelled = false;
      const fail = (cause: unknown) =>
        !cancelled &&
        setBootstrap({
          state: "failed",
          message: cause instanceof Error ? cause.message : String(cause),
        });
      const remembered = rememberedHomeWorkspace(account) !== null;
      ensureHomeWorkspace(account).then((workspaceId) => {
        if (cancelled) return;
        setBootstrap({ state: "ready", workspaceId });
        if (!remembered) return;
        // A remembered id renders at once; the server confirms it meanwhile.
        // If the workspace is gone (data reset, deleted), set it up again. A
        // failed check keeps the remembered id.
        confirmHomeWorkspace(db, account, workspaceId).then(
          (exists) => {
            if (cancelled || exists) return;
            setBootstrap({ state: "pending" });
            ensureHomeWorkspace(account).then(
              (fresh) => !cancelled && setBootstrap({ state: "ready", workspaceId: fresh }),
              fail,
            );
          },
          (cause: unknown) => console.warn("Could not check the home workspace", cause),
        );
      }, fail);
      return () => {
        cancelled = true;
      };
    },
    [db],
  );

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
