import { useEffect, useMemo, useState } from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { AppShell } from "@astryxdesign/core/AppShell";
import { useToast } from "@astryxdesign/core";
import { app } from "../../schema.js";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import type { Me } from "../model/actions.js";
import { resumeDemoShow, setUpAccount } from "../model/seed.js";
import { MeProvider } from "../model/me.js";
import { useRoute, type Route } from "../router.js";
import { AppTopNav } from "./AppTopNav.js";
import { Checklist } from "./Checklist.js";
import { JoinShow } from "./JoinShow.js";
import { Loading } from "./Loading.js";
import { ShowList } from "./ShowList.js";
import { ShowPage } from "./ShowPage.js";

// One bootstrap per account, even when React mounts effects twice.
const bootstraps = new Map<string, ReturnType<typeof setUpAccount>>();

type Setup =
  | { status: "loading" }
  | { status: "ready" }
  | { status: "failed"; retry: () => void }
  | { status: "ready"; unsaved: true; retry: () => void };

/**
 * Makes sure the account has a crew profile; new accounts also get the demo
 * show. Everything applies locally, so the app opens offline too. If the
 * server later rejects part of the demo show, offer to finish it.
 */
function useSetup(route: Route): { me?: Me; setup: Setup } {
  const db = useDb();
  const account = useSession()?.user.account;
  const { data: profiles } = useAll(account ? app.crew.where({ account }) : undefined);
  const [setup, setSetup] = useState<Setup>({ status: "loading" });
  const [attempt, setAttempt] = useState(0);
  const arrivedByInvite = route.page === "join";

  useEffect(() => {
    if (!account) return;
    let bootstrap = bootstraps.get(account);
    if (!bootstrap) {
      bootstrap = setUpAccount(db, account, { withDemo: !arrivedByInvite });
      bootstraps.set(account, bootstrap);
    }
    let cancelled = false;
    const retry = () => {
      bootstraps.delete(account);
      setSetup({ status: "loading" });
      setAttempt((n) => n + 1);
    };
    bootstrap.then(
      async ({ me, isNew, writes }) => {
        if (cancelled) return;
        setSetup({ status: "ready" });
        try {
          // A returning account finishes a demo show that was only partly saved.
          const resumed = isNew || arrivedByInvite ? [] : await resumeDemoShow(db, me);
          await Promise.all([...writes, ...resumed].map((write) => write.wait({ tier: "global" })));
        } catch (error) {
          console.error("The demo show was not fully saved", error);
          if (!cancelled) {
            setSetup({
              status: "ready",
              unsaved: true,
              retry: () => {
                setSetup({ status: "ready" });
                void resumeDemoShow(db, me)
                  .then((resumedWrites) =>
                    Promise.all(resumedWrites.map((write) => write.wait({ tier: "global" }))),
                  )
                  .catch(retry);
              },
            });
          }
        }
      },
      (error) => {
        console.error("Could not set up the crew profile", error);
        bootstraps.delete(account);
        if (!cancelled) setSetup({ status: "failed", retry });
      },
    );
    return () => {
      cancelled = true;
    };
  }, [db, account, arrivedByInvite, attempt]);

  const profile = profiles?.[0];
  const isReady = setup.status === "ready";
  const me = useMemo(
    () => (isReady && account && profile ? { account, profile } : undefined),
    [isReady, account, profile],
  );
  return { me, setup };
}

/** Tells you when the server undid one of your changes, and why when it can. */
function useRejectedWriteToasts() {
  const db = useDb();
  const toast = useToast();
  useEffect(
    () =>
      db.onMutationError((event) => {
        const body =
          event.code === "permission_denied"
            ? "A change was undone: you don't have access to that any more."
            : "A change couldn't be saved and was undone.";
        toast({ type: "error", body });
      }),
    [db, toast],
  );
}

export function StagePlan() {
  const route = useRoute();
  const { me, setup } = useSetup(route);
  useRejectedWriteToasts();

  return (
    <AppShell height="auto" variant="section" topNav={<AppTopNav route={route} me={me} />}>
      {setup.status === "failed" ? (
        <EmptyState
          title="Your crew profile isn't ready"
          description="StagePlan couldn't set up your profile on this device."
          actions={<Button label="Try again" onClick={setup.retry} />}
        />
      ) : me ? (
        <MeProvider value={me}>
          {"unsaved" in setup && (
            <Banner
              status="warning"
              title="The demo show wasn't fully saved"
              description="Part of it didn't reach the server. You can try saving the rest again."
              endContent={<Button label="Try again" onClick={setup.retry} />}
            />
          )}
          <Page route={route} />
        </MeProvider>
      ) : (
        <Loading label="Getting your crew profile ready" />
      )}
    </AppShell>
  );
}

function Page({ route }: { route: Route }) {
  switch (route.page) {
    case "shows":
      return <ShowList />;
    case "checklist":
      return <Checklist />;
    case "join":
      return <JoinShow showId={route.showId} code={route.code} />;
    case "show":
      return <ShowPage showId={route.showId} tab={route.tab} taskId={route.taskId} />;
  }
}
