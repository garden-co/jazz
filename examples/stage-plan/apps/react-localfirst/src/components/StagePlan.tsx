import { useEffect, useMemo, useState } from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { AppShell } from "@astryxdesign/core/AppShell";
import { useToast } from "@astryxdesign/core";
import { app } from "../../schema.js";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import type { Me } from "../model/actions.js";
import { resumeDemoShow, setUpAccount, type DemoShow } from "../model/seed.js";
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

/** The server rejected part of the demo show; the person can finish it. */
type UnsavedDemo = { isSaving: boolean; retry: () => void };

type Setup =
  | { status: "loading" }
  | { status: "failed"; retry: () => void }
  | { status: "ready"; unsavedDemo?: UnsavedDemo };

/**
 * Makes sure the account has a crew profile; new accounts also get the demo
 * show. Everything applies locally, so the app opens offline too. If the
 * server rejects part of the demo show, offer to finish it.
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

    // Waits until every demo write has settled. If the server rejected any,
    // show a banner whose retry writes only what's missing.
    const watchDemo = async (me: Me, demo: DemoShow) => {
      const results = await Promise.allSettled(
        demo.writes.map((write) => write.wait({ tier: "global" })),
      );
      if (cancelled) return;
      if (results.every((result) => result.status === "fulfilled")) {
        setSetup({ status: "ready" });
        return;
      }
      const retry = () => {
        setSetup({ status: "ready", unsavedDemo: { isSaving: true, retry } });
        resumeDemoShow(db, me, demo.showId).then(
          (next) => watchDemo(me, next),
          (error) => {
            console.error("Could not finish the demo show", error);
            if (!cancelled) setSetup({ status: "ready", unsavedDemo: { isSaving: false, retry } });
          },
        );
      };
      setSetup({ status: "ready", unsavedDemo: { isSaving: false, retry } });
    };

    bootstrap.then(
      ({ me, demo }) => {
        if (cancelled) return;
        setSetup({ status: "ready" });
        if (demo) void watchDemo(me, demo);
      },
      (error) => {
        console.error("Could not set up the crew profile", error);
        bootstraps.delete(account);
        if (!cancelled) {
          setSetup({
            status: "failed",
            retry: () => {
              setSetup({ status: "loading" });
              setAttempt((n) => n + 1);
            },
          });
        }
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

/** Tells you when the server undid one of your changes. */
function useRejectedWriteToasts() {
  const db = useDb();
  const toast = useToast();
  useEffect(
    () =>
      db.onMutationError((event) => {
        const body =
          event.code === "permission_denied"
            ? "A change was undone because the server didn't allow it."
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
          {setup.status === "ready" && setup.unsavedDemo && (
            <Banner
              status="warning"
              title="The demo show wasn't fully saved"
              description="The server didn't accept part of it. You can save what's missing."
              endContent={
                <Button
                  label={setup.unsavedDemo.isSaving ? "Saving" : "Save the rest"}
                  isDisabled={setup.unsavedDemo.isSaving}
                  onClick={setup.unsavedDemo.retry}
                />
              }
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
