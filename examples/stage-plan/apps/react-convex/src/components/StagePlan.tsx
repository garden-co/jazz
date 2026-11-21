import { useEffect, useMemo, useRef, useState } from "react";
import { useConvexAuth, useMutation, useQuery } from "convex/react";
import { useAuthActions } from "@convex-dev/auth/react";
import { AppShell } from "@astryxdesign/core/AppShell";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { api } from "../../convex/_generated/api.js";
import type { Me } from "../model/actions.js";
import { MeProvider } from "../model/me.js";
import { useRoute, type Route } from "../router.js";
import { AppTopNav } from "./AppTopNav.js";
import { Checklist } from "./Checklist.js";
import { JoinShow } from "./JoinShow.js";
import { Loading } from "./Loading.js";
import { ShowList } from "./ShowList.js";
import { ShowPage } from "./ShowPage.js";

type Setup = { status: "loading" } | { status: "failed"; retry: () => void } | { status: "ready" };

/**
 * Signs the browser in as an anonymous guest, then makes sure the account
 * has a crew profile; new accounts also get the demo show, in the same
 * mutation, unless they arrived through an invite link.
 */
function useSetup(route: Route): { me?: Me; setup: Setup } {
  const { isAuthenticated, isLoading } = useConvexAuth();
  const { signIn } = useAuthActions();
  const profile = useQuery(api.crew.me, isAuthenticated ? {} : "skip");
  const setUpAccount = useMutation(api.crew.setUp);
  const [setup, setSetup] = useState<Setup>({ status: "loading" });
  const [attempt, setAttempt] = useState(0);
  const arrivedByInvite = route.page === "join";

  const signingIn = useRef(false);
  useEffect(() => {
    if (isLoading || isAuthenticated || signingIn.current) return;
    signingIn.current = true;
    void signIn("anonymous").finally(() => (signingIn.current = false));
  }, [isLoading, isAuthenticated, signIn]);

  const isLoaded = profile !== undefined;
  const needsProfile = profile === null;
  useEffect(() => {
    if (!isLoaded) return;
    if (!needsProfile) {
      setSetup({ status: "ready" });
      return;
    }
    setUpAccount({ withDemo: !arrivedByInvite }).then(
      () => setSetup({ status: "ready" }),
      (error) => {
        console.error("Could not set up the crew profile", error);
        setSetup({
          status: "failed",
          retry: () => {
            setSetup({ status: "loading" });
            setAttempt((n) => n + 1);
          },
        });
      },
    );
  }, [isLoaded, needsProfile, setUpAccount, attempt]);

  const isReady = setup.status === "ready";
  const me = useMemo(
    () => (isReady && profile ? { account: profile.account, profile } : undefined),
    [isReady, profile],
  );
  return { me, setup };
}

export function StagePlan() {
  const route = useRoute();
  const { me, setup } = useSetup(route);

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
