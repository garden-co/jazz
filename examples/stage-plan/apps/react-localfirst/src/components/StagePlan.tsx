import { useEffect, useState } from "react";
import { useAll, useDb, useSession } from "jazz-tools/react";
import { AppShell } from "@astryxdesign/core/AppShell";
import { useToast } from "@astryxdesign/core";
import { app } from "../../schema.js";
import type { Me } from "../model/actions.js";
import { ensureProfile, seedDemoShow } from "../model/seed.js";
import { MeProvider } from "../model/me.js";
import { useRoute, type Route } from "../router.js";
import { AppTopNav } from "./AppTopNav.js";
import { Checklist } from "./Checklist.js";
import { JoinShow } from "./JoinShow.js";
import { Loading } from "./Loading.js";
import { ShowList } from "./ShowList.js";
import { ShowPage } from "./ShowPage.js";

// One bootstrap per account, even when React mounts effects twice.
const bootstraps = new Map<string, Promise<void>>();

/** Makes sure the account has a crew profile; new accounts also get the demo show. */
function useMe(route: Route): Me | undefined {
  const db = useDb();
  const account = useSession()?.user.account;
  const { data: profiles } = useAll(account ? app.crew.where({ account }) : undefined);
  const [ready, setReady] = useState(false);
  const arrivedByInvite = route.page === "join";

  useEffect(() => {
    if (!account) return;
    let bootstrap = bootstraps.get(account);
    if (!bootstrap) {
      bootstrap = ensureProfile(db, account).then(async ({ profile, isNew }) => {
        // Someone opening an invite link starts with that show, not the demo.
        if (isNew && !arrivedByInvite) await seedDemoShow(db, account, profile);
      });
      bootstraps.set(account, bootstrap);
      bootstrap.catch((error) => {
        console.error("Could not set up the crew profile", error);
        bootstraps.delete(account);
      });
    }
    let cancelled = false;
    void bootstrap.then(() => !cancelled && setReady(true));
    return () => {
      cancelled = true;
    };
  }, [db, account, arrivedByInvite]);

  const profile = profiles?.[0];
  return ready && account && profile ? { account, profile } : undefined;
}

/** Reports writes the server rejected, for example after a chief removed you from a show. */
function useRejectedWriteToasts() {
  const db = useDb();
  const toast = useToast();
  useEffect(
    () =>
      db.onMutationError(() => {
        toast({ type: "error", body: "A change was rejected: you no longer have access to it." });
      }),
    [db, toast],
  );
}

export function StagePlan() {
  const route = useRoute();
  const me = useMe(route);
  useRejectedWriteToasts();

  return (
    <AppShell height="auto" variant="section" topNav={<AppTopNav route={route} me={me} />}>
      {me ? (
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
