import { useEffect } from "react";
import { JazzProvider as Jazz, useJazzSession } from "jazz-tools/react";
import { Banner, Button, Center, Spinner } from "@astryxdesign/core";
import { app } from "../jazz.generated";
import { authClient } from "@/src/lib/auth-client";
import { clearPendingMarkerIds } from "@/src/lib/mark-read";

// #region bandchat-provider-load
// Start on the thin client; load the local-first one (WASM, local storage,
// offline writes, and this browser's encryption keys) in its own chunk once
// the page has loaded. Rooms and other plain data show at once; encrypted
// message text waits for the local-first client.
const loadLocalFirst = () => import("jazz-tools/local-first");
// #endregion bandchat-provider-load

// Better Auth signs users in; BandChat's backend (`createJazz` in
// server/jazz.ts) reads its session in `authenticate`.
type AuthActions = { signOut(): Promise<void> };
export function useBandChatLifecycle(): AuthActions {
  const { refresh } = useJazzSession();
  return {
    async signOut() {
      // Signs out of Better Auth, then Jazz follows the backend's answer.
      await authClient.signOut();
      clearPendingMarkerIds();
      await refresh();
      window.location.assign("/");
    },
  };
}

// #region bandchat-provider
export function JazzProvider({ children }: React.PropsWithChildren) {
  return (
    <Jazz
      // `app` tells Jazz which columns are encrypted before the first query.
      config={{ serverUrl: "/jazz", client: loadLocalFirst, encryption: { app } }}
      loading={<LoadingScreen label="Connecting BandChat…" />}
      signedOut={<SignedOutScreen />}
      error={(state) => (
        <StatusScreen>
          <Banner
            status="error"
            collapsible={false}
            title="Could not connect BandChat"
            description={<span role="alert">{state.error?.message}</span>}
            endContent={<Button label="Retry connection" onClick={() => void state.retry()} />}
          />
        </StatusScreen>
      )}
    >
      {children}
    </Jazz>
  );
}
// #endregion bandchat-provider

/**
 * Jazz is signed out while the page still believes it is signed in: the
 * server ended this sign-in (another of the user's browsers removed a lost
 * device), or Jazz is between two sign-ins. Ask Better Auth again. With no
 * session the app shows the sign-in form; with one, Jazz signs in again on
 * its cookie.
 */
export function SignedOutScreen({ retryMs = 3000 }: { retryMs?: number }) {
  const { refetch } = authClient.useSession();
  const { refresh } = useJazzSession();
  useEffect(() => {
    let stopped = false;
    const check = async () => {
      const answer = await authClient.getSession().catch(() => null);
      // No answer (offline, a failed request): try again later.
      if (stopped || !answer || answer.error) return;
      const { data } = answer;
      // Updates the session the app reads: no session renders the sign-in form.
      if (data === null) await refetch();
      else await refresh().catch(() => {});
    };
    void check();
    // Until one of the two happens (a request can fail while offline).
    const timer = setInterval(() => void check(), retryMs);
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, [refetch, refresh, retryMs]);
  return <LoadingScreen label="Signing in…" />;
}

function StatusScreen({ children }: React.PropsWithChildren) {
  return (
    <Center axis="both" padding={4} className="page-fill">
      {children}
    </Center>
  );
}

function LoadingScreen({ label }: { label: string }) {
  return (
    <StatusScreen>
      <Spinner label={label} />
    </StatusScreen>
  );
}
