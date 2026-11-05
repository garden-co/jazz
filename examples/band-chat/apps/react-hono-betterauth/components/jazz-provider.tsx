import { JazzProvider as Jazz, useJazzAuth } from "jazz-tools/react";
import { Banner, Button, Center, Spinner } from "@astryxdesign/core";
import { authClient } from "@/src/lib/auth-client";

// Start on the thin client; load the local-first one (WASM, local storage,
// offline writes) in its own chunk once the page has loaded.
const loadLocalFirst = () => import("jazz-tools/local-first");

// Better Auth signs users in; BandChat's backend (`createJazz` in
// server/jazz.ts) reads its session in `authenticate`.
type AuthActions = { signOut(): Promise<void> };
export function useBandChatLifecycle(): AuthActions {
  const { reauthenticate } = useJazzAuth();
  return {
    async signOut() {
      // Signs out of Better Auth, then Jazz follows the backend's answer.
      await authClient.signOut();
      await reauthenticate();
      window.location.assign("/");
    },
  };
}

export function JazzProvider({ children }: React.PropsWithChildren) {
  return (
    <Jazz
      serverUrl="/jazz"
      client={loadLocalFirst}
      loading={<LoadingScreen label="Connecting BandChat…" />}
      signedOut={<LoadingScreen label="Signing in…" />}
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
