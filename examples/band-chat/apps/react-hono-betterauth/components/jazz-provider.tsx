import { JazzProvider as Jazz, betterAuth, useJazzAuth } from "jazz-tools/react";
import { Banner, Button, Center, Spinner } from "@astryxdesign/core";
import { authClient } from "@/src/lib/auth-client";

const APP_ID: string | undefined = import.meta.env.VITE_JAZZ_APP_ID;
// Better Auth's session drives sign-in; BandChat's backend (`createJazz` in
// server/jazz.ts) reads it in `authenticate`.
const auth = betterAuth(authClient);

type AuthActions = { signOut(): Promise<void> };
export function useBandChatLifecycle(): AuthActions {
  const { logout } = useJazzAuth();
  return {
    async signOut() {
      // Signs out of Jazz first (flushing local writes), then of Better Auth.
      await logout();
      window.location.assign("/");
    },
  };
}

export function JazzProvider({ children }: React.PropsWithChildren) {
  // Fail closed: vite.config.ts sets it from the checked configuration.
  if (!APP_ID)
    return (
      <StatusScreen>
        <Banner
          status="error"
          collapsible={false}
          title="BandChat is not configured"
          description="Set VITE_JAZZ_APP_ID."
        />
      </StatusScreen>
    );
  return (
    <Jazz
      appId={APP_ID}
      serverUrl="/jazz"
      auth={auth}
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
