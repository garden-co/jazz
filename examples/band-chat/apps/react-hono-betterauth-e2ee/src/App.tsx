import { useEffect, useRef, useState } from "react";
import { useDb, useJazzSession } from "jazz-tools/react";
import { Banner, Button, Center, Spinner } from "@astryxdesign/core";
import { JazzProvider, useBandChatLifecycle } from "@/components/jazz-provider";
import { SignInForm } from "@/components/sign-in-form";
import { ThemeProvider } from "@/components/theme-provider";
import { BandChat } from "./BandChat";
import { EncryptionNotices } from "./components/EncryptionNotices";
import { authClient } from "./lib/auth-client";
import { DeviceStateProvider, failureOf, useDeviceState } from "./lib/device-state";
import { clearPendingMarkerIds } from "./lib/mark-read";

/** Signed out: the sign-in form. Signed in: BandChat, synced through `/jazz`. */
export function App() {
  const { data: session, isPending } = authClient.useSession();
  return (
    <ThemeProvider>
      {isPending ? (
        <Center axis="both" padding={4} className="page-fill">
          <Spinner label="Loading BandChat…" />
        </Center>
      ) : session ? (
        <JazzProvider>
          <Dashboard name={session.user.name} />
        </JazzProvider>
      ) : (
        <SignInForm />
      )}
    </ThemeProvider>
  );
}

function Dashboard({ name }: { name: string }) {
  const { signOut } = useBandChatLifecycle();
  const [signOutFailure, setSignOutFailure] = useState<string | null>(null);
  const db = useDb();
  const accountId = useJazzSession().user?.accountId;
  const previousAccountId = useRef(accountId);
  useEffect(() => {
    if (previousAccountId.current !== accountId) clearPendingMarkerIds();
    previousAccountId.current = accountId;
  }, [accountId]);
  // An ordinary sign-out keeps this browser's keys and device record. It
  // also drops a wish to leave, so a later sign-in is not signed out again.
  async function finishMenuSignOut() {
    setSignOutFailure(null);
    try {
      await signOut();
    } catch (cause) {
      setSignOutFailure(failureOf(cause).message);
    }
  }
  const chat = (
    <>
      {signOutFailure ? (
        <Banner
          status="error"
          container="section"
          collapsible={false}
          title="Could not sign out"
          description={signOutFailure}
        />
      ) : null}
      <BandChat defaultDisplayName={name} onSignOut={() => void finishMenuSignOut()} />
    </>
  );
  if (!accountId) return chat;
  return (
    <DeviceStateProvider encryption={db.encryption} accountId={accountId}>
      <SignOutWhenLeft>{chat}</SignOutWhenLeft>
    </DeviceStateProvider>
  );
}

/**
 * Finishes leaving this browser. At start-up and whenever the device list
 * changes, the device state compares the recorded wish to leave with this
 * browser's device ID. A different ID for the same account means another
 * device removed this one and Jazz erased its keys: sign out, then drop the
 * record. A closed encryption runtime means the keys may still be here:
 * stay signed in and keep the record.
 */
function SignOutWhenLeft({ children }: { children: React.ReactNode }) {
  const { leaving } = useDeviceState();
  const { signOut } = useBandChatLifecycle();
  const [failure, setFailure] = useState<string | null>(null);
  const started = useRef(false);

  async function finish() {
    setFailure(null);
    try {
      await signOut();
    } catch (cause) {
      // The button tries again; the effect does not loop on it.
      setFailure(failureOf(cause).message);
      return;
    }
  }

  useEffect(() => {
    if (leaving !== "complete" || started.current) return;
    started.current = true;
    void finish();
  });

  return (
    <>
      <EncryptionNotices
        signOutFailure={
          failure ? (
            <Banner
              status="error"
              container="section"
              collapsible={false}
              title="This browser has left your account, but signing out failed"
              description={failure}
              endContent={<Button label="Finish signing out" onClick={() => void finish()} />}
            />
          ) : null
        }
      />
      {children}
    </>
  );
}
