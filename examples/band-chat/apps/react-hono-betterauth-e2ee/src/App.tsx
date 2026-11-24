import { useEffect, useRef, useState } from "react";
import { useDb, useJazzSession } from "jazz-tools/react";
import { Banner, Button, Center, Spinner } from "@astryxdesign/core";
import { JazzProvider, useBandChatLifecycle } from "@/components/jazz-provider";
import { SignInForm } from "@/components/sign-in-form";
import { ThemeProvider } from "@/components/theme-provider";
import { BandChat } from "./BandChat";
import { EncryptionNotices } from "./components/EncryptionNotices";
import { authClient } from "./lib/auth-client";
import { DeviceStateProvider, useDeviceState } from "./lib/device-state";
import { clearLeaveRecord } from "./lib/leave-record";
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
  const db = useDb();
  const accountId = useJazzSession().user?.accountId;
  const previousAccountId = useRef(accountId);
  useEffect(() => {
    if (previousAccountId.current !== accountId) clearPendingMarkerIds();
    previousAccountId.current = accountId;
  }, [accountId]);
  // An ordinary sign-out keeps this browser's keys and device record. It
  // also drops a wish to leave, so a later sign-in is not signed out again.
  const onSignOut = () => {
    clearLeaveRecord();
    void signOut();
  };
  const chat = <BandChat defaultDisplayName={name} onSignOut={onSignOut} />;
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
  const { refresh } = useJazzSession();
  const [failure, setFailure] = useState<string | null>(null);
  const started = useRef(false);

  async function finish() {
    setFailure(null);
    try {
      const { error } = await authClient.signOut();
      if (error) throw new Error(error.message ?? error.statusText);
    } catch (cause) {
      // The button tries again; the effect does not loop on it.
      setFailure(cause instanceof Error ? cause.message : String(cause));
      return;
    }
    clearPendingMarkerIds();
    clearLeaveRecord();
    await refresh();
    window.location.assign("/");
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
