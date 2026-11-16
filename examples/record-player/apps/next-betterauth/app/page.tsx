"use client";

import { AppShell } from "@astryxdesign/core/AppShell";
import { Button } from "@astryxdesign/core/Button";
import { TopNav, TopNavHeading } from "@astryxdesign/core/TopNav";
import { useJazzSession } from "jazz-tools/react";
import { authClient } from "../src/lib/auth-client";
import { JazzTheme } from "./jazz-theme";
import { RecordPlayerClient } from "./record-player-client";
import { RecordPlayerProvider } from "./record-player-provider";

export default function Home() {
  return (
    <JazzTheme>
      <RecordPlayerProvider
        frame={(content, signedIn) => (
          <AppShell
            height="auto"
            variant="section"
            contentPadding={4}
            topNav={
              <TopNav
                label="RecordPlayer"
                heading={<TopNavHeading heading="RecordPlayer" subheading="Jazz example" />}
                endContent={signedIn ? <SignOut /> : undefined}
              />
            }
          >
            <main className="rp-page">{content}</main>
          </AppShell>
        )}
      >
        <RecordPlayerClient />
      </RecordPlayerProvider>
    </JazzTheme>
  );
}

function SignOut() {
  // Signs out of Better Auth, then Jazz follows the backend's answer.
  const { refresh } = useJazzSession();
  async function signOut() {
    await authClient.signOut();
    await refresh();
  }
  return <Button label="Sign out" variant="ghost" size="sm" onClick={() => void signOut()} />;
}
