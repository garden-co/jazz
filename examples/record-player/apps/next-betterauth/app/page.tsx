"use client";

import { AppShell } from "@astryxdesign/core/AppShell";
import { Button } from "@astryxdesign/core/Button";
import { TopNav, TopNavHeading } from "@astryxdesign/core/TopNav";
import { useJazzAuth } from "jazz-tools/react";
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
  // Signs out of Jazz first (flushing local writes), then of Better Auth.
  const { logout } = useJazzAuth();
  return <Button label="Sign out" variant="ghost" size="sm" onClick={() => void logout()} />;
}
