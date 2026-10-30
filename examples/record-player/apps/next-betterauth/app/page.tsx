"use client";

import { AppShell } from "@astryxdesign/core/AppShell";
import { Button } from "@astryxdesign/core/Button";
import { TopNav, TopNavHeading } from "@astryxdesign/core/TopNav";
import { authClient } from "../src/lib/auth-client";
import { JazzTheme } from "./jazz-theme";
import { RecordPlayerClient } from "./record-player-client";
import { RecordPlayerProvider } from "./record-player-provider";

export default function Home() {
  return (
    <JazzTheme>
      <AppShell
        height="auto"
        variant="section"
        contentPadding={4}
        topNav={
          <TopNav
            label="RecordPlayer"
            heading={<TopNavHeading heading="RecordPlayer" subheading="Jazz example" />}
            endContent={<SignOut />}
          />
        }
      >
        <main className="rp-page">
          <RecordPlayerProvider>
            <RecordPlayerClient />
          </RecordPlayerProvider>
        </main>
      </AppShell>
    </JazzTheme>
  );
}

function SignOut() {
  const { data: session } = authClient.useSession();
  if (!session?.user) return null;
  return (
    <Button label="Sign out" variant="ghost" size="sm" onClick={() => void authClient.signOut()} />
  );
}
