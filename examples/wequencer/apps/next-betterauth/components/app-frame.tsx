"use client";

import type { ReactNode } from "react";
import { AppShell } from "@astryxdesign/core/AppShell";
import { DropdownMenu } from "@astryxdesign/core/DropdownMenu";
import { TopNav, TopNavHeading } from "@astryxdesign/core/TopNav";
import { authClient } from "@/lib/auth-client";
import { useGracefulSignOut } from "@/components/jazz-provider";

/** The signed-in frame: product name on the left, the account menu on the right. */
export function AppFrame({ children }: { children: ReactNode }) {
  const { data: session } = authClient.useSession();
  const gracefulSignOut = useGracefulSignOut();

  async function signOut() {
    try {
      await gracefulSignOut();
      window.location.assign("/");
    } catch {
      // The owner-held lifecycle reopens the selected client and renders the error.
    }
  }

  return (
    <AppShell
      height="auto"
      contentPadding={4}
      topNav={
        <TopNav
          heading={<TopNavHeading heading="Wequencer" headingHref="/dashboard" />}
          endContent={
            session ? (
              <DropdownMenu
                button={{ label: session.user.name, variant: "ghost" }}
                alignment="end"
                items={[{ label: "Sign out", onClick: () => void signOut() }]}
              />
            ) : null
          }
        />
      }
    >
      {children}
    </AppShell>
  );
}
