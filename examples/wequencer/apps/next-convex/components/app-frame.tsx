"use client";

import type { ReactNode } from "react";
import { AppShell } from "@astryxdesign/core/AppShell";
import { DropdownMenu } from "@astryxdesign/core/DropdownMenu";
import { TopNav, TopNavHeading } from "@astryxdesign/core/TopNav";
import { useQuery } from "convex/react";
import { api } from "@/convex/_generated/api";
import { useGracefulSignOut } from "@/components/convex-provider";

/** The signed-in frame: product name on the left, the account menu on the right. */
export function AppFrame({ children }: { children: ReactNode }) {
  const session = useQuery(api.users.viewer);

  return (
    <AppShell
      height="auto"
      contentPadding={4}
      topNav={
        <TopNav
          heading={<TopNavHeading heading="Wequencer" headingHref="/dashboard" />}
          endContent={session ? <AccountMenu name={session.name} /> : null}
        />
      }
    >
      {/* The pages need the signed-in account, which loads with the session. */}
      {session ? children : null}
    </AppShell>
  );
}

/** Only mounted once the account has loaded. */
function AccountMenu({ name }: { name: string }) {
  const gracefulSignOut = useGracefulSignOut();

  async function signOut() {
    try {
      await gracefulSignOut();
      window.location.assign("/");
    } catch {
      // Sign-out failed: the account stays open.
    }
  }

  return (
    <DropdownMenu
      button={{ label: name, variant: "ghost" }}
      alignment="end"
      items={[{ label: "Sign out", onClick: () => void signOut() }]}
    />
  );
}
