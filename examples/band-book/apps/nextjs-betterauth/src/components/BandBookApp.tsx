"use client";

import { useCallback, useMemo } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useAll, useJazzAuth, useSession } from "jazz-tools/react";
import {
  AppShell,
  Avatar,
  DropdownMenu,
  EmptyState,
  Spinner,
  TopNav,
  TopNavHeading,
  VStack,
} from "@astryxdesign/core";
import { app } from "@/schema";
import { authClient } from "@/src/lib/auth-client";
import { buildPageTree } from "@/src/lib/tree";
import { PageScreen } from "./PageScreen";
import { Sidebar } from "./Sidebar";
import { WorkspaceProvider, type WorkspaceState } from "./workspace-context";

/**
 * The signed-in shell: workspace switcher and page tree on the side, the
 * selected page in the main area. The URL holds the selection (`?w=&p=`), so
 * a page can be linked to and survives a reload.
 */
export function BandBookApp({ homeWorkspaceId }: { homeWorkspaceId: string }) {
  const me = useSession()?.user.account ?? null;
  const { logout } = useJazzAuth();
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();
  const { data: authSession } = authClient.useSession();

  const { data: myMemberships } = useAll(me ? app.members.where({ account: me }) : undefined);
  const { data: workspaces } = useAll(app.workspaces.orderBy("name", "asc"));

  const requested = params.get("w");
  const workspace =
    workspaces?.find((candidate) => candidate.id === requested) ??
    workspaces?.find((candidate) => candidate.id === homeWorkspaceId) ??
    workspaces?.[0];
  const workspaceId = workspace?.id;

  const { data: members } = useAll(
    workspaceId ? app.members.where({ workspaceId }).orderBy("displayName", "asc") : undefined,
  );
  // Creation order is the sibling order in the sidebar.
  const { data: pages } = useAll(
    workspaceId
      ? app.pages.where({ workspaceId }).select("*", "$createdAt").orderBy("$createdAt", "asc")
      : undefined,
  );
  const { data: myGrants } = useAll(
    workspaceId && me ? app.pageGrants.where({ workspaceId, account: me }) : undefined,
  );

  const tree = useMemo(() => buildPageTree(pages ?? []), [pages]);
  const role = myMemberships?.find((member) => member.workspaceId === workspaceId)?.role;
  // Only what every piece of advice depends on: the viewer's role and grants.
  // Where a page sits in the tree is part of each page's own advice key.
  const accessVersion = useMemo(
    () =>
      [
        role ?? "none",
        ...(myGrants ?? []).map((grant) => `${grant.pageId}:${grant.role}`).sort(),
      ].join(","),
    [role, myGrants],
  );

  const selectedPageId = params.get("p");
  const openPage = useCallback(
    (pageId: string | null, nextWorkspaceId = workspaceId) => {
      const search = new URLSearchParams();
      if (nextWorkspaceId) search.set("w", nextWorkspaceId);
      if (pageId) search.set("p", pageId);
      router.push(`${pathname}?${search.toString()}`);
    },
    [pathname, router, workspaceId],
  );

  if (!me || !workspaces || !myMemberships)
    return (
      <VStack height="100dvh" justify="center" align="center">
        <Spinner size="lg" label="Loading your bands" />
      </VStack>
    );

  const userName = authSession?.user.name ?? "You";
  const topNav = (
    <TopNav
      label="BandBook"
      heading={<TopNavHeading heading="BandBook" headingHref="/workspace" />}
      endContent={
        <DropdownMenu
          button={{
            label: userName,
            variant: "ghost",
            icon: <Avatar name={userName} size="sm" tooltip={false} />,
          }}
          hasChevron={false}
          alignment="end"
          items={[
            {
              label: "Sign out",
              // Flushes Jazz, then revokes the Better Auth session.
              onClick: () => void logout(),
            },
          ]}
        />
      }
    />
  );

  if (!workspace)
    return (
      <AppShell topNav={topNav} contentPadding={4}>
        <EmptyState
          title="No band yet"
          description="Your demo band is being set up. Reload in a moment, or open an invite link from a bandmate."
        />
      </AppShell>
    );

  const state: WorkspaceState = {
    me,
    workspace,
    role,
    members: members ?? [],
    pages: (pages ?? []) as WorkspaceState["pages"],
    tree: tree as WorkspaceState["tree"],
    accessVersion,
    selectedPageId,
    openPage: (pageId) => openPage(pageId),
  };

  return (
    <WorkspaceProvider value={state}>
      <AppShell
        topNav={topNav}
        sideNav={
          <Sidebar
            workspaces={workspaces}
            memberships={myMemberships}
            onSwitchWorkspace={(id) => openPage(null, id)}
          />
        }
        contentPadding={0}
      >
        <PageScreen />
      </AppShell>
    </WorkspaceProvider>
  );
}
