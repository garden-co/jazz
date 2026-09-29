"use client";

import { createContext, useContext } from "react";
import type { GrantRole, Member, Page, Workspace, WorkspaceRole } from "@/schema";
import { buildPageTree, pageAccess, type PageAccess, type PageTree } from "@/src/lib/tree";

export type WorkspaceState = {
  me: string;
  workspace: Workspace;
  role: WorkspaceRole | undefined;
  members: Member[];
  pages: Page[];
  tree: PageTree<Page>;
  grants: ReadonlyMap<string, GrantRole>;
  selectedPageId: string | null;
  openPage: (pageId: string | null) => void;
};

const WorkspaceContext = createContext<WorkspaceState | null>(null);
export const WorkspaceProvider = WorkspaceContext.Provider;

export function useWorkspace(): WorkspaceState {
  const state = useContext(WorkspaceContext);
  if (!state) throw new Error("useWorkspace must be used inside a workspace");
  return state;
}

export function useTree(pages: Page[]): PageTree<Page> {
  return buildPageTree(pages);
}

/** Edit, view or nothing on one page, derived the same way permissions.ts decides. */
export function usePageAccess(pageId: string): PageAccess {
  const { tree, role, grants } = useWorkspace();
  return pageAccess(tree, pageId, role, grants);
}

/** Owners and band members share pages; only owners manage the band itself. */
export function canShare(role: WorkspaceRole | undefined): boolean {
  return role === "owner" || role === "member";
}

/** Deleting or moving a page needs edit access from above it, like the policy. */
export function useCanRestructure(pageId: string): boolean {
  const { tree, role, grants } = useWorkspace();
  const parentId = tree.byId.get(pageId)?.parentId;
  if (role === "owner" || role === "member") return true;
  return (
    !!parentId && tree.byId.has(parentId) && pageAccess(tree, parentId, role, grants) === "edit"
  );
}

export const ROLE_LABELS: Record<WorkspaceRole, string> = {
  owner: "Owner",
  member: "Band member",
  viewer: "Crew (can view)",
  guest: "Guest",
};

export const GRANT_LABELS: Record<GrantRole, string> = {
  editor: "Can edit",
  viewer: "Can view",
};

export function memberName(members: Member[], account: string | null | undefined): string {
  if (!account) return "Unassigned";
  return members.find((member) => member.account === account)?.displayName ?? "Former member";
}
