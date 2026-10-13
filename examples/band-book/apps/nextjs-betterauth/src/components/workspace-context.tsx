"use client";

import { createContext, useContext } from "react";
import type { GrantRole, Member, Page, Workspace, WorkspaceRole } from "@/schema";
import type { PageTree } from "@/src/lib/tree";

export type WorkspaceState = {
  me: string;
  workspace: Workspace;
  role: WorkspaceRole | undefined;
  members: Member[];
  pages: Page[];
  tree: PageTree<Page>;
  /**
   * Changes when the viewer's role or page grants change, which can change
   * any permission advice. Page placement is keyed per page (`pageAdviceKey`).
   */
  accessVersion: string;
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

/**
 * The part of a page's permission advice that depends on the tree: the page
 * and its chain of parents. It changes when the page or an ancestor moves,
 * not when unrelated pages are added or moved.
 */
export function pageAdviceKey(tree: PageTree<Page>, pageId: string): string {
  return [...tree.ancestors(pageId).map((page) => page.id), pageId].join("/");
}
