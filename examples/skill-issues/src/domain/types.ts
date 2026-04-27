export type ItemKind = "idea" | "issue";
export type ItemStatus = "open" | "in_progress" | "done";

export interface IssueItem {
  kind: ItemKind;
  title: string;
  description: string;
  slug: string;
}

export interface ItemState {
  itemSlug: string;
  status: ItemStatus;
  assigneeUserId?: string;
}

export interface VerifiedUser {
  id: string;
  githubUserId: string;
  githubLogin: string;
  verifiedAt: string;
}
