import type { MemberRole } from "@/schema";

export const ROLE_RANK: Record<MemberRole, number> = { viewer: 0, editor: 1, owner: 2 };

export const ROLE_LABELS: Record<MemberRole, string> = {
  owner: "Owner",
  editor: "Editor",
  viewer: "Viewer",
};

/** A member can hold several rows; the strongest one decides what they can do. */
export function strongestRole(roles: MemberRole[]): MemberRole | undefined {
  return [...roles].sort((a, b) => ROLE_RANK[b] - ROLE_RANK[a])[0];
}
