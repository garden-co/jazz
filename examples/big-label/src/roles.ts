/**
 * Organization roles and what each one may do. `permissions.ts` enforces these
 * rules at the Jazz edge; the UI reads the same table to hide or disable
 * controls a role cannot use. Hiding is a courtesy: the server still refuses.
 */
export const roles = ["admin", "editor", "viewer"] as const;
export type Role = (typeof roles)[number];

/** Roles that can change artists, releases and team release assignments. */
export const catalogueEditors: readonly Role[] = ["admin", "editor"];

export type Capability =
  | "readCatalogue"
  | "editCatalogue"
  | "assignReleases"
  | "deleteCatalogue"
  | "manageTeams"
  | "manageMembers"
  | "editSettings";

export const capabilities: { id: Capability; label: string; roles: readonly Role[] }[] = [
  { id: "readCatalogue", label: "See artists, releases, teams and people", roles },
  { id: "editCatalogue", label: "Add and edit artists and releases", roles: catalogueEditors },
  { id: "assignReleases", label: "Assign releases to teams", roles: catalogueEditors },
  { id: "deleteCatalogue", label: "Delete artists and releases", roles: ["admin"] },
  { id: "manageTeams", label: "Create teams and staff them", roles: ["admin"] },
  { id: "manageMembers", label: "Add members and change roles", roles: ["admin"] },
  { id: "editSettings", label: "Rename the label and edit catalogues", roles: ["admin"] },
];

export const roleLabels: Record<Role, string> = {
  admin: "Admin",
  editor: "Editor",
  viewer: "Viewer",
};

export const roleDescriptions: Record<Role, string> = {
  admin: "an admin",
  editor: "an editor",
  viewer: "a viewer",
};

export function isRole(value: string): value is Role {
  return (roles as readonly string[]).includes(value);
}

export function can(role: string | undefined, capability: Capability) {
  if (!role || !isRole(role)) return false;
  return capabilities.find((entry) => entry.id === capability)!.roles.includes(role);
}
