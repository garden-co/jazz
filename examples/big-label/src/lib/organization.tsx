"use client";

import { createContext, useContext, type ReactNode } from "react";
import { can, type Capability, type Role } from "../roles";

export type CurrentOrganization = {
  id: string;
  name: string;
  membershipId: string;
  personId: string;
  role: Role;
};

const OrganizationContext = createContext<CurrentOrganization | null>(null);

export function OrganizationProvider({
  organization,
  children,
}: {
  organization: CurrentOrganization;
  children: ReactNode;
}) {
  return (
    <OrganizationContext.Provider value={organization}>{children}</OrganizationContext.Provider>
  );
}

/** The organization selected in the switcher, and the viewer's role in it. */
export function useOrganization() {
  const organization = useContext(OrganizationContext);
  if (!organization) throw new Error("useOrganization needs an OrganizationProvider");
  return organization;
}

/** Whether the viewer's role allows a capability. The server enforces the same rule. */
export function useCan(capability: Capability) {
  return can(useOrganization().role, capability);
}
