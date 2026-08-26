export type BootstrapPerson = { id: string; userId: string };
export type BootstrapOrganization = { id: string; slug: string };
export type BootstrapMembership = {
  id: string;
  organizationId: string;
  personId: string;
  userId: string;
  role: string;
};

export type PersonalBootstrapState = {
  people: BootstrapPerson[];
  organizations: BootstrapOrganization[];
  memberships: BootstrapMembership[];
};

export type PersonalBootstrapPlan = {
  person: BootstrapPerson | null;
  organization: BootstrapOrganization | null;
  membership: BootstrapMembership | null;
};

function only<T>(label: string, rows: T[]): T | null {
  if (rows.length > 1) {
    throw new Error(`Bootstrap refused ambiguous ${label}: found ${rows.length} durable rows`);
  }
  return rows[0] ?? null;
}

/**
 * Validate the durable state before choosing anything to reuse. In particular,
 * this never uses "first row wins" for identities supplied by an external
 * authentication system: a pre-existing duplicate must be repaired explicitly.
 */
export function planPersonalBootstrap(
  userId: string,
  slug: string,
  state: PersonalBootstrapState,
): PersonalBootstrapPlan {
  const person = only("person", state.people);
  const organization = only("personal organization", state.organizations);
  const membership = only("personal organization membership", state.memberships);

  if (!membership) return { person, organization, membership: null };
  if (!person || !organization) {
    throw new Error("Bootstrap found a personal membership without its person and organization");
  }
  if (
    membership.userId !== userId ||
    membership.personId !== person.id ||
    membership.organizationId !== organization.id ||
    membership.role !== "admin"
  ) {
    throw new Error("Bootstrap found an invalid personal organization membership");
  }
  return { person, organization, membership };
}
