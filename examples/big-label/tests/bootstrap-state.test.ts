import { describe, expect, it } from "vitest";
import { planPersonalBootstrap } from "../src/lib/bootstrap-state.js";

const userId = "external-user";
const slug = `personal-${userId}`;
const person = { id: "person", userId };
const organization = { id: "organization", slug };
const membership = {
  id: "membership",
  organizationId: organization.id,
  personId: person.id,
  userId,
  role: "admin",
};

describe("personal-organization bootstrap state", () => {
  it("reuses the committed triple after a competing exclusive transaction wins", () => {
    const initial = planPersonalBootstrap(userId, slug, {
      people: [],
      organizations: [],
      memberships: [],
    });
    expect(initial).toEqual({ person: null, organization: null, membership: null });

    // This is the snapshot seen when a losing transaction retries after the
    // winner's all-or-nothing person/org/admin-membership write is durable.
    expect(
      planPersonalBootstrap(userId, slug, {
        people: [person],
        organizations: [organization],
        memberships: [membership],
      }),
    ).toEqual({ person, organization, membership });
  });

  it("fails closed instead of arbitrarily choosing a durable duplicate", () => {
    expect(() =>
      planPersonalBootstrap(userId, slug, {
        people: [person, { id: "duplicate-person", userId }],
        organizations: [organization],
        memberships: [membership],
      }),
    ).toThrow(/ambiguous person/);
    expect(() =>
      planPersonalBootstrap(userId, slug, {
        people: [person],
        organizations: [organization, { id: "duplicate-org", slug }],
        memberships: [membership],
      }),
    ).toThrow(/ambiguous personal organization/);
    expect(() =>
      planPersonalBootstrap(userId, slug, {
        people: [person],
        organizations: [organization],
        memberships: [membership, { ...membership, id: "duplicate-membership" }],
      }),
    ).toThrow(/ambiguous personal organization membership/);
  });

  it("fails closed for a foreign or non-admin claimed personal membership", () => {
    expect(() =>
      planPersonalBootstrap(userId, slug, {
        people: [person],
        organizations: [organization],
        memberships: [{ ...membership, role: "editor" }],
      }),
    ).toThrow(/invalid personal organization membership/);
  });
});
