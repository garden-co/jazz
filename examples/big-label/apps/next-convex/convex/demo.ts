import { v } from "convex/values";
import type { Id } from "./_generated/dataModel";
import { mutation } from "./_generated/server";
import { requireUser } from "./lib/access";
import { createFixture } from "../src/fixtures";

/**
 * Loads a deterministic `createFixture(profile)` slice as new demo labels,
 * with the caller as the admin of each one (the Jazz app's POST
 * /api/demo-data). Like the bootstrap, this is the server's path: browsers
 * never create labels. Labels are keyed by a slug derived from the caller and
 * the fixture, so loading the same profile twice is a no-op. The fixture's
 * other members are synthetic accounts that can't sign in.
 */
export const load = mutation({
  args: { profile: v.union(v.literal("smoke"), v.literal("small")) },
  handler: async (ctx, { profile }) => {
    const userId = await requireUser(ctx);
    const fixture = createFixture(profile, 17);
    const caller = await ctx.db.query("people").withIndex("by_user", (q) => q.eq("userId", userId)).first();
    if (!caller) throw new Error("Bootstrap the personal label before loading demo data");
    const slug = (orgId: string) => `demo-${profile}-${userId}-${orgId}`;
    const existing = await Promise.all(
      fixture.organizations.map((org) =>
        ctx.db.query("organizations").withIndex("by_slug", (q) => q.eq("slug", slug(org.id))).first(),
      ),
    );
    if (existing[0]) return { created: false, organizationIds: existing.flatMap((org) => (org ? [org._id] : [])) };

    const ids = new Map<string, string>();
    const id = <T extends string>(key: string) => ids.get(key)! as Id<T & never>;
    for (const org of fixture.organizations) {
      const organizationId = await ctx.db.insert("organizations", { name: org.name, slug: slug(org.id) });
      ids.set(org.id, organizationId);
      await ctx.db.insert("memberships", { organizationId, personId: caller._id, userId, role: "admin" });
    }
    for (const person of fixture.people) {
      const personUser = await ctx.db.insert("users", { name: person.name });
      ids.set(person.userId, personUser);
      ids.set(person.id, await ctx.db.insert("people", { userId: personUser, name: person.name }));
    }
    for (const m of fixture.memberships) {
      ids.set(
        m.id,
        await ctx.db.insert("memberships", {
          organizationId: id<"organizations">(m.organizationId),
          personId: id<"people">(m.personId),
          userId: id<"users">(m.userId),
          role: m.role,
        }),
      );
    }
    for (const team of fixture.teams) {
      ids.set(team.id, await ctx.db.insert("teams", { organizationId: id(team.organizationId), name: team.name }));
    }
    for (const a of fixture.teamAssignments) {
      await ctx.db.insert("teamAssignments", {
        organizationId: id(a.organizationId),
        teamId: id(a.teamId),
        membershipId: id(a.membershipId),
        role: a.role,
      });
    }
    for (const c of fixture.catalogues) {
      ids.set(c.id, await ctx.db.insert("catalogues", { organizationId: id(c.organizationId), name: c.name, code: c.code }));
    }
    for (const { id: artistId, organizationId, ...artist } of fixture.artists) {
      ids.set(artistId, await ctx.db.insert("artists", { ...artist, organizationId: id(organizationId) }));
    }
    for (const { id: releaseId, organizationId, artistId, catalogueId, releaseDate, ...release } of fixture.releases) {
      ids.set(
        releaseId,
        await ctx.db.insert("releases", {
          ...release,
          organizationId: id(organizationId),
          artistId: id(artistId),
          catalogueId: id(catalogueId),
          releaseDate: Date.parse(releaseDate),
        }),
      );
    }
    for (const rt of fixture.releaseTeams) {
      await ctx.db.insert("releaseTeams", {
        organizationId: id(rt.organizationId),
        releaseId: id(rt.releaseId),
        teamId: id(rt.teamId),
      });
    }
    return { created: true, organizationIds: fixture.organizations.map((org) => ids.get(org.id)!) };
  },
});
