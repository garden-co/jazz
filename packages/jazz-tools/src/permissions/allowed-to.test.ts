import { describe, expect, it } from "vitest";
import * as s from "../schema-builders.js";
import { readablePermissions } from "./dsl.js";

const schema = s.defineSchema({
  groups: s.table({ name: s.string() }, { members: s.reverse("members", "group") }),
  members: s.table(
    { groupId: s.uuid(), accountId: s.uuid() },
    { group: s.rel("groups", "groupId") },
  ),
  profiles: s.table({ accountId: s.uuid() }),
});
const { definePermissions, exists, allowedTo, anyOf, not, session, from } = schema;

/** `profiles.read` built by `rule`, every other table allowing reads. */
function profilesRead(rule: (profile: any) => unknown) {
  return () =>
    definePermissions({
      groups: { read: true },
      members: { read: (m) => allowedTo.read(m.group) },
      profiles: { read: rule as never },
    });
}

const refusal =
  "jazz.permissions.json: tables.profiles.read: `allowedTo.read(…)` inside `exists` is not supported yet: " +
  "repeat the other table's read rule there instead (e.g. share it as a helper function)";

describe("allowedTo inside exists", () => {
  it("is refused in an exists' conditions, combined or not, naming the rule", () => {
    const rules = [
      (p: any) =>
        exists.members({ accountId: p.accountId }, ((m: any) => allowedTo.read(m.group)) as never),
      (p: any) =>
        exists.members(
          { accountId: p.accountId },
          allowedTo.read(schema.tables.members.groupId) as never,
        ),
      (p: any) =>
        exists.members({ accountId: p.accountId }, ((m: any) =>
          anyOf({ accountId: session.user }, not(allowedTo.read(m.group)))) as never),
    ];
    for (const rule of rules) {
      let error: Error | undefined;
      try {
        profilesRead(rule)();
      } catch (e) {
        error = e as Error;
      }
      // (The `at <file:line>` of the `allowedTo` call follows in an app's
      // schema.ts; frames inside jazz-tools, as this test is, are skipped.)
      expect(error?.message.split("\n")[0]).toBe(refusal);
    }
  });

  it("is refused in a hop's conditions, a relation's `.where` and a relation predicate", () => {
    const op = "`allowedTo.delete(…)` inside `exists` is not supported yet";
    const build = (rule: unknown) => () =>
      definePermissions({
        groups: { delete: rule as never },
        members: { delete: true },
        profiles: {},
      });
    expect(
      build(() =>
        exists.groups({}).hop("members", allowedTo.delete(schema.tables.members.groupId) as never),
      ),
    ).toThrow(op);
    expect(
      build((g: any) => g.members.where(allowedTo.delete(schema.tables.members.groupId) as never)),
    ).toThrow(op);
    expect(
      build(() =>
        exists(from.members("m").where(allowedTo.delete(schema.tables.members.groupId) as never)),
      ),
    ).toThrow(op);
  });

  it("is a rule's own, alone or combined", () => {
    const permissions = definePermissions({
      groups: { read: true },
      members: {
        read: (m) => anyOf({ accountId: session.user }, allowedTo.read(m.group)),
      },
      profiles: {
        // The workaround: repeat the other table's rule inside the exists.
        read: (p) => exists.members({ accountId: p.accountId }, { accountId: session.user }),
      },
    });
    expect(readablePermissions(permissions)!.tables).toEqual({
      groups: { read: "true" },
      members: { read: { anyOf: ["accountId = session.user", "allowedTo.read(group)"] } },
      profiles: {
        read: "exists members(accountId = this.accountId, accountId = session.user)",
      },
    });
  });
});

describe("an engine refusal of a policy", () => {
  it("is located at its rule in schema.ts, not at a table an exists in it reads", async () => {
    const { mkdtemp, writeFile } = await import("node:fs/promises");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const { ruleOfError, withTsLocation } = await import("../dev/ts-project.js");
    // What `jazz-tools generate` gets from the loader for a refused
    // `profiles.read` whose `exists` reads `members`.
    const message =
      "jazz.permissions.json: tables.profiles.read: core schema policies only support Eq/Ne SessionCmp operators yet";
    expect(ruleOfError(message)).toEqual({ table: "profiles", rule: "read" });
    const dir = await mkdtemp(join(tmpdir(), "jazz-rule-"));
    const permissionsFile = join(dir, "schema.ts");
    await writeFile(
      permissionsFile,
      [
        "export const permissions = definePermissions({",
        "  members: { read: true },",
        "  profiles: {",
        "    read: (p) => exists.members({ accountId: p.accountId }),",
        "  },",
        "});",
      ].join("\n"),
    );
    const located = await withTsLocation(new Error(message), { permissionsFile } as never);
    expect(located.message).toMatch(/\n {2}at .*schema\.ts:4 \(tables\.profiles\.read\)$/);
  });
});
