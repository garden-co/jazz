import { describe, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import type { SessionContext } from "../permissions/index.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";
import { loadWasmModuleForTest } from "../runtime/testing/wasm-runtime-test-utils.js";

describe.each(["native", "wasm"])("%s schema validation binding", (runtime) => {
  it("accepts a matching policy and rejects the same policy after its column is removed", async () => {
    const binding =
      runtime === "native" ? await import("jazz-napi") : await loadWasmModuleForTest();
    const app = s.defineApp({ notes: s.table({ owner: s.string() }, {}) });
    const permissions = s.definePermissions(app, ({ policy, session }) => {
      policy.notes.allowRead.where({ owner: session.user.identity.subject });
    });
    expect(() =>
      binding.validateSchema(
        encodeSchema(mergePermissionsIntoWasmSchema(app.wasmSchema, permissions)),
      ),
    ).not.toThrow();

    const wrongType = s.definePermissions(app, ({ policy, session }) => {
      policy.notes.allowRead.where({ owner: session.user });
    });
    expect(() =>
      binding.validateSchema(
        encodeSchema(mergePermissionsIntoWasmSchema(app.wasmSchema, wrongType)),
      ),
    ).toThrow("ClaimTypeMismatch");

    const changed = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
    expect(() =>
      binding.validateSchema(
        encodeSchema(mergePermissionsIntoWasmSchema(changed.wasmSchema, permissions)),
      ),
    ).toThrow("owner");
  });

  it("accepts reachable_via(...).seeded_by(...) seeded from session references", async () => {
    const binding =
      runtime === "native" ? await import("jazz-napi") : await loadWasmModuleForTest();
    const app = s.defineApp({
      projects: s.table({ label: s.string() }, {}),
      teams: s.table({ name: s.string() }, {}),
      resource_access_edges: s.table(
        { resource: s.uuid(), team: s.uuid() },
        { resourceRelation: s.rel("projects", "resource"), teamRelation: s.rel("teams", "team") },
      ),
      team_team_edges: s.table(
        { child_team: s.uuid(), parent_team: s.uuid() },
        {
          child_teamRelation: s.rel("teams", "child_team"),
          parent_teamRelation: s.rel("teams", "parent_team"),
        },
      ),
      user_team_edges: s.table(
        { user_id: s.uuid(), user_sub: s.string(), team: s.uuid() },
        { teamRelation: s.rel("teams", "team") },
      ),
    });
    const seededFrom = (userColumn: string, from: (session: SessionContext) => unknown) =>
      s.definePermissions(app, ({ policy, session }) => {
        policy.projects.allowRead.where(
          policy.exists(
            policy.projects
              .reachable_via(
                "resource_access_edges",
                "resource",
                "team",
                session.user,
                "team_team_edges",
                "child_team",
                "parent_team",
              )
              .seeded_by("user_team_edges", userColumn, from(session) as never, "team"),
          ),
        );
      });

    for (const permissions of [
      seededFrom("user_id", (session) => session.user.account),
      seededFrom("user_sub", (session) => session.claims["sub"]),
    ]) {
      expect(() =>
        binding.validateSchema(
          encodeSchema(mergePermissionsIntoWasmSchema(app.wasmSchema, permissions)),
        ),
      ).not.toThrow();
    }
    expect(() => seededFrom("user_sub", () => "sub")).toThrow(
      "seeded_by(...) takes a session reference",
    );
  });
});
