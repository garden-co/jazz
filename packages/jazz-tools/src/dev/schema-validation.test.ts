import { describe, expect, it } from "vitest";
import { testApp, testPermissions } from "../testing/index.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import { encodeSchema } from "../runtime/native-runtime/schema-codec.js";
import { loadWasmModuleForTest } from "../runtime/testing/wasm-runtime-test-utils.js";

// Each policy is compiled (by Jazz's loader) against an app it fits, then
// handed to the binding with a schema it no longer fits: the binding's own
// check is what these tests cover.
describe.each(["native", "wasm"])("%s schema validation binding", (runtime) => {
  it("accepts a matching policy and rejects the same policy after its column is removed", async () => {
    const binding =
      runtime === "native" ? await import("jazz-napi") : await loadWasmModuleForTest();
    const app = testApp({ tables: { notes: { columns: { owner: "string" } } } });
    const permissions = testPermissions(app, {
      tables: { notes: { read: "owner = session.identity.subject" } },
    });
    expect(() =>
      binding.validateSchema(
        encodeSchema(mergePermissionsIntoWasmSchema(app.wasmSchema, permissions)),
      ),
    ).not.toThrow();

    // session.user compared with a string column: compiled where owner is a uuid.
    const uuidOwner = testApp({ tables: { notes: { columns: { owner: "uuid" } } } });
    const wrongType = testPermissions(uuidOwner, {
      tables: { notes: { read: "owner = session.user" } },
    });
    expect(() =>
      binding.validateSchema(
        encodeSchema(mergePermissionsIntoWasmSchema(app.wasmSchema, wrongType)),
      ),
    ).toThrow(
      "session.user has type uuid, but column owner has type string: session.user is the account's id, so store it in an s.uuid() column",
    );

    // An exists on boards' ids, compiled where members.boardId is a reference.
    const boardsTables = (relations: object) => ({
      tables: {
        boards: { columns: {} },
        members: { columns: { boardId: "uuid" }, relations },
      },
    });
    const linked = testApp(boardsTables({}));
    const withRelation = testApp(boardsTables({ board: { rel: "boards", column: "boardId" } }));
    const withoutRelation = testPermissions(withRelation, {
      tables: { members: { insert: "exists boards(id = this.boardId)" } },
    });
    expect(() =>
      binding.validateSchema(
        encodeSchema(mergePermissionsIntoWasmSchema(linked.wasmSchema, withoutRelation)),
      ),
    ).toThrow(
      'column members.boardId is matched against the ids of boards, but it is not declared as a reference: declare it on members, for example `<relation>: s.rel("boards", "boardId")`',
    );

    const changed = testApp({ tables: { notes: { columns: { title: "string" } } } });
    expect(() =>
      binding.validateSchema(
        encodeSchema(mergePermissionsIntoWasmSchema(changed.wasmSchema, permissions)),
      ),
    ).toThrow("owner");
  });

  it("accepts reachable_via(...).seeded_by(...) seeded from session references", async () => {
    const binding =
      runtime === "native" ? await import("jazz-napi") : await loadWasmModuleForTest();
    const app = testApp({
      tables: {
        projects: { columns: { label: "string" } },
        teams: { columns: { name: "string" } },
        resource_access_edges: {
          columns: { resource: "uuid", team: "uuid" },
          relations: {
            resourceRelation: { rel: "projects", column: "resource" },
            teamRelation: { rel: "teams", column: "team" },
          },
        },
        team_team_edges: {
          columns: { child_team: "uuid", parent_team: "uuid" },
          relations: {
            child_teamRelation: { rel: "teams", column: "child_team" },
            parent_teamRelation: { rel: "teams", column: "parent_team" },
          },
        },
        user_team_edges: {
          columns: { user_id: "uuid", user_sub: "string", team: "uuid" },
          relations: { teamRelation: { rel: "teams", column: "team" } },
        },
      },
    });
    // What the DSL's reachable_via(...).seeded_by(...) compiled to: teams
    // reachable from the session's seed rows, joined to the access edges.
    // The seed and step are parenthesised: a bare `select … as id,` would
    // read the next `step:` as another select column.
    const seededFrom = (seed: string) =>
      testPermissions(app, {
        tables: {
          projects: {
            read:
              "exists (from recursive(" +
              `seed: from (from user_team_edges where user_team_edges.${seed} select user_team_edges.team as id), ` +
              "step: from (from team_team_edges where team_team_edges.child_team = id(frontier) select team_team_edges.parent_team as id), " +
              "frontier: id(row), dedupe: (id(row)), maxDepth: 8) " +
              "join resource_access_edges as access on id = access.team " +
              "where access.resource = id(outer))",
          },
        },
      });

    for (const permissions of [
      seededFrom("user_id = session.user"),
      seededFrom("user_sub = session.claims.sub"),
    ]) {
      expect(() =>
        binding.validateSchema(
          encodeSchema(mergePermissionsIntoWasmSchema(app.wasmSchema, permissions)),
        ),
      ).not.toThrow();
    }
  });
});
