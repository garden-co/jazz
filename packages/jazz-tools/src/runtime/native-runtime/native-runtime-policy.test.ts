import { describe, expect, it } from "vitest";
import { readFileSync, writeFileSync } from "node:fs";
import type { WasmSchema } from "../../drivers/types.js";
import { definePermissions } from "../../permissions/index.js";
import { mergePermissionsIntoWasmSchema } from "../../schema-permissions.js";
import { schema as s } from "../../index.js";
import { encodeSchema } from "./schema-codec.js";
import {
  readSchemaPolicyInherits,
  readSchemaSelectPolicyBranches,
  readSchemaSelectPolicyInherits,
  readSchemaSelectPolicyReachables,
} from "./native-runtime-policy.test-support.js";

describe("NativeRuntimeAdapter policy encoding", () => {
  it("serializes an uncorrelated public Exists policy as a bounded existence gate", () => {
    const app = s.defineApp({
      bands: s.table({ name: s.string() }),
      members: s.table({
        bandId: s.ref("bands"),
        userId: s.string(),
      }),
    });
    const permissions = definePermissions(app, ({ policy, session }) => {
      const isMember = policy.members.exists.where({ userId: session.user_id });
      policy.bands.allowRead.where(isMember);
    });

    const policy = readSchemaSelectPolicyBranches(
      encodeSchema(mergePermissionsIntoWasmSchema(app.wasmSchema, permissions)),
      "bands",
    );

    expect(policy).toEqual({
      table: "bands",
      filters: [],
      joins: [
        {
          table: "members",
          onColumn: "id",
          targetTag: 1,
          uncorrelated: true,
          sourceColumn: undefined,
          sourceLookup: undefined,
          filters: [
            {
              tag: 3,
              column: "userId",
              operand: { tag: 2, claim: "user_id" },
            },
          ],
          nestedJoins: [],
        },
      ],
      branches: [],
    });
  });

  it("serializes OR policy branches with Exists source id correlation", () => {
    const policy = readSchemaSelectPolicyBranches(
      encodeSchema({
        documents: {
          columns: [
            { name: "visibility", column_type: { type: "Text" }, nullable: false },
            { name: "title", column_type: { type: "Text" }, nullable: false },
          ],
          policies: {
            select: {
              using: {
                type: "Or",
                exprs: [
                  {
                    type: "Cmp",
                    column: "visibility",
                    op: "Eq",
                    value: { type: "Literal", value: { type: "Text", value: "public" } },
                  },
                  {
                    type: "Exists",
                    table: "document_members",
                    condition: {
                      type: "And",
                      exprs: [
                        {
                          type: "Cmp",
                          column: "document_id",
                          op: "Eq",
                          value: {
                            type: "SessionRef",
                            path: ["__jazz_outer_row", "id"],
                          },
                        },
                        {
                          type: "Cmp",
                          column: "role",
                          op: "Eq",
                          value: { type: "Literal", value: { type: "Text", value: "reader" } },
                        },
                      ],
                    },
                  },
                ],
              },
            },
          },
        },
        document_members: {
          columns: [
            { name: "document_id", column_type: { type: "Uuid" }, nullable: false },
            { name: "role", column_type: { type: "Text" }, nullable: false },
          ],
        },
      }),
      "documents",
    );

    expect(policy).toEqual({
      table: "documents",
      filters: [{ tag: 1, children: [] }],
      joins: [],
      branches: [
        {
          filters: [
            {
              tag: 3,
              column: "visibility",
              operand: { tag: 3, literalTag: 6, value: "public" },
            },
          ],
          joins: [],
        },
        {
          filters: [],
          joins: [
            {
              table: "document_members",
              onColumn: "document_id",
              targetTag: 0,
              sourceColumn: "id",
              sourceLookup: undefined,
              filters: [
                {
                  tag: 3,
                  column: "role",
                  operand: { tag: 3, literalTag: 6, value: "reader" },
                },
              ],
              nestedJoins: [],
            },
          ],
        },
      ],
    });
  });

  it("serializes same-table gather seeds as seeded reachable policies", () => {
    const reachables = readSchemaSelectPolicyReachables(
      encodeSchema({
        resources: {
          columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
          policies: {
            select: {
              using: {
                type: "ExistsRel",
                rel: {
                  Filter: {
                    input: {
                      Join: {
                        left: {
                          Gather: {
                            seed: {
                              Filter: {
                                input: { TableScan: { table: "teams" } },
                                predicate: {
                                  Cmp: {
                                    left: { scope: "teams", column: "identity_key" },
                                    op: "Eq",
                                    right: { SessionRef: ["userId"] },
                                  },
                                },
                              },
                            },
                            step: {
                              Project: {
                                input: {
                                  Join: {
                                    left: {
                                      Filter: {
                                        input: { TableScan: { table: "team_entries" } },
                                        predicate: {
                                          And: [
                                            {
                                              Cmp: {
                                                left: {
                                                  scope: "team_entries",
                                                  column: "member_id",
                                                },
                                                op: "Eq",
                                                right: { RowId: "Frontier" },
                                              },
                                            },
                                            {
                                              Cmp: {
                                                left: {
                                                  scope: "team_entries",
                                                  column: "administrator",
                                                },
                                                op: "Eq",
                                                right: {
                                                  Literal: { type: "Boolean", value: false },
                                                },
                                              },
                                            },
                                          ],
                                        },
                                      },
                                    },
                                    right: {
                                      TableScan: {
                                        table: "teams",
                                        alias: "__recursive_hop_0",
                                      },
                                    },
                                    on: [
                                      {
                                        left: { scope: "team_entries", column: "target_id" },
                                        right: { scope: "__recursive_hop_0", column: "id" },
                                      },
                                    ],
                                    join_kind: "Inner",
                                  },
                                },
                                columns: [],
                              },
                            },
                            frontier_key: { RowId: "Current" },
                            bound: { MaxDepth: 8 },
                            dedupe_key: [{ RowId: "Current" }],
                          },
                        },
                        right: { TableScan: { table: "resource_access", alias: "access" } },
                        on: [
                          {
                            left: { column: "id" },
                            right: { scope: "access", column: "team" },
                          },
                        ],
                        join_kind: "Inner",
                      },
                    },
                    predicate: {
                      And: [
                        {
                          Cmp: {
                            left: { scope: "access", column: "resource" },
                            op: "Eq",
                            right: { RowId: "Outer" },
                          },
                        },
                        {
                          Cmp: {
                            left: { scope: "access", column: "administrator" },
                            op: "Eq",
                            right: { Literal: { type: "Boolean", value: false } },
                          },
                        },
                      ],
                    },
                  },
                },
              },
            },
          },
        },
        teams: {
          columns: [{ name: "identity_key", column_type: { type: "Uuid" }, nullable: false }],
        },
        team_entries: {
          columns: [
            {
              name: "member_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "teams",
            },
            {
              name: "target_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "teams",
            },
            { name: "administrator", column_type: { type: "Boolean" }, nullable: false },
          ],
        },
        resource_access: {
          columns: [
            {
              name: "resource",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "resources",
            },
            {
              name: "team",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "teams",
            },
            { name: "administrator", column_type: { type: "Boolean" }, nullable: false },
          ],
        },
      }),
      "resources",
    );

    expect(reachables).toHaveLength(1);
    expect(reachables[0]).toMatchObject({
      accessTable: "resource_access",
      accessRowColumn: "resource",
      accessTeamColumn: "team",
      accessTeamTargetTag: 0,
      edgeTable: "team_entries",
      edgeMemberColumn: "member_id",
      edgeParentColumn: "target_id",
      maxDepth: 8,
      seed: {
        table: "teams",
        userColumn: "identity_key",
        userClaim: "user_id",
        teamColumn: "id",
        filters: [],
      },
    });
    expect(reachables[0]!.accessFilters).toEqual([
      {
        tag: 3,
        column: "administrator",
        operand: { tag: 3, literalTag: 5, value: false },
      },
    ]);
    expect(reachables[0]!.edgeFilters).toEqual([
      {
        tag: 3,
        column: "administrator",
        operand: { tag: 3, literalTag: 5, value: false },
      },
    ]);
  });

  it("serializes reachable_via seeded_by TS policies as the Rust reachable atom", () => {
    const baseSchema: WasmSchema = {
      resources: {
        columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
      },
      teams: {
        columns: [{ name: "identity_key", column_type: { type: "Text" }, nullable: false }],
      },
      team_team_edges: {
        columns: [
          {
            name: "child_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          {
            name: "parent_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
        ],
      },
      resource_access_edges: {
        columns: [
          {
            name: "resource",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "resources",
          },
          {
            name: "team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          { name: "grant_role", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const app = {
      wasmSchema: baseSchema,
      resources: { _rowType: {} as never, where: (_input: unknown) => undefined },
      teams: { _rowType: {} as never, where: (_input: unknown) => undefined },
      team_team_edges: { _rowType: {} as never, where: (_input: unknown) => undefined },
      resource_access_edges: { _rowType: {} as never, where: (_input: unknown) => undefined },
    };
    const permissions = definePermissions(app, ({ policy, session }) => {
      policy.resources.allowRead.where(
        policy.exists(
          policy.resources
            .reachable_via_with_access_filters(
              "resource_access_edges",
              "resource",
              "team",
              session.sub,
              { grant_role: "viewer" },
              "team_team_edges",
              "child_team",
              "parent_team",
            )
            .seeded_by("teams", "identity_key", "sub", "id"),
        ),
      );
    });
    const reachables = readSchemaSelectPolicyReachables(
      encodeSchema(mergePermissionsIntoWasmSchema(baseSchema, permissions)),
      "resources",
    );

    expect(reachables).toHaveLength(1);
    expect(reachables[0]).toMatchObject({
      accessTable: "resource_access_edges",
      accessRowColumn: "resource",
      accessTeamColumn: "team",
      accessTeamTargetTag: 0,
      edgeTable: "team_team_edges",
      edgeMemberColumn: "child_team",
      edgeParentColumn: "parent_team",
      maxDepth: 8,
      seed: {
        table: "teams",
        userColumn: "identity_key",
        userClaim: "sub",
        teamColumn: "id",
        filters: [],
      },
    });
    expect(reachables[0]!.accessFilters).toEqual([
      {
        tag: 3,
        column: "grant_role",
        operand: { tag: 3, literalTag: 6, value: "viewer" },
      },
    ]);
  });

  it("serializes allowedTo.read as a native inherits policy atom", () => {
    const baseSchema: WasmSchema = {
      resources: {
        columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
      },
      data_entries: {
        columns: [
          {
            name: "resource",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "resources",
          },
          { name: "label", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const app = {
      wasmSchema: baseSchema,
      resources: { _rowType: {} as never, where: (_input: unknown) => undefined },
      data_entries: { _rowType: {} as never, where: (_input: unknown) => undefined },
    };
    const permissions = definePermissions(app, ({ policy, allowedTo }) => {
      policy.resources.allowRead.where({ label: "visible" });
      policy.data_entries.allowRead.where(allowedTo.read("resource"));
    });

    const policy = readSchemaSelectPolicyInherits(
      encodeSchema(mergePermissionsIntoWasmSchema(baseSchema, permissions)),
      "data_entries",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "resource" }],
      joinCount: 0,
    });
  });

  it("serializes allowedTo insert, update, and delete as native inherits policy atoms", () => {
    const baseSchema: WasmSchema = {
      resources: {
        columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
      },
      data_entries: {
        columns: [
          {
            name: "resource",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "resources",
          },
          { name: "label", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const app = {
      wasmSchema: baseSchema,
      resources: { _rowType: {} as never, where: (_input: unknown) => undefined },
      data_entries: { _rowType: {} as never, where: (_input: unknown) => undefined },
    };
    const permissions = definePermissions(app, ({ policy, allowedTo }) => {
      policy.data_entries.allowInsert.where(allowedTo.insert("resource"));
      policy.data_entries.allowUpdate
        .whereOld(allowedTo.update("resource"))
        .whereNew(allowedTo.update("resource"));
      policy.data_entries.allowDelete.where(allowedTo.delete("resource"));
    });

    const encoded = encodeSchema(mergePermissionsIntoWasmSchema(baseSchema, permissions));

    expect(readSchemaPolicyInherits(encoded, "data_entries", "insert")).toEqual({
      inherits: [{ parentColumn: "resource" }],
      joinCount: 0,
    });
    expect(readSchemaPolicyInherits(encoded, "data_entries", "updateUsing")).toEqual({
      inherits: [{ parentColumn: "resource" }],
      joinCount: 0,
    });
    expect(readSchemaPolicyInherits(encoded, "data_entries", "updateCheck")).toEqual({
      inherits: [{ parentColumn: "resource" }],
      joinCount: 0,
    });
    expect(readSchemaPolicyInherits(encoded, "data_entries", "delete")).toEqual({
      inherits: [{ parentColumn: "resource" }],
      joinCount: 0,
    });
  });

  it("serializes authored inherited policies byte-identically to native schema atoms", () => {
    const baseSchema: WasmSchema = {
      resources: {
        columns: [{ name: "label", column_type: { type: "Text" }, nullable: false }],
        policies: {
          select: { using: { type: "True" } },
          insert: { with_check: { type: "True" } },
          update: { using: { type: "True" }, with_check: { type: "True" } },
          delete: { using: { type: "True" } },
        },
      },
      data_entries: {
        columns: [
          {
            name: "resource",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "resources",
          },
          { name: "label", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const app = {
      wasmSchema: baseSchema,
      resources: { _rowType: {} as never, where: (_input: unknown) => undefined },
      data_entries: { _rowType: {} as never, where: (_input: unknown) => undefined },
    };
    const permissions = definePermissions(app, ({ policy, allowedTo }) => {
      policy.data_entries.allowRead.where(allowedTo.read("resource"));
      policy.data_entries.allowInsert.where(allowedTo.insert("resource"));
      policy.data_entries.allowUpdate
        .whereOld(allowedTo.update("resource"))
        .whereNew(allowedTo.update("resource"));
      policy.data_entries.allowDelete.where(allowedTo.delete("resource"));
    });
    const nativeSchema: WasmSchema = {
      ...baseSchema,
      data_entries: {
        ...baseSchema.data_entries,
        policies: {
          select: {
            using: { type: "Inherits", operation: "Select", via_column: "resource" },
          },
          insert: {
            with_check: { type: "Inherits", operation: "Insert", via_column: "resource" },
          },
          update: {
            using: { type: "Inherits", operation: "Update", via_column: "resource" },
            with_check: { type: "Inherits", operation: "Update", via_column: "resource" },
          },
          delete: {
            using: { type: "Inherits", operation: "Delete", via_column: "resource" },
          },
        },
      },
    };

    expect(encodeSchema(mergePermissionsIntoWasmSchema(baseSchema, permissions))).toEqual(
      encodeSchema(nativeSchema),
    );
  });

  it("encodes the policy graph perf fixture byte-stably", () => {
    const fixtureDir = new URL("../../testing/fixtures/policy-graph-perf/", import.meta.url);
    const source = JSON.parse(readFileSync(new URL("schema-source.json", fixtureDir), "utf8")) as {
      mergedSchema: WasmSchema;
    };
    const expectedBytes = new Uint8Array(readFileSync(new URL("schema.native.bin", fixtureDir)));
    const encoded = encodeSchema(source.mergedSchema);

    if (process.env.JAZZ_UPDATE_POLICY_GRAPH_PERF_NATIVE_SCHEMA) {
      writeFileSync(new URL("schema.native.bin", fixtureDir), encoded);
      writeFileSync(
        new URL("schema.native.hex", fixtureDir),
        `${Buffer.from(encoded).toString("hex")}\n`,
      );
      return;
    }

    expect(encoded).toEqual(expectedBytes);
  });

  it("rejects ExistsRel Gather policies without a concrete MaxDepth bound", () => {
    expect(() =>
      encodeSchema({
        teams: {
          columns: [
            {
              name: "parent_id",
              column_type: { type: "Uuid" },
              nullable: true,
              references: "teams",
            },
          ],
          policies: {
            select: {
              using: {
                type: "ExistsRel",
                rel: {
                  Gather: {
                    seed: {
                      Project: {
                        input: {
                          Filter: {
                            input: {
                              Join: {
                                left: { TableScan: { table: "teams", alias: "edge" } },
                                right: { TableScan: { table: "teams", alias: "seed" } },
                                on: [
                                  {
                                    left: { scope: "edge", column: "parent_id" },
                                    right: { scope: "seed", column: "id" },
                                  },
                                ],
                                join_kind: "Inner",
                              },
                            },
                            predicate: {
                              Cmp: {
                                left: { scope: "seed", column: "parent_id" },
                                op: "Eq",
                                right: { SessionRef: ["teamId"] },
                              },
                            },
                          },
                        },
                        columns: [],
                      },
                    },
                    step: {
                      Project: {
                        input: {
                          Join: {
                            left: {
                              Filter: {
                                input: { TableScan: { table: "teams", alias: "edge" } },
                                predicate: {
                                  Cmp: {
                                    left: { scope: "edge", column: "id" },
                                    op: "Eq",
                                    right: { RowId: "Frontier" },
                                  },
                                },
                              },
                            },
                            right: { TableScan: { table: "teams", alias: "next" } },
                            on: [
                              {
                                left: { scope: "edge", column: "parent_id" },
                                right: { scope: "next", column: "id" },
                              },
                            ],
                            join_kind: "Inner",
                          },
                        },
                        columns: [],
                      },
                    },
                    frontier_key: { RowId: "Frontier" },
                    bound: "Fixpoint",
                    dedupe_key: [{ RowId: "Current" }],
                  },
                },
              } as never,
            },
          },
        },
      }),
    ).toThrow("MaxDepth");
  });

  it("serializes InheritsReferencing without a source operation policy as fail-closed", () => {
    const policy = readSchemaSelectPolicyBranches(
      encodeSchema({
        projects: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
          policies: {
            select: {
              using: {
                type: "InheritsReferencing",
                operation: "Select",
                source_table: "todos",
                via_column: "project_id",
              },
            },
          },
        },
        todos: {
          columns: [
            {
              name: "project_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "projects",
            },
            { name: "title", column_type: { type: "Text" }, nullable: false },
          ],
        },
      }),
      "projects",
    );

    expect(policy).toEqual({
      table: "projects",
      filters: [],
      joins: [
        {
          table: "todos",
          onColumn: "project_id",
          targetTag: 0,
          sourceColumn: undefined,
          sourceLookup: undefined,
          filters: [{ tag: 1, children: [] }],
          nestedJoins: [],
        },
      ],
      branches: [],
    });
  });

  it("serializes direct Inherits delete as a native inherits policy atom", () => {
    const policy = readSchemaPolicyInherits(
      encodeSchema({
        messages: {
          columns: [{ name: "room_id", column_type: { type: "Uuid" }, nullable: false }],
          policies: {
            delete: {
              using: {
                type: "Cmp",
                column: "room_id",
                op: "Eq",
                value: { type: "SessionRef", path: ["roomId"] },
              },
            },
          },
        },
        reactions: {
          columns: [
            {
              name: "message_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "messages",
            },
          ],
          policies: {
            delete: {
              using: {
                type: "Inherits",
                operation: "Delete",
                via_column: "message_id",
              },
            },
          },
        },
      }),
      "reactions",
      "delete",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "message_id" }],
      joinCount: 0,
    });
  });

  it("serializes a bounded Inherits depth as a native inherits policy atom", () => {
    const policy = readSchemaSelectPolicyInherits(
      encodeSchema({
        folders: {
          columns: [
            {
              name: "parent_id",
              column_type: { type: "Uuid" },
              nullable: true,
              references: "folders",
            },
          ],
          policies: {
            select: {
              using: {
                type: "Inherits",
                operation: "Select",
                via_column: "parent_id",
                max_depth: 3,
              },
            },
          },
        },
      }),
      "folders",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "parent_id", maxDepth: 3 }],
      joinCount: 0,
    });
  });

  it("serializes direct Inherits through parent exists joins with source lookup", () => {
    const policy = readSchemaSelectPolicyInherits(
      encodeSchema({
        chats: {
          columns: [{ name: "isPublic", column_type: { type: "Boolean" }, nullable: false }],
          policies: {
            select: {
              using: {
                type: "Exists",
                table: "chatMembers",
                condition: {
                  type: "And",
                  exprs: [
                    {
                      type: "Cmp",
                      column: "chatId",
                      op: "Eq",
                      value: { type: "OuterRowRef", column: "id" } as never,
                    },
                    {
                      type: "Cmp",
                      column: "userId",
                      op: "Eq",
                      value: { type: "SessionRef", path: ["user_id"] },
                    },
                  ],
                },
              },
            },
          },
        },
        chatMembers: {
          columns: [
            {
              name: "chatId",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "chats",
            },
            { name: "userId", column_type: { type: "Text" }, nullable: false },
          ],
        },
        canvases: {
          columns: [
            {
              name: "chatId",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "chats",
            },
          ],
          policies: {
            select: {
              using: {
                type: "Inherits",
                operation: "Select",
                via_column: "chatId",
              },
            },
          },
        },
      }),
      "canvases",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "chatId" }],
      joinCount: 0,
    });
  });

  it("serializes nested Inherits through composed source lookups", () => {
    const policy = readSchemaSelectPolicyInherits(
      encodeSchema({
        chats: {
          columns: [{ name: "isPublic", column_type: { type: "Boolean" }, nullable: false }],
          policies: {
            select: {
              using: {
                type: "Exists",
                table: "chatMembers",
                condition: {
                  type: "And",
                  exprs: [
                    {
                      type: "Cmp",
                      column: "chatId",
                      op: "Eq",
                      value: { type: "OuterRowRef", column: "id" } as never,
                    },
                    {
                      type: "Cmp",
                      column: "userId",
                      op: "Eq",
                      value: { type: "SessionRef", path: ["user_id"] },
                    },
                  ],
                },
              },
            },
          },
        },
        chatMembers: {
          columns: [
            {
              name: "chatId",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "chats",
            },
            { name: "userId", column_type: { type: "Text" }, nullable: false },
          ],
        },
        canvases: {
          columns: [
            {
              name: "chatId",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "chats",
            },
          ],
          policies: {
            select: {
              using: {
                type: "Inherits",
                operation: "Select",
                via_column: "chatId",
              },
            },
          },
        },
        strokes: {
          columns: [
            {
              name: "canvasId",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "canvases",
            },
          ],
          policies: {
            select: {
              using: {
                type: "Inherits",
                operation: "Select",
                via_column: "canvasId",
              },
            },
          },
        },
      }),
      "strokes",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "canvasId" }],
      joinCount: 0,
    });
  });
  it("serializes direct Inherits without a parent operation policy as a native inherits policy atom", () => {
    const policy = readSchemaPolicyInherits(
      encodeSchema({
        messages: {
          columns: [{ name: "body", column_type: { type: "Text" }, nullable: false }],
        },
        reactions: {
          columns: [
            {
              name: "message_id",
              column_type: { type: "Uuid" },
              nullable: false,
              references: "messages",
            },
          ],
          policies: {
            delete: {
              using: {
                type: "Inherits",
                operation: "Delete",
                via_column: "message_id",
              },
            },
          },
        },
      }),
      "reactions",
      "delete",
    );

    expect(policy).toEqual({
      inherits: [{ parentColumn: "message_id" }],
      joinCount: 0,
    });
  });
});
