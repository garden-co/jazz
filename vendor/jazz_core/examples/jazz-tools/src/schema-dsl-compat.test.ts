import assert from "node:assert/strict";
import test from "node:test";
import { schema as s } from "./jazz-tools.js";

const gardenSchema = {
  projects: s.table({
    title: s.string(),
    description: s.string().optional(),
    createdAt: s.timestamp(),
  }),
  projectUserSettings: s
    .table({
      projectId: s.ref("projects"),
      userId: s.string().default("local-user"),
      authStatus: s.enum("none", "pending", "connected").default("none"),
      authPayloadMetadata: s.json().optional(),
      copiedFromProjectId: s.ref("projects").optional(),
      updatedAt: s.timestamp(),
    })
    .indexOnly(["projectId", "userId"]),
  conversations: s.table({
    projectId: s.ref("projects"),
    parentConversationId: s.ref("conversations").optional(),
    kind: s.enum(["note", "conversation"]),
    title: s.string(),
    excerpt: s.string().default(""),
  }),
};

type GardenSchema = s.Schema<typeof gardenSchema>;
const app: s.App<GardenSchema> = s.defineApp(gardenSchema);
type ProjectRow = s.RowOf<typeof app.projects>;

const _projectRow: ProjectRow = { id: "project-1" };

test("schema DSL column builders materialize to core WasmDb column definitions", () => {
  assert.equal(_projectRow.id, "project-1");
  assert.equal(app.projects._table, "projects");

  assert.deepEqual(gardenSchema.projects.columns, [
    { name: "title", column_type: "Text" },
    { name: "description", column_type: "Text", nullable: true },
    { name: "createdAt", column_type: "Text", timestamp: true },
  ]);

  assert.deepEqual(gardenSchema.projectUserSettings.indexed_columns, ["projectId", "userId"]);
  assert.deepEqual(gardenSchema.projectUserSettings.columns, [
    { name: "projectId", column_type: "Uuid", references: "projects" },
    { name: "userId", column_type: "Text", default: "local-user" },
    {
      name: "authStatus",
      column_type: "Text",
      default: "none",
      enum: ["none", "pending", "connected"],
    },
    { name: "authPayloadMetadata", column_type: "Text", nullable: true, json: true },
    { name: "copiedFromProjectId", column_type: "Uuid", nullable: true, references: "projects" },
    { name: "updatedAt", column_type: "Text", timestamp: true },
  ]);

  assert.deepEqual(gardenSchema.conversations.columns[1], {
    name: "parentConversationId",
    column_type: "Uuid",
    nullable: true,
    references: "conversations",
  });
  assert.deepEqual(gardenSchema.conversations.columns[2], {
    name: "kind",
    column_type: "Text",
    enum: ["note", "conversation"],
  });

  const columnIndexed = s.table({
    lookupKey: s.string().indexOnly(),
    value: s.string(),
  });
  assert.deepEqual(columnIndexed.indexed_columns, ["lookupKey"]);
});
