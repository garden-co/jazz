import { schema as s, TypedTableQueryBuilder } from "../../../../src/index.js";

export const schema = {
  projects: s
    .table({
      name: s.string(),
    })
    .encryptionSpace(),
  documents: s.table({
    title: s.string().encrypted("projectId"),
    content: s.string().encrypted("projectId"),
    projectId: s.ref("projects"),
  }),
};

export type AppSchema = s.Schema<typeof schema>;
export const baseApp: s.App<AppSchema> = s.defineApp(schema);

// Use the wasmSchema from baseApp which already has encryptionSpace properly set
const wasmSchema = (baseApp as any).wasmSchema;

export const app: s.App<AppSchema> = {
  projects: new TypedTableQueryBuilder("projects", wasmSchema),
  documents: new TypedTableQueryBuilder("documents", wasmSchema),
  wasmSchema,
} as s.App<AppSchema>;

export type Project = s.RowOf<typeof app.projects>;
export type Document = s.RowOf<typeof app.documents>;
