// Central route patterns used by typed navigation, redirects, and active route matching.
// Keep this in sync with the generated TanStack route tree.

export const appRoutes = {
  connections: "/conn",
  newConnection: "/conn/new",
  connection: "/conn/$connectionId",
  branch: "/conn/$connectionId/$branch",
  schemaHash: "/conn/$connectionId/$branch/$schemaHash",
  dataExplorer: "/conn/$connectionId/$branch/$schemaHash/data-explorer",
  liveQuery: "/conn/$connectionId/$branch/$schemaHash/live-query",
  tableData: "/conn/$connectionId/$branch/$schemaHash/data-explorer/$tableName/data",
  tableSchema: "/conn/$connectionId/$branch/$schemaHash/data-explorer/$tableName/schema",
} as const;
