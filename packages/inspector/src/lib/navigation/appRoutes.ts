// Central route patterns used by typed navigation, redirects, and active route matching.
// It avoids duplicating raw path strings across consumers of these routes.

export const appRoutes = {
  connections: "/conn",
  newConnection: "/conn/new",
  connection: "/conn/$connectionId",
  branch: "/conn/$connectionId/$branch",
  schemaHash: "/conn/$connectionId/$branch/$schemaHash",
  dataExplorer: "/conn/$connectionId/$branch/$schemaHash/data-explorer",
  liveQuery: "/conn/$connectionId/$branch/$schemaHash/live-query",
  tableData: "/conn/$connectionId/$branch/$schemaHash/table/$tableName/data",
  tableSchema: "/conn/$connectionId/$branch/$schemaHash/table/$tableName/schema",
} as const;
