import { createFileRoute } from "@tanstack/react-router";

import { TableDataGrid } from "#data-explorer/TableDataGrid.tsx";

export const Route = createFileRoute(
  "/conn/$connectionId/$branch/$schemaHash/data-explorer/$tableName/data",
)({
  component: TableDataRoute,
});

function TableDataRoute(): React.ReactElement {
  return <TableDataGrid />;
}
