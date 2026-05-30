import { createFileRoute } from "@tanstack/react-router";

import { TableDataGrid } from "#data-explorer/TableDataGrid.tsx";
import { validateTableDataSearch } from "#data-explorer/tableSearchParams.ts";

export const Route = createFileRoute(
  "/conn/$connectionId/$branch/$schemaHash/data-explorer/$tableName/data",
)({
  validateSearch: validateTableDataSearch,
  component: TableDataRoute,
});

function TableDataRoute(): React.ReactElement {
  return <TableDataGrid />;
}
