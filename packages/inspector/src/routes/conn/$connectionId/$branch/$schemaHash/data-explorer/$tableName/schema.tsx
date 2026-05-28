import { createFileRoute } from "@tanstack/react-router";

import { TableSchemaDefinition } from "#data-explorer/TableSchemaDefinition.tsx";

export const Route = createFileRoute(
  "/conn/$connectionId/$branch/$schemaHash/data-explorer/$tableName/schema",
)({
  component: TableSchemaRoute,
});

function TableSchemaRoute(): React.ReactElement {
  return <TableSchemaDefinition />;
}
