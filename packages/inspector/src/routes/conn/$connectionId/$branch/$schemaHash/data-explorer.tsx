import { createFileRoute } from "@tanstack/react-router";

import { InspectorLayout } from "#inspector-layout/index";

export const Route = createFileRoute("/conn/$connectionId/$branch/$schemaHash/data-explorer")({
  component: DataExplorerLayoutRoute,
});

function DataExplorerLayoutRoute(): React.ReactElement {
  return <InspectorLayout />;
}
