import { createFileRoute } from "@tanstack/react-router";

import { InspectorLayout } from "#inspector-layout/index";
import { DataExplorer } from "#pages/data-explorer/index.tsx";

export const Route = createFileRoute("/conn/$connectionId/$branch/$schemaHash/data-explorer")({
  component: DataExplorerLayoutRoute,
});

function DataExplorerLayoutRoute(): React.ReactElement {
  return (
    <InspectorLayout>
      <DataExplorer />
    </InspectorLayout>
  );
}
