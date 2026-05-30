import { createFileRoute, useParams } from "@tanstack/react-router";

import { InspectorLayout } from "#inspector-layout/index";
import { appRoutes } from "#lib/navigation/appRoutes.ts";
import { DataExplorer } from "#pages/data-explorer/index.tsx";

export const Route = createFileRoute("/conn/$connectionId/$branch/$schemaHash/data-explorer")({
  component: DataExplorerLayoutRoute,
});

function DataExplorerLayoutRoute(): React.ReactElement {
  const routeParams = useParams({ from: appRoutes.dataExplorer });

  return (
    <InspectorLayout routeParams={routeParams}>
      <DataExplorer routeParams={routeParams} />
    </InspectorLayout>
  );
}
