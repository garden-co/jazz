import { Outlet, createFileRoute, useParams } from "@tanstack/react-router";

import { InspectorLayout } from "#inspector-layout/index";
import { appRoutes } from "#lib/navigation/appRoutes.ts";

export const Route = createFileRoute("/conn/$connectionId/$branch/$schemaHash/live-query")({
  component: LiveQueryLayoutRoute,
});

function LiveQueryLayoutRoute(): React.ReactElement {
  const routeParams = useParams({ from: appRoutes.liveQuery });

  return (
    <InspectorLayout routeParams={routeParams}>
      <Outlet />
    </InspectorLayout>
  );
}
