import { createFileRoute } from "@tanstack/react-router";

import { InspectorLayout } from "#inspector-layout/index";

export const Route = createFileRoute("/conn/$connectionId/$branch/$schemaHash/live-query")({
  component: LiveQueryLayoutRoute,
});

function LiveQueryLayoutRoute(): React.ReactElement {
  return <InspectorLayout />;
}
