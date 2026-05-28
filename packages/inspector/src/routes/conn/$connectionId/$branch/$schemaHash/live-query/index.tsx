import { createFileRoute } from "@tanstack/react-router";

import { LiveQuery } from "#pages/live-query/index.tsx";

export const Route = createFileRoute("/conn/$connectionId/$branch/$schemaHash/live-query/")({
  component: LiveQueryRoute,
});

function LiveQueryRoute(): React.ReactElement {
  return <LiveQuery />;
}
