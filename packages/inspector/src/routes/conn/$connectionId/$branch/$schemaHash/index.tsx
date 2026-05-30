import { createFileRoute } from "@tanstack/react-router";

import { redirectToDataExplorerTarget } from "#lib/navigation/inspectorNavigation.ts";

export const Route = createFileRoute("/conn/$connectionId/$branch/$schemaHash/")({
  loader: ({ params }) => {
    redirectToDataExplorerTarget(params);
  },
});
