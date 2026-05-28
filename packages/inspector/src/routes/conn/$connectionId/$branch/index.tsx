import { createFileRoute } from "@tanstack/react-router";

import {
  redirectToConnections,
  redirectToDataExplorerTarget,
  resolveStoredInspectorNavigationTarget,
} from "#lib/navigation/inspectorNavigation.ts";

export const Route = createFileRoute("/conn/$connectionId/$branch/")({
  loader: async ({ params }) => {
    const target = await resolveStoredInspectorNavigationTarget({
      connectionId: params.connectionId,
      branchOverride: params.branch,
    });
    if (target === null) {
      redirectToConnections();
    }

    redirectToDataExplorerTarget(target);
  },
});
