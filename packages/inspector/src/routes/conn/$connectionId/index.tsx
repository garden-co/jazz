import { createFileRoute } from "@tanstack/react-router";

import {
  redirectToConnections,
  redirectToDataExplorerTarget,
  resolveStoredInspectorNavigationTarget,
} from "#lib/navigation/inspectorNavigation.ts";

export const Route = createFileRoute("/conn/$connectionId/")({
  loader: async ({ params }) => {
    const target = await resolveStoredInspectorNavigationTarget({
      connectionId: params.connectionId,
    });
    if (target === null) {
      redirectToConnections();
    }

    redirectToDataExplorerTarget(target);
  },
});
