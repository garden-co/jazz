import { createFileRoute } from "@tanstack/react-router";

import {
  redirectToDataExplorerTarget,
  resolveActiveStoredInspectorNavigationTarget,
} from "#lib/navigation/inspectorNavigation.ts";

export const Route = createFileRoute("/conn/")({
  loader: async () => {
    const target = await resolveActiveStoredInspectorNavigationTarget();
    if (target !== null) {
      redirectToDataExplorerTarget(target);
    }
  },
  component: ConnectionsIndexRoute,
});

function ConnectionsIndexRoute(): React.ReactElement | null {
  return null;
}
