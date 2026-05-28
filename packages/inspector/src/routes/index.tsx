import { createFileRoute, redirect } from "@tanstack/react-router";

import { appRoutes } from "#lib/navigation/appRoutes.ts";
import {
  redirectToDataExplorerTarget,
  resolveActiveStoredInspectorNavigationTarget,
} from "#lib/navigation/inspectorNavigation.ts";

export const Route = createFileRoute("/")({
  loader: async () => {
    const target = await resolveActiveStoredInspectorNavigationTarget();
    if (target !== null) {
      redirectToDataExplorerTarget(target);
    }

    throw redirect({ to: appRoutes.connections });
  },
});
