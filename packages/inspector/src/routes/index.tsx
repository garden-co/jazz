import { createFileRoute, redirect } from "@tanstack/react-router";

import { readFragmentConfig } from "#lib/config/connections.ts";
import { appRoutes } from "#lib/navigation/appRoutes.ts";
import {
  redirectToDataExplorerTarget,
  resolveActiveStoredInspectorNavigationTarget,
} from "#lib/navigation/inspectorNavigation.ts";

export const Route = createFileRoute("/")({
  loader: async () => {
    // Inspector links may carry connection prefill values in the URL hash.
    // Prefer the add-connection screen over opening the previously active local connection.
    if (readFragmentConfig() !== null) {
      throw redirect({ to: appRoutes.newConnection });
    }

    const target = await resolveActiveStoredInspectorNavigationTarget();
    if (target !== null) {
      redirectToDataExplorerTarget(target);
    }

    throw redirect({ to: appRoutes.connections });
  },
});
