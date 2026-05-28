import { createFileRoute, redirect } from "@tanstack/react-router";

import { appRoutes } from "#lib/navigation/appRoutes.ts";

export const Route = createFileRoute("/")({
  loader: () => {
    throw redirect({ to: appRoutes.connections });
  },
});
