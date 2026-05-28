// Redirect to default section `conn/$connectionId/$branch/$schemaHash/data-explorer`

import { createFileRoute, redirect } from "@tanstack/react-router";

import { appRoutes } from "#lib/navigation/appRoutes.ts";

export const Route = createFileRoute("/conn/$connectionId/$branch/$schemaHash/")({
  loader: ({ params }) => {
    throw redirect({
      to: appRoutes.dataExplorer,
      params,
    });
  },
});
