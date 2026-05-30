// Shared TanStack Router setup for standalone browser usage and extension memory routing.

import {
  RouterProvider,
  createMemoryHistory,
  createRouter,
  type RouterHistory,
} from "@tanstack/react-router";

import { routeTree } from "./routeTree.gen.ts";
import { appRoutes } from "#lib/navigation/appRoutes.ts";

interface CreateInspectorRouterOptions {
  history?: RouterHistory;
}

export function createInspectorRouter({ history }: CreateInspectorRouterOptions = {}) {
  return createRouter({
    routeTree,
    history,
  });
}

export type InspectorRouter = ReturnType<typeof createInspectorRouter>;

declare module "@tanstack/react-router" {
  interface Register {
    router: InspectorRouter;
  }
}

interface InspectorRouterProviderProps {
  router: InspectorRouter;
}

export function InspectorRouterProvider({ router }: InspectorRouterProviderProps) {
  return <RouterProvider router={router} />;
}

export function createInspectorMemoryRouter() {
  return createInspectorRouter({
    history: createMemoryHistory({
      initialEntries: [
        appRoutes.dataExplorer
          .replace("$connectionId", "extension")
          .replace("$branch", "runtime")
          .replace("$schemaHash", "registered"),
      ],
    }),
  });
}
