import { createRouter } from "@tanstack/react-router";

import { JazzAndAuth } from "./jazz";
import { routeTree } from "./routeTree.gen";

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

export const router = createRouter({
  routeTree,
  context: {
    me: undefined!,
  },
  Wrap: JazzAndAuth,
});
