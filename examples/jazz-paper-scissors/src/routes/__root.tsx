import { Outlet, createRootRouteWithContext } from "@tanstack/react-router";
import { TanStackRouterDevtools } from "@tanstack/react-router-devtools";
import type { Account } from "jazz-tools";
import { JazzAndAuth } from "../components/ui/jazz-and-auth";

interface RouterContext {
  me: Account;
}

export const Route = createRootRouteWithContext<RouterContext>()({
  component: () => (
    <>
      <JazzAndAuth>
        <Outlet />
        <TanStackRouterDevtools />
      </JazzAndAuth>
    </>
  ),
});
