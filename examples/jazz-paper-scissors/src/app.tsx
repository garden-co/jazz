import { RouterProvider, createRouter } from "@tanstack/react-router";
import { useAccount } from "jazz-react";
import { routeTree } from "./routeTree.gen";

// Create a new router instance
const router = createRouter({
  routeTree,
  context: {
    // @ts-expect-error - just a placeholder - me is set in the App component down below
    me: undefined,
  },
  defaultPreload: "intent",
  scrollRestoration: true,
  defaultStructuralSharing: true,
  defaultPreloadStaleTime: 0,
});

// Register the router instance for type safety
declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

export function App() {
  const { me } = useAccount();

  if (!me) {
    return <div>Loading...</div>;
  }

  return <RouterProvider router={router} context={{ me }} />;
}
