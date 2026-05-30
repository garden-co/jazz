import { createContext, useContext } from "react";
import { render, type RenderResult } from "@testing-library/react";
import {
  Outlet,
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";

interface RenderWithRouterOptions {
  initialEntry?: string;
  routePath?: string;
}

const RoutedElementContext = createContext<React.ReactElement | null>(null);

function RoutedElement(): React.ReactElement | null {
  return useContext(RoutedElementContext);
}

function createTestRouter(routePath: string, initialEntry: string) {
  const rootRoute = createRootRoute({ component: () => <Outlet /> });
  const testRoute = createRoute({
    getParentRoute: () => rootRoute,
    path: routePath,
    component: RoutedElement,
  });
  return createRouter({
    routeTree: rootRoute.addChildren([testRoute]),
    history: createMemoryHistory({ initialEntries: [initialEntry] }),
  });
}

interface RouterHarnessProps {
  element: React.ReactElement;
  router: ReturnType<typeof createTestRouter>;
}

function RouterHarness({ element, router }: RouterHarnessProps): React.ReactElement {
  return (
    <RoutedElementContext.Provider value={element}>
      <RouterProvider router={router} />
    </RoutedElementContext.Provider>
  );
}

export function renderWithRouter(
  element: React.ReactElement,
  {
    initialEntry = "/conn/connection/main/schema/data-explorer/todos/data",
    routePath = "/conn/$connectionId/$branch/$schemaHash/data-explorer/$tableName/data",
  }: RenderWithRouterOptions = {},
): RenderResult {
  const router = createTestRouter(routePath, initialEntry);

  // TanStack Router resolves the initial match through RouterProvider effects.
  // Tests should wait for the first routed element with findBy* before using getBy* assertions.
  const result = render(<RouterHarness element={element} router={router} />);

  return {
    ...result,
    rerender: (nextElement: React.ReactNode) => {
      result.rerender(<RouterHarness element={<>{nextElement}</>} router={router} />);
    },
  };
}
