import { Outlet, createFileRoute } from "@tanstack/react-router";
import { StandaloneConnectionProvider } from "#contexts/standalone-connection-context";

export const Route = createFileRoute("/conn")({
  component: ConnectionRoute,
});

function ConnectionRoute(): React.ReactElement {
  return (
    <StandaloneConnectionProvider>
      <Outlet />
    </StandaloneConnectionProvider>
  );
}
