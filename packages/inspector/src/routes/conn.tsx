import { Outlet, createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/conn")({
  component: ConnectionRoute,
});

function ConnectionRoute(): React.ReactElement {
  return <Outlet />;
}
