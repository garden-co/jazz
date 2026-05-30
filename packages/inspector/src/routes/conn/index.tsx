import { createFileRoute } from "@tanstack/react-router";

import { ConnectionManager } from "../../components/connection-manager/connectionManager";

export const Route = createFileRoute("/conn/")({
  component: ConnectionsIndexRoute,
});

function ConnectionsIndexRoute(): React.ReactElement {
  return <ConnectionManager />;
}
