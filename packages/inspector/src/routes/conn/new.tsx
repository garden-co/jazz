import { createFileRoute } from "@tanstack/react-router";
import { ConnectionFormPage } from "../../components/connection-manager/connectionFormPage";

export const Route = createFileRoute("/conn/new")({
  component: NewConnectionRoute,
});

function NewConnectionRoute(): React.ReactElement {
  return <ConnectionFormPage mode="connect" />;
}
