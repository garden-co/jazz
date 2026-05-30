import { createFileRoute } from "@tanstack/react-router";
import { ConnectionFormPage } from "../../../components/connection-manager/connectionFormPage";
import { useStandaloneConnection } from "#contexts/standalone-connection-context";

export const Route = createFileRoute("/conn/edit/$connectionId")({
  component: EditConnectionRoute,
});

function EditConnectionRoute(): React.ReactElement {
  const { connections } = useStandaloneConnection();
  const { connectionId } = Route.useParams();
  const connection = connections.find((storedConnection) => storedConnection.id === connectionId);

  return <ConnectionFormPage mode="edit" connection={connection} />;
}
