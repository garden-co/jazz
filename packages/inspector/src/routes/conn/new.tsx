import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/conn/new")({
  component: NewConnectionRoute,
});

function NewConnectionRoute(): React.ReactElement | null {
  return null;
}
