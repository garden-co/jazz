import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/conn/$connectionId/")({
  component: ConnectionIndexRoute,
});

function ConnectionIndexRoute(): React.ReactElement | null {
  return null;
}
