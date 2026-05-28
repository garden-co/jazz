import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/conn/$connectionId/$branch/")({
  component: BranchIndexRoute,
});

function BranchIndexRoute(): React.ReactElement | null {
  return null;
}
