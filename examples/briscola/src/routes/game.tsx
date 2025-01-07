import { Outlet, createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/game")({
  beforeLoad: async ({ context: { me } }) => {
    if (!me) {
      throw redirect({
        to: "/",
      });
    }
  },
});
