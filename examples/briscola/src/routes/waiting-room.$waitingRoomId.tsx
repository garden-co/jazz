import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { StartGameRequest, WaitingRoom } from "@/schema";
import { createFileRoute, redirect } from "@tanstack/react-router";
import { Group, type ID, InboxSender } from "jazz-tools";
import { useEffect } from "react";

export const Route = createFileRoute("/waiting-room/$waitingRoomId")({
  component: RouteComponent,
  loader: async ({ context: { me }, params: { waitingRoomId } }) => {
    if (!me) {
      throw redirect({ to: "/" });
    }

    const waitingRoom = await WaitingRoom.load(
      waitingRoomId as ID<WaitingRoom>,
      me,
      { account1: {}, account2: {}, game: {} },
    );

    if (waitingRoom?.game) {
      throw redirect({ to: `/game/${waitingRoom.game.id}` });
    }

    if (!waitingRoom?.account1?.isMe) {
      const sender = await InboxSender.load<StartGameRequest, WaitingRoom>(
        import.meta.env.VITE_JAZZ_WORKER_ACCOUNT,
        me,
      );

      sender.sendMessage(
        StartGameRequest.create(
          { waitingRoom },
          { owner: Group.create({ owner: me }) },
        ),
      );
    }
    return { waitingRoom };
  },
});

function RouteComponent() {
  const { waitingRoomId } = Route.useParams();
  const { waitingRoom } = Route.useLoaderData();
  const navigate = Route.useNavigate();

  useEffect(() => {
    if (!waitingRoom) {
      return;
    }

    return waitingRoom?.subscribe({ game: {} }, () => {
      if (waitingRoom.game) {
        navigate({ to: `/game/${waitingRoom.game.id}` });
      }
    });
  }, [waitingRoom]);

  return (
    <div className="h-screen flex flex-col w-full place-items-center justify-center p-2">
      <Card className="w-[500px]">
        <CardHeader>
          <CardTitle>Waiting for opponent to join the game</CardTitle>
          <CardDescription>
            Share this ID with your friend to join the game. The game will
            automatically start once they join.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <pre>{waitingRoomId}</pre>
        </CardContent>
      </Card>
    </div>
  );
}
