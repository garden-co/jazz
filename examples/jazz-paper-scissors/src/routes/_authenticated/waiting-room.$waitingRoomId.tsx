import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { WORKER_ID } from "@/constants";
import { JoinGameRequest, WaitingRoom } from "@/schema";
import { createFileRoute, redirect } from "@tanstack/react-router";
import { Group, type ID, InboxSender } from "jazz-tools";
import { ClipboardCopyIcon, Loader2Icon } from "lucide-react";
import { useEffect, useState } from "react";

export const Route = createFileRoute(
  "/_authenticated/waiting-room/$waitingRoomId",
)({
  component: RouteComponent,
  loader: async ({ context: { me }, params: { waitingRoomId } }) => {
    const waitingRoom = await WaitingRoom.load(
      waitingRoomId as ID<WaitingRoom>,
      {
        resolve: {
          game: true,
        },
      },
    );

    if (!waitingRoom) {
      throw redirect({ to: "/" });
    }
    if (!waitingRoom?.account1?.isMe) {
      const sender = await InboxSender.load<JoinGameRequest, WaitingRoom>(
        WORKER_ID,
        me,
        // { account1: {}, account2: {}, me, game: {} },
      );
      sender.sendMessage(
        JoinGameRequest.create(
          { type: "joinGame", waitingRoom },
          { owner: Group.create({ owner: me }) },
        ),
      );
      // If the waiting room already has a game, redirect to the game
      if (waitingRoom?.game) {
        throw redirect({ to: `/game/${waitingRoom.game.id}` as string });
      }
    }

    return { waitingRoom };
  },
});

function RouteComponent() {
  const { waitingRoom } = Route.useLoaderData();
  const navigate = Route.useNavigate();
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    if (!waitingRoom) {
      return;
    }

    return waitingRoom.subscribe(
      {
        resolve: {
          game: true,
        },
      },
      async () => {
        if (waitingRoom.game) {
          navigate({ to: `/game/${waitingRoom.game.id}` });
        }
      },
    );
  }, [waitingRoom]);

  const onCopyClick = () => {
    navigator.clipboard.writeText(window.location.toString());
    setCopied(true);
  };

  return (
    <div className="h-screen flex flex-col w-full place-items-center justify-center p-2">
      <Card className="w-[500px]">
        <CardHeader>
          <CardTitle className="flex items-center">
            <Loader2Icon className="animate-spin inline h-8 w-8 mr-2" />
            Waiting for opponent to join the game
          </CardTitle>
          <CardDescription>
            Share this link with your friend to join the game. The game will
            automatically start once they join.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex">
            <Input
              className="w-full border bg-muted rounded-e-none"
              readOnly
              value={`${window.location}`}
            />
            <Button onClick={onCopyClick} className="rounded-s-none w-25">
              {copied ? (
                "Copied!"
              ) : (
                <>
                  <ClipboardCopyIcon className="w-5 h-5" />
                  Copy
                </>
              )}
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
