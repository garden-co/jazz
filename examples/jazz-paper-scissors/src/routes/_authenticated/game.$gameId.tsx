import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardSmall,
  CardTitle,
} from "@/components/ui/card";
import { WORKER_ID } from "@/constants";
import { Game, NewGameIntent, PlayIntent } from "@/schema";
import { createFileRoute, redirect } from "@tanstack/react-router";
import { experimental_useInboxSender, useCoState } from "jazz-react";
import { type ID } from "jazz-tools";
import { Badge, CircleHelp, Scissors, ScrollText } from "lucide-react";
import { useEffect, useState } from "react";

const playIcon = (selection: string | undefined) => {
  switch (selection) {
    case "rock":
      return <Badge className="w-5 h-5" />;
    case "paper":
      return <ScrollText className="w-5 h-5" />;
    case "scissors":
      return <Scissors className="w-5 h-5" />;
    default:
      return <CircleHelp className="w-5 h-5" />;
  }
};

export const Route = createFileRoute("/_authenticated/game/$gameId")({
  component: RouteComponent,
  loader: async ({ context: { me }, params: { gameId } }) => {
    const game = await Game.load(gameId as ID<Game>, {
      resolve: {
        player1: true,
        player2: true,
      },
    });
    if (!game) {
      throw redirect({ to: "/" });
    }

    return { gameId, me, loaderGame: game };
  },
});

function RouteComponent() {
  const { gameId, me, loaderGame } = Route.useLoaderData();

  const isPlayer1 = loaderGame.player1?.account?.isMe;
  const player = isPlayer1 ? "player1" : "player2";

  const [playSelection, setPlaySelection] = useState(
    loaderGame[player]?.playSelection ?? "",
  );
  const sendInboxMessage = experimental_useInboxSender(WORKER_ID);

  const game = useCoState(Game, gameId as ID<Game>);

  useEffect(() => {
    let gameCompleted = Boolean(loaderGame.outcome);

    return loaderGame.subscribe((game) => {
      if (gameCompleted && !game.outcome) {
        setPlaySelection(""); // Reset play selection when one player clicks on "Start a new game"
      }

      gameCompleted = Boolean(game.outcome);
    });
  }, []);

  if (!game) {
    return null;
  }

  const gameComplete = game.outcome !== undefined;

  const opponent = isPlayer1 ? "player2" : "player1";

  const currentPlayer = game[player];
  const opponentPlayer = game[opponent];

  const opponentSelection = opponentPlayer?.playSelection;

  const onSubmit = async (playSelection: string) => {
    sendInboxMessage(
      PlayIntent.create({ type: "play", gameId, player, playSelection }),
    );
  };

  const onNewGame = async () => {
    sendInboxMessage(NewGameIntent.create({ type: "newGame", gameId }));
  };

  return (
    <Card className="mx-auto max-w-5xl">
      <div className="mx-auto text-center">
        <CardHeader>
          <CardTitle>Jazz, Paper, Scissors!</CardTitle>
          <span>Welcome {isPlayer1 ? "Player 1" : "Player 2"}</span>
          <span>
            {game?.player1Score ?? 0} - {game?.player2Score ?? 0}
          </span>
        </CardHeader>
        {gameComplete ? (
          <>
            <div className="border">
              Game Over,{" "}
              {game?.outcome === player
                ? "You Win!"
                : game?.outcome === "draw"
                  ? "It's a Draw!"
                  : "You Lose!"}
            </div>
            <Button onClick={onNewGame}>Start a new game</Button>
          </>
        ) : null}
        <CardContent>
          <div>
            {playSelection === "" ? "Make Your Selection" : "Your Selection: "}
          </div>
          <CardSmall>{playIcon(playSelection)}</CardSmall>
          {gameComplete ? null : (
            <>
              <dl className="grid grid-cols-3 gap-x-8 gap-y-16 text-center">
                <Button
                  variant={"outline"}
                  size={"icon"}
                  onClick={() => setPlaySelection("rock")}
                >
                  <Badge className="w-5 h-5" />
                </Button>
                <Button
                  variant={"outline"}
                  size={"icon"}
                  onClick={() => setPlaySelection("paper")}
                >
                  <ScrollText className="w-5 h-5" />
                </Button>
                <Button
                  variant={"outline"}
                  size={"icon"}
                  onClick={() => setPlaySelection("scissors")}
                >
                  <Scissors className="w-5 h-5" />
                </Button>
              </dl>
              <div className="m-4">
                <Button
                  disabled={
                    playSelection === "" ||
                    Boolean(currentPlayer?.playSelection)
                  }
                  onClick={() => onSubmit(playSelection)}
                >
                  Go!
                </Button>
              </div>
            </>
          )}
          <div>Your Opponent Selected:</div>
          <CardSmall>{playIcon(opponentSelection)}</CardSmall>
        </CardContent>
      </div>
    </Card>
  );
}
