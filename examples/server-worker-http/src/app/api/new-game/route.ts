import { jazzServerAccount } from "@/jazzServerAccount";
import { createGameState } from "@/schema";
import { serverApi } from "@/serverApi";
import { JazzRequestError } from "jazz-tools";

export async function POST(request: Request) {
  const response = await serverApi.newGame.handle(
    request,
    jazzServerAccount.worker,
    async ({ game }, madeBy) => {
      const isPlayer1 = game.player1.account.$jazz.id === madeBy.$jazz.id;
      const isPlayer2 = game.player2.account.$jazz.id === madeBy.$jazz.id;

      if (!isPlayer1 && !isPlayer2) {
        throw new JazzRequestError("You are not a player in this game", 400);
      }

      if (game.outcome) {
        game.$jazz.applyDiff(
          createGameState({
            account1: game.player1.account,
            account2: game.player2.account,
            worker: jazzServerAccount.worker,
          }),
        );
      }

      return {
        game,
      };
    },
  );

  return response;
}
