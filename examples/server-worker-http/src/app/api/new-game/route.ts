import { JazzRequestError } from "jazz-tools";
import { jazzServerAccount } from "@/jazzServerAccount";
import { createGameState } from "@/schema";
import { serverApi } from "@/serverApi";

export async function POST(request: Request) {
  const response = await serverApi.newGame.handle(
    request,
    jazzServerAccount.worker,
    async ({ game }, madeBy) => {
      const isPlayer1 = game.player1.account.id === madeBy.id;
      const isPlayer2 = game.player2.account.id === madeBy.id;

      if (!isPlayer1 && !isPlayer2) {
        throw new JazzRequestError("You are not a player in this game", 400);
      }

      if (game.outcome) {
        game.applyDiff(
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
