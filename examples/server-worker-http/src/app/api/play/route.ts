import { jazzServerAccount } from "@/jazzServerAccount";
import { PlaySelection } from "@/schema";
import { serverApi } from "@/serverApi";
import { Group, JazzRequestError } from "jazz-tools";

export async function POST(request: Request) {
  const response = await serverApi.play.handle(
    request,
    jazzServerAccount.worker,
    async ({ game, selection }, madeBy) => {
      const isPlayer1 = game.player1.account.$jazz.id === madeBy.$jazz.id;
      const isPlayer2 = game.player2.account.$jazz.id === madeBy.$jazz.id;

      if (!isPlayer1 && !isPlayer2) {
        throw new JazzRequestError("You are not a player in this game", 400);
      }

      const group = Group.create({ owner: jazzServerAccount.worker });
      group.addMember(madeBy, "reader");

      if (isPlayer1 && game.player1.playSelection) {
        throw new JazzRequestError("You have already played", 400);
      } else if (isPlayer2 && game.player2.playSelection) {
        throw new JazzRequestError("You have already played", 400);
      }

      const playSelection = PlaySelection.create(
        { value: selection, group },
        group,
      );

      if (isPlayer1) {
        game.player1.$jazz.set("playSelection", playSelection);
      } else {
        game.player2.$jazz.set("playSelection", playSelection);
      }

      if (game.player1.playSelection && game.player2.playSelection) {
        game.$jazz.set(
          "outcome",
          determineOutcome(
            game.player1.playSelection.value,
            game.player2.playSelection.value,
          ),
        );

        // Reveal the play selections to the other player
        game.player1.playSelection.group.addMember(
          game.player2.account,
          "reader",
        );
        game.player2.playSelection.group.addMember(
          game.player1.account,
          "reader",
        );

        if (game.outcome === "player1") {
          game.$jazz.set("player1Score", game.player1Score + 1);
        } else if (game.outcome === "player2") {
          game.$jazz.set("player2Score", game.player2Score + 1);
        }
      }

      return {
        game,
        result: "success",
      };
    },
  );

  return response;
}

/**
 * Given a player selections, returns the winner of the current game.
 */
function determineOutcome(
  player1Choice: "rock" | "paper" | "scissors",
  player2Choice: "rock" | "paper" | "scissors",
) {
  if (player1Choice === player2Choice) {
    return "draw";
  } else if (
    (player1Choice === "rock" && player2Choice === "scissors") ||
    (player1Choice === "paper" && player2Choice === "rock") ||
    (player1Choice === "scissors" && player2Choice === "paper")
  ) {
    return "player1";
  } else {
    return "player2";
  }
}
