import { jazzServerAccount } from "@/jazzServerAccount";
import { Game, createGameState } from "@/schema";
import { serverApi } from "@/serverApi";
import { Account, Group, JazzRequestError } from "jazz-tools";

export async function POST(request: Request) {
  return serverApi.joinGame.handle(
    request,
    jazzServerAccount.worker,
    async ({ waitingRoom }, madeBy) => {
      if (madeBy.$jazz.id === waitingRoom.creator.$jazz.id) {
        throw new JazzRequestError("You can't join your own waiting room", 400);
      }

      waitingRoom.$jazz.set(
        "game",
        createGame({
          account1: waitingRoom.creator,
          account2: madeBy,
          worker: jazzServerAccount.worker,
        }),
      );

      return {
        waitingRoom,
      };
    },
  );
}

interface CreateGameParams {
  account1: Account;
  account2: Account;
  worker: Account;
}

function createGame({ account1, account2, worker }: CreateGameParams) {
  const gameGroup = Group.create({ owner: worker });
  gameGroup.addMember(account1, "reader");
  gameGroup.addMember(account2, "reader");

  const game = Game.create(
    {
      ...createGameState({ account1, account2, worker }),
      player1Score: 0,
      player2Score: 0,
    },
    gameGroup,
  );

  return game;
}
