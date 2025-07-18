import { jazzServerAccount } from "@/jazzServerAccount";
import { Game, Player } from "@/schema";
import { serverApi } from "@/serverApi";
import { Account, Group, JazzRequestError } from "jazz-tools";

export async function POST(request: Request) {
  return serverApi.joinGame.handle(
    request,
    jazzServerAccount.worker,
    async ({ waitingRoom }, madeBy) => {
      if (madeBy.id === waitingRoom.creator.id) {
        throw new JazzRequestError("You can't join your own waiting room", 400);
      }

      waitingRoom.game = createGame({
        account1: waitingRoom.creator,
        account2: madeBy,
        worker: jazzServerAccount.worker,
      });

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

export function createGame({ account1, account2, worker }: CreateGameParams) {
  const publicReadOnly = Group.create({ owner: worker });
  publicReadOnly.addMember(account1, "reader");
  publicReadOnly.addMember(account2, "reader");

  const player1 = createPlayer({ account: account1, worker });
  const player2 = createPlayer({ account: account2, worker });

  const game = Game.create(
    {
      player1: player1,
      player2: player2,
      player1Score: 0,
      player2Score: 0,
    },
    { owner: publicReadOnly },
  );

  return game;
}

interface CreatePlayerParams {
  account: Account;
  worker: Account;
}

function createPlayer({ account, worker }: CreatePlayerParams) {
  const publicRead = Group.create({ owner: worker });
  publicRead.addMember("everyone", "reader");

  const player = Player.create(
    {
      account: account,
    },
    { owner: publicRead },
  );

  return player;
}
