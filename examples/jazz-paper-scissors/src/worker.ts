import {
  Game,
  InboxMessage,
  NewGameIntent,
  PlayIntent,
  Player,
  WaitingRoom,
} from "@/schema";
import { startWorker } from "jazz-nodejs";
import { Account, Group, co } from "jazz-tools";
import { determineWinner } from "./lib/utils";

if (!process.env.VITE_JAZZ_WORKER_ACCOUNT || !process.env.JAZZ_WORKER_SECRET) {
  throw new Error(".env missing, run `pnpm generate-env`");
}

const {
  worker,
  experimental: { inbox },
} = await startWorker({
  accountID: process.env.VITE_JAZZ_WORKER_ACCOUNT,
  syncServer: "wss://cloud.jazz.tools/?key=jazz-paper-scissors@garden.co ",
});

inbox.subscribe(
  InboxMessage,
  async (message, senderID) => {
    const playerAccount = await co.account().load(senderID, { loadAs: worker });
    if (!playerAccount) {
      return;
    }

    switch (message.type) {
      case "play":
        handlePlayIntent(senderID, message);
        break;

      case "newGame":
        handleNewGameIntent(senderID, message);
        break;

      case "createGame":
        const waitingRoomGroup = Group.create({ owner: worker });
        waitingRoomGroup.addMember("everyone", "reader");
        const waitingRoom = WaitingRoom.create(
          { account1: playerAccount },
          { owner: waitingRoomGroup },
        );

        console.log("waiting room created with id:", waitingRoom.id);
        return waitingRoom;

      case "joinGame":
        const joinGameRequest = message;
        if (
          !joinGameRequest.waitingRoom ||
          !joinGameRequest.waitingRoom.account1
        ) {
          console.error("No waiting room in join game request");
          return;
        }

        // @ts-expect-error - https://github.com/garden-co/jazz/issues/1332
        joinGameRequest.waitingRoom.account2 = playerAccount;

        const game = await createGame({
          account1: joinGameRequest.waitingRoom.account1,
          account2: joinGameRequest.waitingRoom.account2!,
        });
        console.log("game created with id:", game.id);

        joinGameRequest.waitingRoom.game = game;
        return joinGameRequest.waitingRoom.game;
    }
  },
  { retries: 3 },
);

console.log("worker", worker.id, "started");

interface CreateGameParams {
  account1: Account;
  account2: Account;
}

async function createGame({ account1, account2 }: CreateGameParams) {
  const publicReadOnly = Group.create({ owner: worker });
  publicReadOnly.addMember(account1, "reader");
  publicReadOnly.addMember(account2, "reader");

  const player1 = createPlayer({ account: account1 });
  const player2 = createPlayer({ account: account2 });

  const game = Game.create(
    {
      player1: player1,
      player2: player2,
      player1Score: 0,
      player2Score: 0,
    },
    { owner: publicReadOnly },
  );

  await game.waitForSync();

  return game;
}

interface CreatePlayerParams {
  account: Account;
}

function createPlayer({ account }: CreatePlayerParams) {
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

async function handleNewGameIntent(_: string, message: NewGameIntent) {
  const gameId = message.gameId;

  const game = await Game.load(gameId, {
    loadAs: worker,
    resolve: {
      player1: true,
      player2: true,
    },
  });

  if (!game) {
    throw new Error("Game not found");
  }

  if (game.outcome) {
    game.outcome = undefined;
    game.player1.playSelection = undefined;

    if (game.player2) {
      game.player2.playSelection = undefined;
    }
  }
}

async function handlePlayIntent(_: string, message: PlayIntent) {
  // determine current player, update game with outcome
  const gameId = message.gameId;
  if (!gameId) {
    console.error("Game not found");
    return;
  }

  const game = await Game.load(gameId, {
    loadAs: worker,
    resolve: {
      player1: true,
      player2: true,
    },
  });

  if (!game) {
    throw new Error("Game not found");
  }

  const currentPlayer = game[message.player as "player1" | "player2"];

  if (!currentPlayer) {
    throw new Error("Player not found");
  }

  if (currentPlayer.playSelection) {
    throw new Error("Player already made a selection");
  }

  currentPlayer.playSelection = message.playSelection;

  const player1Selection = game?.player1.playSelection;
  const player2Selection = game?.player2?.playSelection;

  // once both players have a selection, determine the winner
  if (
    !!player1Selection &&
    player1Selection !== undefined &&
    !!player2Selection &&
    player2Selection !== undefined
  ) {
    const outcome = determineWinner(player1Selection, player2Selection);
    game.outcome = outcome;
    if (outcome === "player1") {
      game.player1Score += 1;
    } else if (outcome === "player2") {
      game.player2Score += 1;
    }
  }
}
