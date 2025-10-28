import { experimental_defineRequest, z } from "jazz-tools";
import { Game, WaitingRoom } from "./schema";

const workerId = process.env.NEXT_PUBLIC_JAZZ_WORKER_ACCOUNT!;

const createGameRequest = experimental_defineRequest({
  url: "/api/create-game",
  workerId,
  request: {},
  response: {
    schema: { waitingRoom: WaitingRoom },
    resolve: { waitingRoom: { creator: true } },
  },
});

const joinGameRequest = experimental_defineRequest({
  url: "/api/join-game",
  workerId,
  request: {
    schema: {
      waitingRoom: WaitingRoom,
    },
    resolve: {
      waitingRoom: {
        creator: true,
      },
    },
  },
  response: {
    schema: {
      waitingRoom: WaitingRoom,
    },
    resolve: {
      waitingRoom: {
        game: {
          player1: {
            account: true,
          },
          player2: {
            account: true,
          },
        },
      },
    },
  },
});

const newGameRequest = experimental_defineRequest({
  url: "/api/new-game",
  workerId,
  request: {
    schema: {
      game: Game,
    },
    resolve: {
      game: {
        player1: {
          account: true,
        },
        player2: {
          account: true,
        },
      },
    },
  },
  response: {
    schema: {
      game: Game,
    },
    resolve: {
      game: {
        player1State: {
          $onError: null,
        },
        player2State: {
          $onError: null,
        },
        player1: true,
        player2: true,
      },
    },
  },
});

const playRequest = experimental_defineRequest({
  url: "/api/play",
  workerId,
  request: {
    schema: {
      game: Game,
      selection: z.literal(["rock", "paper", "scissors"]),
    },
    resolve: {
      game: {
        player1: {
          account: true,
          playSelection: {
            group: true,
          },
        },
        player2: {
          account: true,
          playSelection: {
            group: true,
          },
        },
      },
    },
  },
  response: {
    schema: {
      game: Game,
    },
    resolve: {
      game: true,
    },
  },
});

export const serverApi = {
  createGame: createGameRequest,
  joinGame: joinGameRequest,
  newGame: newGameRequest,
  play: playRequest,
};
