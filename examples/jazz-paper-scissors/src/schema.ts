import { co, z } from "jazz-tools";

export const Player = co.map({
  account: co.account(),
  playSelection: z.optional(z.literal(["rock", "paper", "scissors"])),
});

export const Game = co.map({
  player1: Player,
  player2: z.optional(Player),
  outcome: z.optional(z.literal(["player1", "player2", "draw"])),
  player1Score: z.number(),
  player2Score: z.number(),
});

export const WaitingRoom = co.map({
  account1: co.account(),
  account2: z.optional(co.account()),
  game: z.optional(Game),
});

export const PlayIntent = co.map({
  type: z.literal("play"),
  gameId: z.string(),
  player: z.literal(["player1", "player2"]),
  playSelection: z.literal(["rock", "paper", "scissors"]),
});

export const NewGameIntent = co.map({
  type: z.literal("newGame"),
  gameId: z.string(),
});

export const CreateGameRequest = co.map({
  type: z.literal("createGame"),
});

export const JoinGameRequest = co.map({
  type: z.literal("joinGame"),
  waitingRoom: WaitingRoom,
});

export const InboxMessage = z.discriminatedUnion("type", [
  PlayIntent,
  NewGameIntent,
  CreateGameRequest,
  JoinGameRequest,
]);
