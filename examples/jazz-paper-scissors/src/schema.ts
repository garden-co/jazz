import { co, z } from "jazz-tools";

export const Player = co.map({
  account: co.account(),
  playSelection: z.optional(z.literal(["rock", "paper", "scissors"])),
});
export type Player = co.loaded<typeof Player>;

export const Game = co.map({
  player1: Player,
  player2: z.optional(Player),
  outcome: z.optional(z.literal(["player1", "player2", "draw"])),
  player1Score: z.number(),
  player2Score: z.number(),
});
export type Game = co.loaded<typeof Game>;

export const WaitingRoom = co.map({
  account1: co.account(),
  account2: z.optional(co.account()),
  game: z.optional(Game),
});
export type WaitingRoom = co.loaded<typeof WaitingRoom>;

export const PlayIntent = co.map({
  type: z.literal("play"),
  gameId: z.string(),
  player: z.literal(["player1", "player2"]),
  playSelection: z.literal(["rock", "paper", "scissors"]),
});
export type PlayIntent = co.loaded<typeof PlayIntent>;

export const NewGameIntent = co.map({
  type: z.literal("newGame"),
  gameId: z.string(),
});
export type NewGameIntent = co.loaded<typeof NewGameIntent>;

export const CreateGameRequest = co.map({
  type: z.literal("createGame"),
});
export type CreateGameRequest = co.loaded<typeof CreateGameRequest>;

export const JoinGameRequest = co.map({
  type: z.literal("joinGame"),
  waitingRoom: WaitingRoom,
});
export type JoinGameRequest = co.loaded<typeof JoinGameRequest>;

export const InboxMessage = z.discriminatedUnion([
  PlayIntent,
  NewGameIntent,
  CreateGameRequest,
  JoinGameRequest,
]);
export type InboxMessage = co.loaded<typeof InboxMessage>;
