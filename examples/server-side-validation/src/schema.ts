import { Account, Group, co, experimental_defineRequest, z } from "jazz-tools";

export const PlaySelection = co.map({
  value: z.literal(["rock", "paper", "scissors"]),
  group: Group,
});
export type PlaySelection = co.loaded<typeof PlaySelection>;

export const Player = co.map({
  account: co.account(),
  playSelection: PlaySelection.optional(),
});
export type Player = co.loaded<typeof Player>;

export const Game = co.map({
  player1: Player,
  player2: Player,
  outcome: z.literal(["player1", "player2", "draw"]).optional(),
  player1Score: z.number(),
  player2Score: z.number(),
});
export type Game = co.loaded<typeof Game>;

export const WaitingRoom = co.map({
  creator: co.account(),
  game: z.optional(Game),
});
export type WaitingRoom = co.loaded<typeof WaitingRoom>;
