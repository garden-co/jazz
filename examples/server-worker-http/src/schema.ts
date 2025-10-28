import { Account, Group, co, z } from "jazz-tools";

export const PlayerState = co.map({
  currentSelection: z.literal(["rock", "paper", "scissors"]).optional(),
  submitted: z.boolean(),
  resetRequested: z.boolean(),
});
export type PlayerState = co.loaded<typeof PlayerState>;

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
  player1State: PlayerState,
  player2State: PlayerState,
  outcome: z.literal(["player1", "player2", "draw"]).optional(),
  player1Score: z.number(),
  player2Score: z.number(),
});
export type Game = co.loaded<typeof Game>;

export const WaitingRoom = co.map({
  creator: co.account(),
  game: co.optional(Game),
});
export type WaitingRoom = co.loaded<typeof WaitingRoom>;

export function createGameState(params: {
  account1: Account;
  account2: Account;
  worker: Account;
}) {
  const { account1, account2, worker } = params;
  const gameGroup = Group.create({ owner: worker });
  gameGroup.addMember(account1, "reader");
  gameGroup.addMember(account2, "reader");

  const player1 = Player.create(
    {
      account: account1,
    },
    gameGroup,
  );

  const player2 = Player.create(
    {
      account: account2,
    },
    gameGroup,
  );

  const player1StateGroup = Group.create(worker);
  player1StateGroup.addMember(account1, "writer");

  const player1State = PlayerState.create(
    {
      currentSelection: undefined,
      submitted: false,
      resetRequested: false,
    },
    player1StateGroup,
  );

  const player2StateGroup = Group.create(worker);
  player2StateGroup.addMember(account2, "writer");

  const player2State = PlayerState.create(
    {
      currentSelection: undefined,
      submitted: false,
      resetRequested: false,
    },
    player2StateGroup,
  );

  return {
    player1,
    player2,
    player1State,
    player2State,
    outcome: undefined,
  };
}
