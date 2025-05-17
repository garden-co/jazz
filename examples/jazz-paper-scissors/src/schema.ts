import { Account, CoMap, coField } from "jazz-tools";

export class Game extends CoMap {
  player1 = coField.ref(Player);
  player2? = coField.ref(Player);
  outcome? = coField.literal("player1", "player2", "draw");
  player1Score = coField.number;
  player2Score = coField.number;

  /**
   * Given a player, returns the opponent in the current game.
   */
  static getOpponent(game: Game, player: Player) {
    // TODO: player may be unrelated to this game
    const opponent =
      player.account?.id === game.player1?.account?.id
        ? game.player2
        : game.player1;

    if (!opponent) {
      throw new Error("Opponent not found");
    }

    return opponent.ensureLoaded({
      // account: {},
      resolve: {},
    });
  }
}

export class Player extends CoMap {
  account = coField.ref(Account);
  playSelection? = coField.literal("rock", "paper", "scissors");
}

export class WaitingRoom extends CoMap {
  account1 = coField.ref(Account);
  account2 = coField.optional.ref(Account);
  game = coField.optional.ref(Game);
}

export class InboxMessage extends CoMap {
  type = coField.literal("play", "createGame", "joinGame", "newGame");
}

export class PlayIntent extends InboxMessage {
  type = coField.literal("play");
  gameId = coField.string;
  player = coField.literal("player1", "player2");
  playSelection = coField.literal("rock", "paper", "scissors");
}

export class NewGameIntent extends InboxMessage {
  type = coField.literal("newGame");
  gameId = coField.string;
}

export class CreateGameRequest extends InboxMessage {
  type = coField.literal("createGame");
}

export class JoinGameRequest extends InboxMessage {
  type = coField.literal("joinGame");
  waitingRoom = coField.ref(WaitingRoom);
}
