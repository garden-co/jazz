import { Account, CoMap, co } from "jazz-tools";

export class Game extends CoMap {
  player1 = co.ref(Player);
  player2? = co.ref(Player);
  outcome? = co.literal("player1", "player2", "draw");
  player1Score = co.number;
  player2Score = co.number;

  /**
   * Given a player, returns the opponent in the current game.
   */
  getOpponent(player: Player) {
    // TODO: player may be unrelated to this game
    const opponent =
      player.account?.id === this.player1?.account?.id
        ? this.player2
        : this.player1;

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
  account = co.ref(Account);
  playSelection? = co.string;
}

export class WaitingRoom extends CoMap {
  account1 = co.ref(Account);
  account2 = co.optional.ref(Account);
  game = co.optional.ref(Game);
}

export class InboxMessage extends CoMap {
  type = co.literal("play", "createGame", "joinGame", "newGame");
}

export class PlayIntent extends InboxMessage {
  type = co.literal("play");
  gameId = co.string;
  player = co.literal("player1", "player2");
  playSelection = co.string;
}

export class NewGameIntent extends InboxMessage {
  type = co.literal("newGame");
  gameId = co.string;
}

export class CreateGameRequest extends InboxMessage {
  type = co.literal("createGame");
}

export class JoinGameRequest extends InboxMessage {
  type = co.literal("joinGame");
  waitingRoom = co.ref(WaitingRoom);
}
