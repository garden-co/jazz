import { Account, CoMap, SchemaUnion, co } from "jazz-tools";

export class Game extends CoMap {
  player1 = co.ref(Player);
  player2? = co.ref(Player);
  outcome? = co.string;

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

class BaseInboxMessage extends CoMap {
  type = co.literal("play", "createGame", "joinGame");
}

export class PlayIntent extends BaseInboxMessage {
  type = co.literal("play");
  gameId = co.string;
  player = co.literal("player1", "player2");
  playSelection = co.string;
}

export class CreateGameRequest extends BaseInboxMessage {
  type = co.literal("createGame");
}

export class JoinGameRequest extends BaseInboxMessage {
  type = co.literal("joinGame");
  waitingRoom = co.ref(WaitingRoom);
}

export const InboxMessage = SchemaUnion.Of<BaseInboxMessage>((raw) => {
  switch (raw.get("type")) {
    case "play":
      return PlayIntent;
    case "createGame":
      return CreateGameRequest;
    case "joinGame":
      return JoinGameRequest;
    default:
      throw new Error("Unknown request type");
  }
});
