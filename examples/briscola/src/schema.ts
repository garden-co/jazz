import { Account, CoList, CoMap, Inbox, co } from "jazz-tools";

export const CardValues = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] as const;

export const CardValue = co.literal(...CardValues);

export const Suits = ["S", "B", "C", "D"] as const;

export const Suit = co.literal(...Suits);

export class CardMeta extends CoMap {
  index = co.optional.number;
}
export class CardData extends CoMap {
  value = CardValue;
  suit = Suit;
}

export class Card extends CoMap {
  data = co.ref(CardData);
  meta = co.optional.ref(CardMeta);
}

export class CardList extends CoList.Of(co.ref(Card)) {
  getSorted() {
    return this.toSorted(
      (a, b) => (a?.meta?.index ?? 0) - (b?.meta?.index ?? 0),
    );
  }
}

export class PlayIntent extends CoMap {
  card = co.optional.ref(Card);
}

export class Player extends CoMap {
  account = co.ref(Account);
  playIntent = co.ref(PlayIntent); // write Tavolo - write me - quando un giocatore gioca una carta la scrive qui, il Game la legge, la valida e la mette sul tavolo
  hand = co.ref(CardList); // write Tavolo - read me - quando il Game mi da le carte le scrive qui, quando valida la giocata la toglie da qui
  scoredCards = co.ref(CardList); // write Tavolo - read everyone -
}

export class Game extends CoMap {
  deck = co.ref(CardList);

  briscola = co.literal(...Suits);
  /**
   * The card that was played in the current turn.
   */
  playedCard = co.optional.ref(Card);

  activePlayer = co.ref(Player);
  player1 = co.ref(Player);
  player2 = co.ref(Player);

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

    return opponent;
  }
}

export class GameList extends CoList.Of(co.ref(Game)) {}

export class WaitingRoom extends CoMap {
  account1 = co.ref(Account);
  account2 = co.optional.ref(Account);
  game = co.optional.ref(Game);
}

export class DealerAccountRoot extends CoMap {
  activeGames = co.ref(GameList);
}

export class DealerAccount extends Account {
  root = co.ref(DealerAccountRoot);

  migrate() {
    if (!this._refs.root) {
      this.root = DealerAccountRoot.create(
        {
          activeGames: GameList.create([], { owner: this }),
        },
        { owner: this },
      );
    }
  }
}

export class StartGameRequest extends CoMap {
  waitingRoom = co.optional.ref(WaitingRoom);
}
