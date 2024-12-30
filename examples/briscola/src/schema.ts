import { Account, CoFeed, CoList, CoMap, co } from "jazz-tools";

export const CardValues = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] as const;

export const CardValue = co.literal(...CardValues);

export const Suits = ["S", "B", "C", "D"] as const;

export const Suit = co.literal(...Suits);

export class Card extends CoMap {
  value = CardValue;
  suit = Suit;
}

export class CardList extends CoList.Of(co.ref(Card)) {}

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

// Env: ID e invito publico write only
export class Inbox extends CoFeed.Of(co.ref(Game)) {}
