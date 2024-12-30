import {
  Card,
  CardList,
  CardValues,
  Game,
  PlayIntent,
  Player,
  type Suit,
  Suits,
} from "@/schema";
import { startWorker } from "jazz-nodejs";
import { Account, Group, type ID } from "jazz-tools";

const { worker } = await startWorker({
  syncServer: "wss://cloud.jazz.tools/?key=you@example.com",
});

async function createGame() {
  const everyoneWriter = Group.create({ owner: worker });
  everyoneWriter.addMember("everyone", "writer");

  const [acc1, acc2] = await Promise.all([
    Account.load("co_zo41be46XeAEuRjGYETaoPpZrKU" as ID<Account>, worker, {}),
    Account.load("co_zY45YcrgutxEjWCVC7aVG56yL83" as ID<Account>, worker, {}),
  ]);

  if (!acc1 || !acc2) {
    return;
  }

  const player1 = createPlayer({ owner: everyoneWriter, account: acc1 });
  const player2 = createPlayer({ owner: everyoneWriter, account: acc2 });

  const deck = createDeck({ owner: everyoneWriter });

  while (player1.hand && player1.hand?.length < 3) {
    drawCard(player1, deck);
  }

  while (player2.hand && player2.hand?.length < 3) {
    drawCard(player2, deck);
  }

  const game = Game.create(
    {
      deck,
      briscola: deck[0]?.suit!,
      activePlayer: player1,
      player1: player1,
      player2: player2,
    },
    { owner: everyoneWriter },
  );

  await game.waitForSync();

  return game.id;
}

interface CreatePlayerParams {
  owner: Group;
  account: Account;
}
function createPlayer({ owner, account }: CreatePlayerParams) {
  const player = Player.create(
    {
      scoredCards: CardList.create([], {
        owner,
      }),
      playIntent: PlayIntent.create({}, { owner }),
      account,
      hand: CardList.create([], { owner }),
    },
    { owner },
  );

  return player;
}

interface CreateDeckParams {
  owner: Group;
}
function createDeck({ owner }: CreateDeckParams) {
  const allCards = Suits.flatMap((suit) => {
    return CardValues.map((value) => {
      return { value, suit };
    });
  });
  shuffle(allCards);

  const deck = CardList.create(
    allCards.map((card) => {
      return Card.create(card, { owner });
    }),
    { owner },
  );

  return deck;
}

function drawCard(player: Player, deck: CardList) {
  const card = deck.pop();
  if (card) {
    // TODO: set permission of the card to read-only player1
    player.hand?.push(card);
  }
}

// Fisher–Yates shuffle
function shuffle(array: unknown[]) {
  let currentIndex = array.length;

  while (currentIndex) {
    const randomIndex = Math.floor(Math.random() * currentIndex);
    currentIndex--;

    [array[currentIndex], array[randomIndex]] = [
      array[randomIndex],
      array[currentIndex],
    ];
  }
}

function getCardValue(card: Card) {
  switch (card.value) {
    case 1:
      return 20;
    case 3:
      return 15;
    default:
      return card.value;
  }
}

async function resumeGame(gameId: ID<Game>) {
  const game = await Game.load(gameId, worker, {
    deck: [{}],
    player1: { hand: [{}], scoredCards: [{}], account: {}, playIntent: {} },
    player2: { hand: [{}], scoredCards: [{}], account: {}, playIntent: {} },
    activePlayer: {
      account: {},
    },
  });

  if (!game) {
    return;
  }

  const onPlayIntent = (player: Player) => (intent: PlayIntent) => {
    console.log("Got message from player", player.account?.id);
    // Ignore play intent if it's not the player's turn.
    if (game.activePlayer?.account?.id !== player.account?.id || !intent.card) {
      console.log("skipping");
      return;
    }

    const opponent = game.getOpponent(player);
    // game.player1.account?.id === player.account?.id
    //   ? game.player2
    //   : game.player1;

    console.log(
      "player",
      player.account?.id,
      "played",
      intent.card.value,
      intent.card.suit,
    );

    const cardIndex =
      player.hand?.findIndex((card) => {
        return (
          card?.value === player.playIntent?.card?.value &&
          card?.suit === player.playIntent?.card?.suit
        );
      }) ?? -1;

    if (cardIndex === -1) {
      return;
    }
    // remove the card from player's hand
    player.hand?.splice(cardIndex, 1);

    // If there's already a card on the table, check who wins and move both cads to winner's scoredCards
    if (game.playedCard) {
      let winner: Player;

      // If both cards have the same suit, the one with the highest value wins
      if (game.playedCard.suit === intent.card.suit) {
        winner =
          getCardValue(intent.card) > getCardValue(game.playedCard)
            ? player
            : opponent;
      } else {
        // else the active player wins only if they played a briscola.
        // (we already know the other player didn't)
        if (intent.card.suit === game.briscola) {
          winner = player;
        } else {
          winner = opponent;
        }
      }

      winner.scoredCards?.push(game.playedCard, intent.card);

      // the winner draws first
      if (game.deck.length > 0) {
        drawCard(winner, game.deck);
        drawCard(game.getOpponent(winner), game.deck);
      }

      // @ts-expect-error types are wonky
      game.activePlayer = winner;
      delete game.playedCard;
    } else {
      // else, just put the card on the table and switch active player
      game.playedCard = intent.card;

      // Switch active player
      // @ts-expect-error types are wonky
      game.activePlayer = opponent;
    }
  };

  game.player1.playIntent.subscribe({ card: {} }, onPlayIntent(game.player1));
  game.player2.playIntent.subscribe({ card: {} }, onPlayIntent(game.player2));
}

let gameId: ID<Game> | undefined;

gameId = await createGame();
console.log("Game created with id:", gameId);

resumeGame(gameId ?? ("co_znBaWhhHHfkVgE2EZiWjv4sm3hF" as ID<Game>));
