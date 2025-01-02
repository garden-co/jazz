import {
  Card,
  CardData,
  CardList,
  CardMeta,
  CardValues,
  DealerAccount,
  Game,
  PlayIntent,
  Player,
  StartGameRequest,
  Suits,
  WaitingRoom,
} from "@/schema";
import { startWorker } from "jazz-nodejs";
import { Account, Group, type ID } from "jazz-tools";

const {
  worker,
  experimental: { inbox },
} = await startWorker({
  AccountSchema: DealerAccount,
  accountID: import.meta.env.VITE_JAZZ_WORKER_ACCOUNT,
  syncServer: "wss://cloud.jazz.tools/?key=you@example.com",
});

inbox.subscribe(
  StartGameRequest,
  async (message, senderID) => {
    const playerAccount = await Account.load(senderID, worker, {});
    if (!playerAccount) {
      return;
    }

    // if there's already a waiting room, add the player to it and start the game.
    if (message.waitingRoom && message.waitingRoom.account1) {
      console.log("Join game request from", senderID);
      message.waitingRoom.account2 = playerAccount;

      const game = await createGame({
        account1: message.waitingRoom.account1,
        account2: message.waitingRoom.account2,
      });
      console.log("game created with id:", game.id);
      await worker.ensureLoaded({ root: { activeGames: [{}] } });
      worker.root?.activeGames?.push(game);
      message.waitingRoom.game = game;
      resumeGame({ gameId: game.id });

      return message.waitingRoom;
    }

    console.log("Create game request from", senderID);
    // else, create a new waiting room.
    const waitingRoomGroup = Group.create({ owner: worker });
    waitingRoomGroup.addMember("everyone", "reader");
    const waitingRoom = WaitingRoom.create(
      { account1: playerAccount },
      { owner: waitingRoomGroup },
    );

    console.log("waiting room created with id:", waitingRoom.id);

    return waitingRoom;
  },
  { retries: 3 },
);

interface CreateGameParams {
  account1: Account;
  account2: Account;
}
async function createGame({ account1, account2 }: CreateGameParams) {
  const publicReadOnly = Group.create({ owner: worker });
  publicReadOnly.addMember(account1, "reader");
  publicReadOnly.addMember(account2, "reader");

  const player1 = createPlayer({ owner: publicReadOnly, account: account1 });
  const player2 = createPlayer({ owner: publicReadOnly, account: account2 });

  const deck = createDeck({ publicReadOnlyGroup: publicReadOnly });

  const player1Reader = Group.create({ owner: worker });
  player1Reader.addMember(account1, "reader");
  const publicReadPlayer1Write = Group.create({ owner: worker });
  publicReadPlayer1Write.addMember(account1, "writer");
  publicReadPlayer1Write.addMember("everyone", "reader");

  const player2Reader = Group.create({ owner: worker });
  player2Reader.addMember(account2, "reader");
  const publicReadPlayer2Write = Group.create({ owner: worker });
  publicReadPlayer2Write.addMember(account2, "writer");
  publicReadPlayer2Write.addMember("everyone", "reader");

  while (player1.hand && player1.hand?.length < 3) {
    await drawCard(player1, publicReadPlayer1Write, player1Reader, deck);
  }

  while (player2.hand && player2.hand?.length < 3) {
    await drawCard(player2, publicReadPlayer2Write, player2Reader, deck);
  }

  const game = Game.create(
    {
      deck,
      briscola: deck[0]?.data?.suit!,
      activePlayer: player1,
      player1: player1,
      player2: player2,
    },
    { owner: publicReadOnly },
  );

  await game.waitForSync();

  return game;
}

interface CreatePlayerParams {
  owner: Group;
  account: Account;
}
function createPlayer({ owner, account }: CreatePlayerParams) {
  const playerWrite = Group.create({ owner: worker });
  playerWrite.addMember(account, "writer");

  const player = Player.create(
    {
      scoredCards: CardList.create([], {
        owner,
      }),
      playIntent: PlayIntent.create({}, { owner: playerWrite }),
      account,
      hand: CardList.create([], { owner }),
    },
    { owner },
  );

  return player;
}

interface CreateDeckParams {
  publicReadOnlyGroup: Group;
}
function createDeck({ publicReadOnlyGroup }: CreateDeckParams) {
  const allCards = Suits.flatMap((suit) => {
    return CardValues.map((value) => {
      return { value, suit };
    });
  });
  shuffle(allCards);

  const deck = CardList.create(
    allCards.map((card, i) => {
      const cardDataGroup = Group.create({ owner: worker });

      return Card.create(
        {
          // The first card is the briscola, it should be visible by everyone,
          // so we make its owner the public read-only group.
          data: CardData.create(card, {
            owner: i === 0 ? publicReadOnlyGroup : cardDataGroup,
          }),
        },
        { owner: publicReadOnlyGroup },
      );
    }),
    { owner: publicReadOnlyGroup },
  );

  return deck;
}

async function drawCard(
  player: Player,
  publicReadPlayerWrite: Group,
  playerRead: Group,
  deck: CardList,
) {
  const card = deck.pop();

  if (card) {
    // Create the card meta. This is visible to everyone. It's used to sort the cards in the UI.
    card.meta = CardMeta.create({ index: 0 }, { owner: publicReadPlayerWrite });
    // Extends the card data group, which at creation is private to the worker,
    // with the player's read group, so the player can read the card data.

    await card.ensureLoaded({ data: {} });
    card.data?._owner.castAs(Group).extend(playerRead);
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

function getCardValue(card: CardData) {
  switch (card.value) {
    case 1:
      return 20;
    case 3:
      return 15;
    default:
      return card.value;
  }
}
interface ResumeGameParams {
  gameId: ID<Game>;
}
async function resumeGame({ gameId }: ResumeGameParams) {
  console.log("Resuming game", gameId);
  const game = await Game.load(gameId, worker, {
    deck: [{ data: {} }],
    player1: {
      hand: [{ data: {} }],
      scoredCards: [{}],
      account: {},
      playIntent: {},
    },
    player2: {
      hand: [{ data: {} }],
      scoredCards: [{}],
      account: {},
      playIntent: {},
    },
    activePlayer: {
      account: {},
    },
  });
  const publicReadOnly = game?.deck._owner.castAs(Group);

  if (!game || !publicReadOnly) {
    // TODO: Error
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

    console.log(
      "player",
      player.account?.id,
      "played",
      intent.card.data?.value,
      intent.card.data?.suit,
    );

    const cardIndex =
      player.hand?.findIndex((card) => {
        return (
          card?.data?.value === player.playIntent?.card?.data?.value &&
          card?.data?.suit === player.playIntent?.card?.data?.suit
        );
      }) ?? -1;

    if (cardIndex === -1) {
      return;
    }
    // remove the card from player's hand
    player.hand?.splice(cardIndex, 1);

    // make the newly played card's data visible to everyone by extending its group with
    // the public read-only group so that everyone can see the card.
    intent.card.data?._owner.castAs(Group).extend(publicReadOnly);

    // If there's already a card on the table, it means both players have played.
    if (game.playedCard) {
      // Check who's this turn's winner
      let winner: Player;
      // If both cards have the same suit, the one with the highest value wins
      if (game.playedCard.data?.suit === intent.card.data?.suit) {
        winner =
          getCardValue(intent.card.data!) > getCardValue(game.playedCard.data!)
            ? player
            : opponent;
      } else {
        // else the active player wins only if they played a briscola.
        // (we already know the other player didn't)
        if (intent.card.data?.suit === game.briscola) {
          winner = player;
        } else {
          winner = opponent;
        }
      }

      // Put the cards in the winner's scored cards pile.
      winner.scoredCards?.push(game.playedCard, intent.card);

      // The winner of the round always draws first.
      console.log("deck size", game.deck.length);
      if (game.deck.length > 0) {
        drawCard(
          winner,
          winner.hand?.[0]?._owner.castAs(Group)!,
          winner.hand?.[0]?.data?._owner.castAs(Group)!,
          game.deck,
        );

        const opponent = game.getOpponent(winner);
        drawCard(
          opponent,
          opponent.hand?.[0]?._owner.castAs(Group)!,
          opponent.hand?.[0]?.data?._owner.castAs(Group)!,
          game.deck,
        );
      }

      // @ts-expect-error types are wonky
      game.activePlayer = winner;
      // And finally, remove the played card from the table.
      delete game.playedCard;

      // TODO: if there are no more cards in the deck and both players have played all their cards, end the game.
    } else {
      // else, just put the card on the table and switch the active player.
      game.playedCard = intent.card;

      // @ts-expect-error types are wonky
      game.activePlayer = opponent;
    }
  };

  game.player1.playIntent.subscribe(
    { card: { data: {} } },
    onPlayIntent(game.player1),
  );
  game.player2.playIntent.subscribe(
    { card: { data: {} } },
    onPlayIntent(game.player2),
  );
}

await worker.ensureLoaded({ root: { activeGames: [{}] } });

worker.root?.activeGames?.forEach((game) => {
  if (!game) return;

  resumeGame({
    gameId: game.id,
  });
});
