import {
  Card,
  CardData,
  CardList,
  CardMeta,
  CardValues,
  Game,
  InboxMessage,
  JoinGameRequest,
  PlayIntent,
  Player,
  Suits,
  WaitingRoom,
} from "@/schema";
import { startWorker } from "jazz-nodejs";
import { Account, Group, type ID } from "jazz-tools";

const {
  worker,
  experimental: { inbox },
} = await startWorker({
  accountID: process.env.VITE_JAZZ_WORKER_ACCOUNT,
  syncServer: "wss://cloud.jazz.tools/?key=you@example.com",
});

inbox.subscribe(
  InboxMessage,
  async (message, senderID) => {
    const playerAccount = await Account.load(senderID, worker, {});
    if (!playerAccount) {
      return;
    }

    switch (message.type) {
      case "play":
        console.log("play message from", senderID);
        handlePlayIntent(senderID, message.castAs(PlayIntent));
        break;
      case "createGame":
        console.log("create game message from", senderID);

        const waitingRoomGroup = Group.create({ owner: worker });
        waitingRoomGroup.addMember("everyone", "reader");
        const waitingRoom = WaitingRoom.create(
          { account1: playerAccount },
          { owner: waitingRoomGroup },
        );

        console.log("waiting room created with id:", waitingRoom.id);

        return waitingRoom;
      case "joinGame":
        console.log("join game message from", senderID);
        const joinGameRequest = message.castAs(JoinGameRequest);
        if (
          !joinGameRequest.waitingRoom ||
          !joinGameRequest.waitingRoom.account1
        ) {
          console.error("No waiting room in join game request");
          return;
        }
        joinGameRequest.waitingRoom.account2 = playerAccount;

        const game = await createGame({
          account1: joinGameRequest.waitingRoom.account1,
          account2: joinGameRequest.waitingRoom.account2,
        });
        console.log("game created with id:", game.id);

        joinGameRequest.waitingRoom.game = game;
        return joinGameRequest.waitingRoom;
    }
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

  const player1 = createPlayer({ account: account1 });
  const player2 = createPlayer({ account: account2 });

  const deck = createDeck({ publicReadOnlyGroup: publicReadOnly });

  while (player1.hand && player1.hand?.length < 3) {
    await drawCard(player1, deck);
  }

  while (player2.hand && player2.hand?.length < 3) {
    await drawCard(player2, deck);
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
  account: Account;
}
function createPlayer({ account }: CreatePlayerParams) {
  const publicRead = Group.create({ owner: worker });
  publicRead.addMember("everyone", "reader");

  const player = Player.create(
    {
      scoredCards: CardList.create([], {
        owner: publicRead,
      }),
      account,
      hand: CardList.create([], { owner: publicRead }),
    },
    { owner: publicRead },
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

async function drawCard(player: Player, deck: CardList) {
  const card = deck.pop();

  const playerAccount = (await player.ensureLoaded({ account: {} }))?.account;

  if (!playerAccount) {
    console.error("failed to load player account");
    return;
  }

  if (card) {
    const metaGroup = Group.create({ owner: worker });
    metaGroup.addMember("everyone", "reader");
    metaGroup.addMember(playerAccount, "writer");
    // Create the card meta. This is visible to everyone.
    // It's used to sort the cards in the UI.
    card.meta = CardMeta.create({}, { owner: metaGroup });
    // Add the player to the card's data group so that
    // the player can read the card data.
    card.data?._owner.castAs(Group).addMember(playerAccount, "reader");
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

async function handlePlayIntent(senderId: ID<Account>, playIntent: PlayIntent) {
  const playedCard = await playIntent.ensureLoaded({ card: { data: {} } });
  if (!playedCard) {
    console.log("No card in play intent");
    return;
  }

  const state = await playIntent.ensureLoaded({
    game: {
      playedCard: { data: {} },
      deck: [{ data: {} }],
      activePlayer: { account: {} },
      player1: { hand: [{ data: {} }], scoredCards: [{}] },
      player2: { hand: [{ data: {} }], scoredCards: [{}] },
    },
  });

  if (!state?.game) {
    console.log("No game found");
    return;
  }

  if (state.game.activePlayer.account.id !== senderId) {
    console.log("Not player's turn");
    return;
  }

  const publicReadOnly = state.game?.deck._owner.castAs(Group);

  const player = state.game.activePlayer;
  const opponent = await state.game.getOpponent(player);

  if (!opponent) {
    console.error("failed to get opponent");
    return;
  }

  console.log(
    "player",
    player.account?.id,
    "played",
    playedCard.card.data?.value,
    playedCard.card.data?.suit,
  );

  const cardIndex =
    player.hand?.findIndex((card) => {
      return card?.id === playedCard.card.id;
    }) ?? -1;

  if (cardIndex === -1) {
    console.log("Card not found in player's hand");
    return;
  }

  // remove the card from player's hand
  player.hand?.splice(cardIndex, 1);

  // make the newly played card's data visible to everyone by extending its group with
  // the public read-only group so that everyone can see the card.
  const group = await playedCard.card.data._owner
    .castAs(Group)
    .ensureLoaded({});
  group?.extend(publicReadOnly);

  // If there's already a card on the table, it means both players have played.
  if (state.game.playedCard) {
    // Check who's this turn's winner
    let winner: Player;
    // If both cards have the same suit, the one with the highest value wins
    if (state.game.playedCard.data?.suit === playedCard.card.data?.suit) {
      winner =
        getCardValue(playedCard.card.data) >
        getCardValue(state.game.playedCard.data)
          ? player
          : opponent;
    } else {
      // else the active player wins only if they played a briscola.
      // (we already know the other player didn't)
      if (playedCard.card.data.suit === state.game.briscola) {
        winner = player;
      } else {
        winner = opponent;
      }
    }

    // Put the cards in the winner's scored cards pile.
    winner.scoredCards?.push(state.game.playedCard, playedCard.card);

    // The winner of the round always draws first.
    if (state.game.deck.length > 0) {
      drawCard(winner, state.game.deck);

      const opponent = await state.game.getOpponent(winner);
      if (!opponent) {
        console.error("failed to get opponent");
        return;
      }
      drawCard(opponent, state.game.deck);
    }

    // @ts-expect-error types are wonky
    state.game.activePlayer = winner;
    // And finally, remove the played card from the table.
    delete state.game.playedCard;

    // TODO: if there are no more cards in the deck and both players have played all their cards, end the game.
  } else {
    // else, just put the card on the table and switch the active player.
    state.game.playedCard = playedCard.card;

    state.game.activePlayer = opponent;
  }
}
