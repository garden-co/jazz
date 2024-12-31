import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useCoState } from "@/main";
import { type Card, Game, type Player, type Suit } from "@/schema";
import * as RadioGroup from "@radix-ui/react-radio-group";
import { createFileRoute } from "@tanstack/react-router";
import type { ID, co } from "jazz-tools";
import { AnimatePresence, LayoutGroup, Reorder, m, motion } from "motion/react";
import type { FormEventHandler, ReactNode } from "react";
import bastoni from "../../bastoni.svg?url";
import coppe from "../../coppe.svg?url";
import denari from "../../denari.svg?url";
import spade from "../../spade.svg?url";

export const Route = createFileRoute("/game/$gameId")({
  component: RouteComponent,
});

function RouteComponent() {
  const { gameId } = Route.useParams();

  const game = useCoState(Game, gameId as ID<Game>, {
    // TODO: load intent only for current user
    deck: [{}],
    playedCard: { data: {} },
    player1: {
      hand: [{ data: {} }],
      scoredCards: [{ data: {} }],
      account: {},
      playIntent: {},
    },
    player2: {
      hand: [{ data: {} }],
      scoredCards: [{ data: {} }],
      account: {},
      playIntent: {},
    },
    activePlayer: { account: {} },
  });

  // TODO: loading
  if (!game) return null;

  const [opponent, me] = game.player1.account.isMe
    ? [game.player2, game.player1]
    : [game.player1, game.player2];

  const handleSubmit: FormEventHandler<HTMLFormElement> = (e) => {
    e.preventDefault();

    if (!game.activePlayer.account.isMe) {
      alert("not your turn!");
      return;
    }

    const playedCard = new FormData(e.target as HTMLFormElement).get(
      "play",
    ) as string;

    const playedCardValue: number = Number.parseInt(playedCard?.slice(1));
    const playedCardSuit: string = playedCard?.[0];

    const pc = me.hand.find(
      (card) =>
        card.data?.value === playedCardValue &&
        card.data.suit === playedCardSuit,
    );

    me.playIntent.card = pc;
  };

  return (
    <LayoutGroup>
      <div className="flex flex-col h-full p-2 bg-green-800">
        <PlayerArea player={opponent}>
          <ul className="flex gap-2 flex-row-reverse place-content-center ">
            <AnimatePresence>
              {opponent.hand.getSorted().map((card) => (
                <motion.li key={card?.id} layout>
                  <PlayingCard card={card} faceDown />
                </motion.li>
              ))}
            </AnimatePresence>
          </ul>
        </PlayerArea>

        <div className="container grow items-center justify-center grid grid-cols-2">
          <div className="relative flex justify-center items-center">
            {game.deck[0] && (
              <PlayingCard
                className="rotate-[88deg] left-1/2 absolute"
                card={game.deck[0]}
              />
            )}
            <CardStack cards={game.deck.slice(1)} faceDown />
          </div>

          <div className="relative h-full items-center flex justify-center">
            <AnimatePresence>
              {game.playedCard && (
                <motion.div
                  className="absolute"
                  key={game.playedCard.id}
                  layoutId={`${game.playedCard?.data?.suit}${game.playedCard?.data?.value}`}
                >
                  <PlayingCard card={game.playedCard} />
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        </div>

        <form onSubmit={handleSubmit}>
          <PlayerArea player={me}>
            <div className="">
              <RadioGroup.Root
                className="flex place-content-center"
                aria-label="Play card"
                orientation="horizontal"
                loop
                name="play"
                required
                asChild
              >
                <Reorder.Group
                  axis="x"
                  values={me.hand.getSorted()}
                  onReorder={(cards) => {
                    cards.forEach((card, i) => {
                      if (!card) return;
                      card.order = i;
                    });
                  }}
                >
                  <AnimatePresence>
                    {me.hand
                      .getSorted()
                      .filter(
                        (card) =>
                          card?.data?.value !== undefined &&
                          card.data.suit !== undefined,
                      )
                      .map((card, i, cards) => (
                        <Reorder.Item
                          key={`${card?.data?.suit}${card?.data?.value}`}
                          value={card}
                          initial={{
                            translateY: 800,
                          }}
                          animate={{
                            rotate: i * 15 - (15 * (cards.length - 1)) / 2,
                            ...(cards.length === 3 &&
                              i === 1 && {
                                marginTop: -15,
                              }),
                            translateY: 0,
                          }}
                          whileDrag={{ scale: 1.1 }}
                          exit={{
                            scale: 1.1,
                          }}
                          layout
                          layoutId={`${card?.data?.suit}${card?.data?.value}`}
                        >
                          <RadioGroup.Item
                            value={`${card?.data?.suit}${card?.data?.value}`}
                            className="relative data-[state=checked]:border"
                            asChild
                          >
                            <motion.button>
                              <PlayingCard card={card} />
                            </motion.button>
                          </RadioGroup.Item>
                        </Reorder.Item>
                      ))}
                  </AnimatePresence>
                </Reorder.Group>
              </RadioGroup.Root>
            </div>
          </PlayerArea>
        </form>
      </div>
    </LayoutGroup>
  );
}

interface CardStackProps {
  cards: Card[];
  className?: string;
  faceDown?: boolean;
}

function CardStack({ cards, className, faceDown = false }: CardStackProps) {
  return (
    <div className={cn("relative p-4 w-[200px] h-[280px]", className)}>
      <AnimatePresence>
        {cards.map((card, i) => (
          <motion.div
            key={card?.id}
            className="absolute"
            style={{
              rotate: `${(i % 3) * (i % 5) * 3}deg`,
            }}
            layout
          >
            <PlayingCard
              card={card}
              faceDown={faceDown}
              layoutId={
                faceDown ? undefined : `${card?.data?.suit}${card?.data?.value}`
              }
            />
          </motion.div>
        ))}
      </AnimatePresence>
    </div>
  );
}

interface PlayingCardProps {
  card: co<Card>;
  faceDown?: boolean;
  className?: string;
  layoutId?: string;
}
function PlayingCard({
  card,
  className,
  faceDown = false,
  layoutId,
}: PlayingCardProps) {
  const cardImage = getCardImage(card.data?.suit!);
  if (!faceDown && card.data?.value === undefined && card.data?.suit) {
    return null;
  }

  return (
    <motion.div
      className={cn(
        "block aspect-card w-[150px] bg-white touch-none rounded-lg shadow-lg transform-gpu p-2 border",
        className,
      )}
      style={{
        ...(faceDown && {
          backgroundImage: `url(https://placecats.com/150/243)`,
          backgroundSize: "cover",
        }),
      }}
      layoutId={layoutId}
    >
      <div className="border-zinc-400 border rounded-lg h-full px-1 flex flex-col ">
        {!faceDown && (
          <>
            <div className="text-4xl font-bold text-black self-start">
              {card.data?.value}
            </div>
            <div className="grow flex justify-center items-center">
              <img
                src={cardImage}
                className="pointer-events-none max-h-[140px]"
              />
            </div>
            <div className="text-4xl font-bold text-black rotate-180 transform self-end">
              {card.data?.value}
            </div>
          </>
        )}
      </div>
    </motion.div>
  );
}

function getCardImage(suit: typeof Suit) {
  switch (suit) {
    case "C":
      return coppe;
    case "D":
      return denari;
    case "S":
      return spade;
    case "B":
      return bastoni;
  }
}

interface PlayerAreaProps {
  player: Player;
  children: ReactNode;
}
function PlayerArea({ children, player }: PlayerAreaProps) {
  return (
    <div className={cn("flex", !player.account?.isMe && "flex-row-reverse")}>
      <div className="flex items-center justify-center w-1/3">
        {player.account?.isMe && (
          <Button size="lg" type="submit">
            Play
          </Button>
        )}
      </div>
      <div className="w-1/3">{children}</div>
      <div
        className={cn(
          "flex justify-center flex-col items-center w-1/3",
          !player.account?.isMe && "flex-col-reverse",
        )}
      >
        <span className="font-semibold text-lg">
          {getScore(player.scoredCards?.map((c) => c!) ?? [])}
        </span>
        <CardStack cards={player.scoredCards?.map((c) => c!) ?? []} />
      </div>
    </div>
  );
}

function getScore(cards: Card[]) {
  return cards.reduce((acc, card) => {
    switch (card.data?.value) {
      case 3:
        return acc + 10;
      case 1:
        return acc + 11;
      case 10:
        return acc + 4;
      case 9:
        return acc + 3;
      case 8:
        return acc + 2;
    }
    return acc;
  }, 0);
}
