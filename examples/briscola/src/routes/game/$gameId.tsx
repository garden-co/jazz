import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { type Card, Game, type Player } from "@/schema";
import * as RadioGroup from "@radix-ui/react-radio-group";
import { createFileRoute, notFound, redirect } from "@tanstack/react-router";
import type { ID } from "jazz-tools";
import { AnimatePresence, LayoutGroup, Reorder, motion } from "motion/react";
import type { FormEventHandler, ReactNode } from "react";

import { PlayingCard } from "@/components/playing-card";
import { useCoState } from "@/jazz";

export const Route = createFileRoute("/game/$gameId")({
  component: RouteComponent,
  loader: async ({ params: { gameId }, context: { me } }) => {
    // !FIXME: this is useless, the layout takes care of this
    if (!me) {
      throw redirect({
        to: "/",
      });
    }
    const game = await Game.load(gameId as ID<Game>, me, {});

    if (!game) {
      throw notFound();
    }
  },
  // TODO: better loading screen
  pendingComponent: () => <div>...</div>,
  // TODO: better not found page
  notFoundComponent: () => <div>Game not found</div>,
});

function RouteComponent() {
  const { gameId } = Route.useParams();

  const game = useCoState(Game, gameId as ID<Game>, {
    // TODO: load intent only for current user
    deck: [{}],
    playedCard: { data: {} },
    player1: {
      hand: [{ data: {}, meta: {} }],
      scoredCards: [{ data: {} }],
      account: {},
      playIntent: {},
    },
    player2: {
      hand: [{ data: {}, meta: {} }],
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
                  <PlayingCard card={card} faceDown layoutId={card?.id} />
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
                layoutId={`${game.deck[0]?.id}`}
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
                  animate={{
                    rotate: 0,
                  }}
                >
                  <PlayingCard
                    card={game.playedCard}
                    layoutId={`${game.playedCard?.id}`}
                  />
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
                      if (!card?.meta) return;
                      card.meta.index = i;
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
                            translateY: 0,
                          }}
                          whileDrag={{ scale: 1.1 }}
                          exit={{
                            scale: 1.1,
                          }}
                          layout
                          layoutId={`${card?.id}`}
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
          <motion.div key={card?.id} className="absolute" animate layout>
            <PlayingCard card={card} faceDown={faceDown} layoutId={card?.id} />
          </motion.div>
        ))}
      </AnimatePresence>
    </div>
  );
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
