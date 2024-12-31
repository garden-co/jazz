import type { Card, Suit } from "@/schema";
import type { co } from "jazz-tools";
import { motion } from "motion/react";

import { cn } from "@/lib/utils";
import bastoni from "../img/bastoni.svg?url";
import coppe from "../img/coppe.svg?url";
import denari from "../img/denari.svg?url";
import spade from "../img/spade.svg?url";

interface Props {
  card: co<Card>;
  faceDown?: boolean;
  className?: string;
  layoutId?: string;
}
export function PlayingCard({
  card,
  className,
  faceDown = false,
  layoutId,
}: Props) {
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
