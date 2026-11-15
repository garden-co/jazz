import { app, app as boardsOnly, app as full, type Card, type CardStatus } from "@/jazz.generated";
import { MAX_ITEMS } from "@/app-extras";

export const cards = full.cards.limit(MAX_ITEMS);
export const boards = boardsOnly.boards.where({ name: { contains: "x" } });
export const all = app.cards;

export function label(card: Card, status: CardStatus): string {
  return `${card.title}: ${status}`;
}
