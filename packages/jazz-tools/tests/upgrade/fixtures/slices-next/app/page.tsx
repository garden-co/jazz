import { app, boardsOnly, full, MAX_ITEMS, type Card, type CardStatus } from "@/schema";

export const cards = full.slice("cards").cards.limit(MAX_ITEMS);
export const boards = boardsOnly.boards.where({ name: { contains: "x" } });
export const all = app.cards;

export function label(card: Card, status: CardStatus): string {
  return `${card.title}: ${status}`;
}
