import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";

export const MAX_ITEMS = 50;

const schema = {
  boards: s.table({ name: s.string() }, { cardsViaBoard: s.reverse("cards", "board") }),
  cards: s.table(
    {
      boardId: s.uuid(),
      title: s.string(),
      status: s.enum("todo", "doing", "done"),
    },
    { board: s.rel("boards", "boardId") },
  ),
};

export const full = s.defineSliceableApp(schema, { migrations });

export const app = full.slice("boards", "cards");
export const boardsOnly = full.slice("boards");

export type Board = s.RowOf<typeof app.boards>;
export type Card = s.RowOf<typeof app.cards>;
export type CardStatus = Card["status"];
export type CardWithBoard = s.RowOf<ReturnType<typeof app.cards.include<{ board: true }>>>;
export type CardDraft = s.InsertOf<typeof app.cards> & { draft: true };
