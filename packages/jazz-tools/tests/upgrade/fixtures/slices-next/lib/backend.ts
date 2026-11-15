import { createJazz, memory } from "jazz-tools/backend";
import { schema as s } from "jazz-tools";
import { app } from "@/schema";
import permissions from "@/permissions";

export const jazz = createJazz({ app, permissions, store: memory() });

export type Draft = s.InsertOf<typeof app.cards>;
export type BoardRow = s.RowOf<typeof app.boards>;
