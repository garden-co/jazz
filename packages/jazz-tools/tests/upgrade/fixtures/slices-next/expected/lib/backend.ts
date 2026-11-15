import { createJazz, memory } from "jazz-tools/backend";
import { app } from "@/jazz.generated";
import permissions from "@/jazz.permissions.json" with { type: "json" };
import type { Board, CardInsert } from "@/jazz.generated";

export const jazz = createJazz({ app, permissions, store: memory() });

export type Draft = CardInsert;
export type BoardRow = Board;
