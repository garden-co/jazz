import { co, z } from "jazz-tools";

export const ReactionTypes = [
  "aww",
  "love",
  "haha",
  "wow",
  "tiny",
  "chonkers",
] as const;

export type ReactionType = (typeof ReactionTypes)[number];

export const Reactions = co.feed(z.literal([...ReactionTypes]));
