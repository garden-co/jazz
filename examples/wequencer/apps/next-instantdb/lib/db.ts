import { init, type InstaQLEntity } from "@instantdb/react";
import schema, { type AppSchema } from "@/instant.schema";

export { type Instrument, type MemberRole } from "@/instant.schema";

export const db = init({
  appId: process.env.NEXT_PUBLIC_INSTANT_APP_ID!,
  schema,
  useDateObjects: true,
});

type Entity<E extends keyof AppSchema["entities"]> = InstaQLEntity<
  AppSchema,
  E,
  {},
  undefined,
  true
>;
export type Session = Entity<"sessions">;
export type Track = Entity<"tracks">;
export type Pattern = Entity<"patterns">;
export type Step = Entity<"steps">;
