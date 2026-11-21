// The app's writes: each is an RPC call to the server (server/writes.ts).
// `db.mutate()` sends it and returns `{ server }`, which says whether the
// server accepted it. Sending a message is optimistic: it shows at once and
// is dropped again if the server refuses it.
import type { Message } from "../../shared/types";
import { call } from "./api";
import { queryClient } from "./live";

export type MutatorResultDetails = { type: "success" } | { type: "error"; error: { message: string } };
export type MutatorResult = { server: Promise<MutatorResultDetails> };
type Optimistic = { key: readonly unknown[]; patch: (data: any) => any };
export type Mutation = { name: string; args: object; optimistic?: Optimistic };

const m =
  <A extends object>(name: string, optimistic?: (args: A) => Optimistic) =>
  (args: A): Mutation => ({ name, args, optimistic: optimistic?.(args) });

type Id = string;
export const mutators = {
  profiles: {
    create: m<{ id: Id; displayName: string }>("profiles.create"),
    update: m<{ id: Id; displayName: string }>("profiles.update"),
  },
  rooms: {
    create: m<{ id: Id; memberId: Id; name: string; profileId: Id }>("rooms.create"),
    rename: m<{ id: Id; name: string }>("rooms.rename"),
  },
  members: {
    admit: m<{ id: Id; roomId: Id; memberAuthor: Id; memberProfileId: Id }>("members.admit"),
    remove: m<{ id: Id }>("members.remove"),
  },
  joinRequests: {
    create: m<{ id: Id; roomId: Id; profileId: Id }>("joinRequests.create"),
    delete: m<{ id: Id }>("joinRequests.delete"),
  },
  readMarkers: { mark: m<{ id: Id; roomId: Id; lastReadAt: number }>("readMarkers.mark") },
  messages: {
    send: m<{ id: Id; roomId: Id; senderId: Id; text: string }>("messages.send", (args) => ({
      key: ["room", args.roomId, "messages"],
      patch: (newestFirst: Message[]) => [{ ...args, canvasId: null, createdAt: Date.now() }, ...newestFirst],
    })),
    delete: m<{ id: Id }>("messages.delete"),
  },
  reactions: { toggle: m<{ id: Id; roomId: Id; messageId: Id; emoji: string }>("reactions.toggle") },
  sketches: {
    start: m<{ canvasId: Id; messageId: Id; roomId: Id; senderId: Id }>("sketches.start"),
    draw: m<{ id: Id; canvasId: Id; roomId: Id; color: string; width: number; points: number[] }>("sketches.draw"),
    undo: m<{ id: Id }>("sketches.undo"),
  },
};

export const db = {
  mutate({ name, args, optimistic }: Mutation): MutatorResult {
    const sent = call(name, args);
    if (optimistic) {
      // As in TanStack's optimistic-update docs: stop refetches that would
      // overwrite the patch, then patch the cache.
      const { key, patch } = optimistic;
      void queryClient.cancelQueries({ queryKey: key }).then(() =>
        queryClient.setQueriesData({ queryKey: key }, (data) => (data === undefined ? data : patch(data))),
      );
    }
    const server = sent.then(
      (): MutatorResultDetails => ({ type: "success" }),
      (error: Error): MutatorResultDetails => {
        if (optimistic) void queryClient.invalidateQueries({ queryKey: optimistic.key });
        return { type: "error", error };
      },
    );
    return { server };
  },
};
