// The app's writes: each is an RPC call to the API (app/api/[[...route]]).
// `db.mutate()` sends it and returns `{ server }`, which says whether the
// server accepted it. Pad toggles are optimistic: the pad flips at once and
// flips back (refetch) if the server refuses.
import { call } from "@/lib/data/api";
import { queryClient } from "@/lib/data/live";
import type { MemberRole, Step } from "@/schema";

export type MutatorResultDetails = { type: "success" } | { type: "error"; error: { message: string; status?: number } };
export type MutatorResult = { server: Promise<MutatorResultDetails> };
type Optimistic = { key: readonly unknown[]; patch: (data: any) => any };
export type Mutation = { name: string; args: object; optimistic?: Optimistic };

const m =
  <A extends object>(name: string, optimistic?: (args: A) => Optimistic) =>
  (args: A): Mutation => ({ name, args, optimistic: optimistic?.(args) });

type Id = string;
type TrackFields = { position: number; name: string; color: string; instrument: string };
type Observation = { id: Id; session_id: Id; playing: boolean; bar: number; observed_at: number; tempo_bpm: number; pattern_id?: Id | null };

export const mutators = {
  sessions: {
    create: m<{
      id: Id;
      title: string;
      tempo_bpm: number;
      memberId: Id;
      pattern: { id: Id; length: number };
      tracks: ({ id: Id } & TrackFields)[];
      steps: Step[];
      observationId: Id;
    }>("sessions.create"),
  },
  members: {
    add: m<{ id: Id; session_id: Id; member_author: string; role: Exclude<MemberRole, "owner"> }>("members.add"),
    remove: m<{ id: Id }>("members.remove"),
  },
  tracks: {
    add: m<{ id: Id; session_id: Id } & TrackFields>("tracks.add"),
    update: m<{ id: Id; name?: string; instrument?: string; volume?: number; muted?: boolean; solo?: boolean }>("tracks.update"),
    remove: m<{ id: Id }>("tracks.remove"),
  },
  patterns: {
    add: m<{ id: Id; session_id: Id; position: number; name: string; length: number }>("patterns.add"),
    setLength: m<{ id: Id; length: number }>("patterns.setLength"),
  },
  steps: {
    set: m<Step>("steps.set", (step) => ({
      key: ["session", step.session_id, "steps", step.pattern_id],
      patch: (steps: Step[]) => [...steps.filter((s) => s.id !== step.id), step].sort((a, b) => a.position - b.position),
    })),
  },
  transport: { observe: m<Observation>("transport.observe") },
  presence: {
    beat: m<{ id: Id; session_id: Id; profile_id: Id; cursor_step: number; heartbeat_at: number }>("presence.beat"),
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
      (error: Error & { status?: number }): MutatorResultDetails => {
        if (optimistic) void queryClient.invalidateQueries({ queryKey: optimistic.key });
        return { type: "error", error };
      },
    );
    return { server };
  },
};
