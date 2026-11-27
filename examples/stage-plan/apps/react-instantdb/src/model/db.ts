import { init, type InstaQLEntity, type TransactionChunk } from "@instantdb/react";
import schema, {
  type AppSchema,
  type TaskStatus,
  type ActivityKind,
} from "../../instant.schema.js";

export { TASK_STATUSES, type TaskStatus, type ActivityKind } from "../../instant.schema.js";

export const db = init({
  appId: import.meta.env.VITE_INSTANT_APP_ID,
  schema,
  useDateObjects: true,
});

// Rows in the shapes the UI works with (the Jazz app's row types): the ids of
// linked rows are flattened into `showId`, `crewId` and so on.
export type Crew = InstaQLEntity<AppSchema, "crew">;
export type Show = InstaQLEntity<AppSchema, "shows">;
export type ShowCrew = {
  id: string;
  showId: string;
  crewId: string;
  account: string;
  role: "chief" | "crew";
};
export type Task = Omit<TaskRow, "updatedAt" | "assigneeId" | "notes"> & {
  showId: string;
  status: TaskStatus;
  assigneeId: string | null;
  notes: string | null;
  $updatedAt: Date;
};
export type Activity = {
  id: string;
  showId: string;
  taskId: string;
  actorId: string;
  kind: ActivityKind;
  detail?: string | null;
  $createdAt: Date;
  actor?: Crew | null;
};
export type ChecklistItem = InstaQLEntity<AppSchema, "checklistItems">;

type Listener = (event: { code: "permission_denied" | "error"; error: unknown }) => void;
const listeners = new Set<Listener>();

/** Tells `listener` whenever the server rejects a transaction (Instant rolls it back). */
export function onMutationError(listener: Listener) {
  listeners.add(listener);
  return () => void listeners.delete(listener);
}

/** `db.transact`, reporting a rejected write to `onMutationError` listeners. */
export function transact(chunks: TransactionChunk<any, any> | TransactionChunk<any, any>[]) {
  const write = db.transact(chunks);
  write.catch((error) => {
    const denied = String((error as { message?: string })?.message).includes("Permission denied");
    for (const listener of listeners)
      listener({ code: denied ? "permission_denied" : "error", error });
  });
  return write;
}

type TaskRow = InstaQLEntity<AppSchema, "tasks", {}, undefined, true>;
export function toTask(row: TaskRow, showId: string): Task {
  const { updatedAt, ...rest } = row;
  return {
    ...rest,
    showId,
    status: row.status as TaskStatus,
    assigneeId: row.assigneeId ?? null,
    notes: row.notes ?? null,
    $updatedAt: updatedAt,
  };
}

type ShowCrewRow = InstaQLEntity<AppSchema, "showCrew", { crew: {} }>;
export function toCrewMember(row: ShowCrewRow, showId: string) {
  return {
    id: row.id,
    showId,
    crewId: row.crew?.id ?? "",
    account: row.account,
    role: row.role as ShowCrew["role"],
    crew: row.crew ?? null,
  };
}
