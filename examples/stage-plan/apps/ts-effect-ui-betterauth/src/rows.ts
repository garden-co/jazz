import { Schema } from "effect";
import { TASK_STATUSES } from "../app-extras";

/**
 * Effect Schemas of the rows StagePlan's components receive in messages.
 * Messages are Schemas in Effect UI, so they can be logged and replayed.
 */

export const Crew = Schema.Struct({
  id: Schema.String,
  account: Schema.String,
  name: Schema.String,
});

export const Show = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  venue: Schema.String,
  date: Schema.String,
  doors: Schema.String,
  chiefAccount: Schema.String,
});

export const TaskStatus = Schema.Literals(TASK_STATUSES);

export const Task = Schema.Struct({
  id: Schema.String,
  showId: Schema.String,
  title: Schema.String,
  status: TaskStatus,
  assigneeId: Schema.NullOr(Schema.String),
  notes: Schema.NullOr(Schema.String),
  rank: Schema.Number,
});
