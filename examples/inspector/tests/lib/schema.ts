import { co, z } from "jazz-tools";

export const ReactionTypes = ["thumb-up", "thumb-down"] as const;

export type ReactionType = (typeof ReactionTypes)[number];

export const ReactionsList = co.feed(z.literal([...ReactionTypes]));

export const Issue = co.map({
  title: z.string(),
  status: z.enum(["open", "closed"]),
  labels: co.list(z.string()),
  reactions: ReactionsList,
  file: co.optional(co.fileStream()),
  image: co.optional(co.image()),
  lead: co.optional(co.account()),
});

export const Project = co.map({
  name: z.string(),
  description: z.string(),
  issues: co.list(Issue),
});

export const Organization = co.map({
  name: z.string(),
  projects: co.list(Project),
  image: co.image(),
});
