import { useQuery } from "convex/react";
import { Avatar } from "@astryxdesign/core/Avatar";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { List, ListItem } from "@astryxdesign/core/List";
import { Text } from "@astryxdesign/core/Text";
import { Timestamp } from "@astryxdesign/core/Timestamp";
import { api } from "../../convex/_generated/api.js";
import type { Id } from "../../convex/_generated/dataModel.js";
import type { Activity, Task } from "../model/actions.js";

type ActivityFeedProps = {
  /** Which activity to show: a show's or a single task's. */
  of: { showId: Id<"shows"> } | { taskId: Id<"tasks"> };
  tasks: Pick<Task, "_id" | "title">[];
  isCompact?: boolean;
};

/** Newest first: who did what, and when. */
export function ActivityFeed({ of, tasks, isCompact }: ActivityFeedProps) {
  const entries = useQuery(api.activity.list, { of, limit: isCompact ? 20 : 100 });
  if (!entries) return null;
  if (entries.length === 0) {
    return <EmptyState title="Nothing has happened yet" isCompact={isCompact} />;
  }
  const titles = new Map(tasks.map((task) => [task._id, task.title]));
  return (
    <List density={isCompact ? "compact" : "balanced"} hasDividers={!isCompact}>
      {entries.map((entry) => {
        const actor = entry.actor?.name ?? "Former crew";
        return (
          <ListItem
            key={entry._id}
            startContent={<Avatar name={actor} size="sm" />}
            label={
              <Text>
                <Text weight="semibold">{actor}</Text> {describe(entry, titles.get(entry.taskId))}
              </Text>
            }
            description={
              <Timestamp
                value={new Date(entry._creationTime).toISOString()}
                format="relative"
                type="supporting"
                color="secondary"
                isLive
              />
            }
          />
        );
      })}
    </List>
  );
}

function describe(entry: Activity, taskTitle = "a removed task") {
  switch (entry.kind) {
    case "created":
      return entry.detail ? `added “${taskTitle}” to ${entry.detail}` : `added “${taskTitle}”`;
    case "moved":
      return `moved “${taskTitle}” to ${entry.detail}`;
    case "assigned":
      return `assigned “${taskTitle}” to ${entry.detail}`;
    case "renamed":
      return `renamed a task to “${entry.detail}”`;
    case "commented":
      return `commented on “${taskTitle}”`;
    case "deleted":
      return `deleted “${entry.detail ?? taskTitle}”`;
  }
}
