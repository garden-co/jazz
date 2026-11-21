import { useQuery } from "../model/live.js";
import { Avatar } from "@astryxdesign/core/Avatar";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { List, ListItem } from "@astryxdesign/core/List";
import { Text } from "@astryxdesign/core/Text";
import { Timestamp } from "@astryxdesign/core/Timestamp";
import { queries } from "../../queries.js";
import type { Activity, Task } from "../../schema.js";

type ActivityFeedProps = {
  /** Which activity to show: a show's or a single task's. */
  showId: string;
  taskId?: string;
  tasks: Pick<Task, "id" | "title">[];
  isCompact?: boolean;
};

/** Newest first: who did what, and when. */
export function ActivityFeed({ showId, taskId, tasks, isCompact }: ActivityFeedProps) {
  const [entries, result] = useQuery(
    queries.activity.feed({ showId, taskId, limit: isCompact ? 20 : 100 }),
  );
  if (entries.length === 0 && result.type === "unknown") return null;
  if (entries.length === 0) {
    return <EmptyState title="Nothing has happened yet" isCompact={isCompact} />;
  }
  const titles = new Map(tasks.map((task) => [task.id, task.title]));
  return (
    <List density={isCompact ? "compact" : "balanced"} hasDividers={!isCompact}>
      {entries.map((entry) => {
        const actor = entry.actor?.name ?? "Former crew";
        return (
          <ListItem
            key={entry.id}
            startContent={<Avatar name={actor} size="sm" />}
            label={
              <Text>
                <Text weight="semibold">{actor}</Text> {describe(entry, titles.get(entry.taskId))}
              </Text>
            }
            description={
              <Timestamp
                value={new Date(entry.createdAt).toISOString()}
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
