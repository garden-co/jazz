import { Button } from "@astryxdesign/core/Button";
import { List, ListItem } from "@astryxdesign/core/List";
import { ProgressBar } from "@astryxdesign/core/ProgressBar";
import { formatBytes } from "../large-values.js";
import type { UploadTask } from "../use-uploads.js";

interface UploadQueueProps {
  tasks: readonly UploadTask[];
  onCancel: (id: string) => void;
  onDismiss: (id: string) => void;
}

export function UploadQueue({ tasks, onCancel, onDismiss }: UploadQueueProps) {
  if (tasks.length === 0) return null;
  return (
    <List aria-label="Uploads" density="compact">
      {tasks.map((task) =>
        task.status === "failed" ? (
          <ListItem
            key={task.id}
            label={task.name}
            description={`Upload failed: ${task.error}`}
            endContent={
              <Button
                label="Dismiss"
                variant="ghost"
                size="sm"
                onClick={() => onDismiss(task.id)}
              />
            }
          />
        ) : (
          <ListItem
            key={task.id}
            label={
              <ProgressBar
                label={task.name}
                value={task.uploaded}
                max={Math.max(task.size, 1)}
                hasValueLabel
                formatValueLabel={(value, max) =>
                  task.status === "saving"
                    ? `Saving ${formatBytes(task.size)}…`
                    : `${formatBytes(value)} of ${formatBytes(task.size === 0 ? 0 : max)}`
                }
              />
            }
            endContent={
              <Button label="Cancel" variant="ghost" size="sm" onClick={() => onCancel(task.id)} />
            }
          />
        ),
      )}
    </List>
  );
}
