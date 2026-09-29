import { Button } from "@astryxdesign/core/Button";
import { ProgressBar } from "@astryxdesign/core/ProgressBar";
import { HStack, StackItem, VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
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
    <VStack gap={3} as="ul" aria-label="Uploads" className="plain-list">
      {tasks.map((task) => (
        <HStack key={task.id} as="li" gap={3} vAlign="end">
          <StackItem size="fill">
            {task.status === "failed" ? (
              <VStack gap={1}>
                <Text weight="medium" maxLines={1}>
                  {task.name}
                </Text>
                <Text type="supporting" color="secondary">
                  Upload failed: {task.error}
                </Text>
              </VStack>
            ) : (
              <ProgressBar
                label={task.name}
                value={task.uploaded}
                max={Math.max(task.size, 1)}
                hasValueLabel
                formatValueLabel={(value, max) =>
                  `${formatBytes(value)} of ${formatBytes(task.size === 0 ? 0 : max)}`
                }
              />
            )}
          </StackItem>
          {task.status === "failed" ? (
            <Button label="Dismiss" variant="ghost" size="sm" onClick={() => onDismiss(task.id)} />
          ) : (
            <Button label="Cancel" variant="ghost" size="sm" onClick={() => onCancel(task.id)} />
          )}
        </HStack>
      ))}
    </VStack>
  );
}
