import { useEffect, useState } from "react";
import { useQuery } from "convex/react";
import { Avatar } from "@astryxdesign/core/Avatar";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { Divider } from "@astryxdesign/core/Divider";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Grid } from "@astryxdesign/core/Grid";
import { Layout, LayoutContent, LayoutFooter } from "@astryxdesign/core/Layout";
import { Selector } from "@astryxdesign/core/Selector";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { TextArea } from "@astryxdesign/core/TextArea";
import { TextInput } from "@astryxdesign/core/TextInput";
import { Timestamp } from "@astryxdesign/core/Timestamp";
import { api } from "../../convex/_generated/api.js";
import {
  rankAfter,
  STATUS_LABELS,
  TASK_STATUSES,
  type CrewMember,
  type Task,
  type TaskStatus,
} from "../model/actions.js";
import { useWrite } from "../model/write.js";
import { ActivityFeed } from "./ActivityFeed.js";


type TaskDialogProps = {
  task?: Task;
  /** Every task on the show, to rank a task at the end of its new column. */
  tasks: Task[];
  crew: CrewMember[];
  /** The route names a task this person can't see (deleted, or another show's). */
  isMissing: boolean;
  onClose: () => void;
};

const UNASSIGNED = "unassigned";

export function TaskDialog({ task, tasks, crew, isMissing, onClose }: TaskDialogProps) {
  const isOpen = Boolean(task) || isMissing;
  return (
    <Dialog isOpen={isOpen} onOpenChange={(open) => !open && onClose()} width={640}>
      {task ? (
        <TaskDetail key={task._id} task={task} tasks={tasks} crew={crew} onClose={onClose} />
      ) : (
        <Layout
          header={<DialogHeader title="Task not found" onOpenChange={onClose} />}
          content={
            <LayoutContent>
              <EmptyState
                title="This task is gone"
                description="It was deleted, or it belongs to a show you're not on."
              />
            </LayoutContent>
          }
        />
      )}
    </Dialog>
  );
}

function TaskDetail({
  task,
  tasks,
  crew,
  onClose,
}: {
  task: Task;
  tasks: Task[];
  crew: CrewMember[];
  onClose: () => void;
}) {
  const [title, setTitle] = useState(task.title);
  const [notes, setNotes] = useState(task.notes ?? "");
  // The notes as they were when you started typing.
  const [notesBase, setNotesBase] = useState<string | null>(task.notes ?? null);
  const [comment, setComment] = useState("");
  const comments = useQuery(api.comments.list, { taskId: task._id }) ?? [];
  // Deleting is for the chief or the task's creator. Ask the server before offering it.
  const canDelete = useQuery(api.tasks.canDelete, { taskId: task._id }) ?? false;
  const renameTask = useWrite(api.tasks.rename);
  const setTaskNotes = useWrite(api.tasks.setNotes);
  const moveTask = useWrite(api.tasks.move);
  const assignTask = useWrite(api.tasks.assign);
  const addComment = useWrite(api.comments.add);
  const deleteTask = useWrite(api.tasks.remove);

  // Remote edits replace the local draft unless you're typing in the field.
  const [focused, setFocused] = useState<"title" | "notes" | null>(null);
  useEffect(() => {
    if (focused !== "title") setTitle(task.title);
  }, [task.title, focused]);
  useEffect(() => {
    if (focused !== "notes") setNotes(task.notes ?? "");
  }, [task.notes, focused]);

  const commitTitle = () => {
    setFocused(null);
    const next = title.trim();
    if (next && next !== task.title) void renameTask({ taskId: task._id, title: next });
    else setTitle(task.title);
  };
  const commitNotes = () => {
    setFocused(null);
    if (notes !== (notesBase ?? "")) void setTaskNotes({ taskId: task._id, notes });
  };

  const crewOptions = [
    { value: UNASSIGNED, label: "Nobody yet" },
    ...crew.flatMap((member) =>
      member.crew ? [{ value: member.crew._id, label: member.crew.name }] : [],
    ),
  ];

  return (
    <Layout
      header={<DialogHeader title={task.title} onOpenChange={onClose} />}
      content={
        <LayoutContent>
          <VStack gap={5}>
            <TextInput
              label="Task"
              value={title}
              onChange={setTitle}
              onFocus={() => setFocused("title")}
              onBlur={commitTitle}
              onEnter={commitTitle}
            />
            <Grid columns={{ minWidth: 200 }} gap={4}>
              <Selector
                label="Status"
                value={task.status}
                options={TASK_STATUSES.map((status) => ({
                  value: status,
                  label: STATUS_LABELS[status],
                }))}
                onChange={(status) => {
                  const next = status as TaskStatus;
                  if (next !== task.status) {
                    void moveTask({
                      taskId: task._id,
                      status: next,
                      rank: rankAfter(tasks.filter((t) => t.status === next)),
                    });
                  }
                }}
              />
              <Selector
                label="Assigned to"
                value={task.assigneeId ?? UNASSIGNED}
                options={crewOptions}
                onChange={(value) => {
                  const assignee = crew.find((member) => member.crew?._id === value)?.crew ?? null;
                  if ((assignee?._id ?? null) !== (task.assigneeId ?? null)) {
                    void assignTask({ taskId: task._id, assigneeId: assignee?._id ?? null });
                  }
                }}
              />
            </Grid>
            <TextArea
              label="Notes"
              isOptional
              value={notes}
              onChange={setNotes}
              onFocus={() => {
                setNotesBase(task.notes ?? null);
                setFocused("notes");
              }}
              onBlur={commitNotes}
              rows={3}
            />
            <Text color="secondary" type="supporting">
              Last changed{" "}
              <Timestamp value={new Date(task.updatedAt).toISOString()} format="relative" isLive />
            </Text>

            <Divider />
            <VStack gap={3}>
              <Heading level={3}>Comments</Heading>
              {comments.length === 0 && <Text color="secondary">No comments yet.</Text>}
              {comments.map((entry) => (
                <HStack key={entry._id} gap={3} vAlign="start">
                  <Avatar name={entry.author?.name ?? "Former crew"} size="sm" />
                  <VStack gap={0.5}>
                    <HStack gap={2} vAlign="center" wrap="wrap">
                      <Text weight="semibold">{entry.author?.name ?? "Former crew"}</Text>
                      <Timestamp
                        value={new Date(entry._creationTime).toISOString()}
                        format="relative"
                        type="supporting"
                        color="secondary"
                        isLive
                      />
                    </HStack>
                    <Text as="p">{entry.body}</Text>
                  </VStack>
                </HStack>
              ))}
              <form
                onSubmit={(event) => {
                  event.preventDefault();
                  if (!comment.trim()) return;
                  void addComment({ taskId: task._id, body: comment.trim() });
                  setComment("");
                }}
              >
                <VStack gap={2}>
                  <TextArea
                    label="Add a comment"
                    isLabelHidden
                    placeholder="Add a comment for the crew"
                    value={comment}
                    onChange={setComment}
                    rows={2}
                  />
                  <HStack hAlign="end">
                    <Button label="Comment" type="submit" isDisabled={!comment.trim()} />
                  </HStack>
                </VStack>
              </form>
            </VStack>

            <Divider />
            <VStack gap={3}>
              <Heading level={3}>Activity</Heading>
              <ActivityFeed
                of={{ taskId: task._id }}
                tasks={[task]}
                isCompact
              />
            </VStack>
          </VStack>
        </LayoutContent>
      }
      footer={
        canDelete ? (
          <LayoutFooter hasDivider>
            <HStack hAlign="end">
              <Button
                label="Delete task"
                variant="destructive"
                onClick={() => {
                  void deleteTask({ taskId: task._id });
                  onClose();
                }}
              />
            </HStack>
          </LayoutFooter>
        ) : undefined
      }
    />
  );
}
