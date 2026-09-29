import { useEffect, useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
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
import { app, TASK_STATUSES, type Task, type TaskStatus } from "../../schema.js";
import {
  addComment,
  assignTask,
  deleteTask,
  moveTask,
  rankAfter,
  renameTask,
  STATUS_LABELS,
  updateTaskNotes,
  type CrewMember,
} from "../model/actions.js";
import { useMe } from "../model/me.js";
import { ActivityFeed } from "./ActivityFeed.js";

type TaskWithTime = Task & { $updatedAt: Date };

type TaskDialogProps = {
  task?: TaskWithTime;
  crew: CrewMember[];
  /** The route names a task this person can't see (deleted, or another show's). */
  isMissing: boolean;
  onClose: () => void;
};

const UNASSIGNED = "unassigned";

export function TaskDialog({ task, crew, isMissing, onClose }: TaskDialogProps) {
  const isOpen = Boolean(task) || isMissing;
  return (
    <Dialog isOpen={isOpen} onOpenChange={(open) => !open && onClose()} width={640}>
      {task ? (
        <TaskDetail key={task.id} task={task} crew={crew} onClose={onClose} />
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
  crew,
  onClose,
}: {
  task: TaskWithTime;
  crew: CrewMember[];
  onClose: () => void;
}) {
  const db = useDb();
  const me = useMe();
  const [title, setTitle] = useState(task.title);
  const [notes, setNotes] = useState(task.notes ?? "");
  const [comment, setComment] = useState("");
  const [canDelete, setCanDelete] = useState(false);
  const { data: comments = [] } = useAll(
    app.comments
      .where({ taskId: task.id })
      .select("*", "$createdAt")
      .include({ author: true })
      .orderBy("$createdAt", "asc"),
  );
  const { data: columnTasks = [] } = useAll(
    app.tasks.where({ showId: task.showId }).select("rank", "status"),
  );

  // Remote edits replace the local draft unless you're typing in the field.
  const [focused, setFocused] = useState<"title" | "notes" | null>(null);
  useEffect(() => {
    if (focused !== "title") setTitle(task.title);
  }, [task.title, focused]);
  useEffect(() => {
    if (focused !== "notes") setNotes(task.notes ?? "");
  }, [task.notes, focused]);

  // Deleting is for the chief or the task's creator. Ask Jazz before offering it.
  useEffect(() => {
    let cancelled = false;
    void db.canDelete(app.tasks, task.id).then((result) => {
      if (!cancelled) setCanDelete(result !== "denied");
    });
    return () => {
      cancelled = true;
    };
  }, [db, task.id]);

  const commitTitle = () => {
    setFocused(null);
    const next = title.trim();
    if (next && next !== task.title) void renameTask(db, me, task, next);
    else setTitle(task.title);
  };
  const commitNotes = () => {
    setFocused(null);
    if (notes !== (task.notes ?? "")) updateTaskNotes(db, task, notes);
  };

  const crewOptions = [
    { value: UNASSIGNED, label: "Nobody yet" },
    ...crew.flatMap((member) =>
      member.crew ? [{ value: member.crew.id, label: member.crew.name }] : [],
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
                    void moveTask(
                      db,
                      me,
                      task,
                      next,
                      rankAfter(columnTasks.filter((t) => t.status === next)),
                    );
                  }
                }}
              />
              <Selector
                label="Assigned to"
                value={task.assigneeId ?? UNASSIGNED}
                options={crewOptions}
                onChange={(value) => {
                  const assignee = crew.find((member) => member.crew?.id === value)?.crew ?? null;
                  if ((assignee?.id ?? null) !== (task.assigneeId ?? null)) {
                    void assignTask(db, me, task, assignee);
                  }
                }}
              />
            </Grid>
            <TextArea
              label="Notes"
              isOptional
              value={notes}
              onChange={setNotes}
              onFocus={() => setFocused("notes")}
              onBlur={commitNotes}
              rows={3}
            />
            <Text color="secondary" type="supporting">
              Last changed{" "}
              <Timestamp value={task.$updatedAt.toISOString()} format="relative" isLive />
            </Text>

            <Divider />
            <VStack gap={3}>
              <Heading level={3}>Comments</Heading>
              {comments.length === 0 && <Text color="secondary">No comments yet.</Text>}
              {comments.map((entry) => (
                <HStack key={entry.id} gap={3} vAlign="start">
                  <Avatar name={entry.author?.name ?? "Former crew"} size="sm" />
                  <VStack gap={0.5}>
                    <HStack gap={2} vAlign="center" wrap="wrap">
                      <Text weight="semibold">{entry.author?.name ?? "Former crew"}</Text>
                      <Timestamp
                        value={entry.$createdAt.toISOString()}
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
                  void addComment(db, me, task, comment.trim());
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
                query={app.activity.where({ taskId: task.id })}
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
                  void deleteTask(db, me, task);
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
