import { useEffect, useState, type DragEvent, type KeyboardEvent } from "react";
import { useDb } from "jazz-tools/react";
import { Avatar } from "@astryxdesign/core/Avatar";
import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { ClickableCard } from "@astryxdesign/core/ClickableCard";
import { Grid } from "@astryxdesign/core/Grid";
import { Kbd } from "@astryxdesign/core/Kbd";
import { Section } from "@astryxdesign/core/Section";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { TASK_STATUSES, type Task, type TaskStatus } from "../../schema.js";
import {
  addTask,
  moveTask,
  rankAfter,
  rankBetween,
  STATUS_LABELS,
  type CrewMember,
} from "../data/actions.js";
import { useMe } from "../data/me.js";
import { href } from "../router.js";

type BoardProps = { showId: string; tasks: Task[]; crew: CrewMember[] };

/**
 * The stage-prep board. Move a card by dragging it to another column, or
 * focus it and use the arrow keys: left and right change column, up and down
 * change its place in the column.
 */
export function Board({ showId, tasks, crew }: BoardProps) {
  const db = useDb();
  const me = useMe();
  const [dropTarget, setDropTarget] = useState<TaskStatus | null>(null);
  const [focusTaskId, setFocusTaskId] = useState<string | null>(null);
  const columns = TASK_STATUSES.map((status) => ({
    status,
    tasks: tasks.filter((task) => task.status === status),
  }));

  // Keep keyboard focus on a card after it moves to another column.
  useEffect(() => {
    if (!focusTaskId) return;
    document.querySelector<HTMLElement>(`[data-task-id="${focusTaskId}"] a`)?.focus();
  }, [focusTaskId, tasks]);

  const move = (task: Task, status: TaskStatus, rank: number) => {
    void moveTask(db, me, task, status, rank);
  };

  const onCardKeyDown = (event: KeyboardEvent, task: Task) => {
    const columnIndex = TASK_STATUSES.indexOf(task.status);
    const column = columns[columnIndex].tasks;
    const index = column.findIndex((other) => other.id === task.id);
    let handled = true;
    if (event.key === "ArrowLeft" && columnIndex > 0) {
      const target = TASK_STATUSES[columnIndex - 1];
      move(task, target, rankAfter(columns[columnIndex - 1].tasks));
    } else if (event.key === "ArrowRight" && columnIndex < TASK_STATUSES.length - 1) {
      const target = TASK_STATUSES[columnIndex + 1];
      move(task, target, rankAfter(columns[columnIndex + 1].tasks));
    } else if (event.key === "ArrowUp" && index > 0) {
      move(task, task.status, rankBetween(column[index - 2]?.rank, column[index - 1].rank));
    } else if (event.key === "ArrowDown" && index < column.length - 1) {
      move(task, task.status, rankBetween(column[index + 1].rank, column[index + 2]?.rank));
    } else {
      handled = false;
    }
    if (handled) {
      event.preventDefault();
      setFocusTaskId(task.id);
    }
  };

  const onDrop = (event: DragEvent, status: TaskStatus, beforeTask?: Task) => {
    event.preventDefault();
    event.stopPropagation();
    setDropTarget(null);
    const task = tasks.find(
      (candidate) => candidate.id === event.dataTransfer.getData("text/task-id"),
    );
    if (!task || task.id === beforeTask?.id) return;
    const column = columns[TASK_STATUSES.indexOf(status)].tasks.filter(
      (other) => other.id !== task.id,
    );
    if (!beforeTask) return move(task, status, rankAfter(column));
    const index = column.findIndex((other) => other.id === beforeTask.id);
    move(task, status, rankBetween(column[index - 1]?.rank, beforeTask.rank));
  };

  return (
    <VStack gap={4}>
      <AddTaskForm
        onAdd={(title) => addTask(db, me, showId, title, "todo", rankAfter(columns[0].tasks))}
      />
      <Grid columns={{ minWidth: 240 }} gap={4} align="start">
        {columns.map(({ status, tasks: columnTasks }) => (
          <Section
            key={status}
            variant="muted"
            padding={3}
            className="board-column"
            aria-label={STATUS_LABELS[status]}
            data-status={status}
            data-drop-target={dropTarget === status ? "true" : undefined}
            onDragOver={(event) => {
              event.preventDefault();
              setDropTarget(status);
            }}
            onDragLeave={(event) => {
              if (!event.currentTarget.contains(event.relatedTarget as Node | null))
                setDropTarget(null);
            }}
            onDrop={(event) => onDrop(event, status)}
          >
            <VStack gap={3}>
              <HStack gap={2} vAlign="center" justify="between">
                <Text weight="semibold" as="h2">
                  {STATUS_LABELS[status]}
                </Text>
                <Badge
                  label={columnTasks.length}
                  variant={status === "blocked" && columnTasks.length ? "warning" : "neutral"}
                />
              </HStack>
              {columnTasks.map((task) => (
                <TaskCard
                  key={task.id}
                  task={task}
                  assignee={crew.find((member) => member.crewId === task.assigneeId)?.crew}
                  onKeyDown={(event) => onCardKeyDown(event, task)}
                  onDrop={(event) => onDrop(event, status, task)}
                />
              ))}
              {columnTasks.length === 0 && (
                <Text color="secondary" type="supporting">
                  Nothing here yet
                </Text>
              )}
            </VStack>
          </Section>
        ))}
      </Grid>
      <Text color="secondary" type="supporting">
        Drag a card to another column, or focus it and press <Kbd keys="left" />{" "}
        <Kbd keys="right" /> to move it and <Kbd keys="up" /> <Kbd keys="down" /> to reorder.
      </Text>
    </VStack>
  );
}

type TaskCardProps = {
  task: Task;
  assignee?: CrewMember["crew"];
  onKeyDown: (event: KeyboardEvent) => void;
  onDrop: (event: DragEvent) => void;
};

function TaskCard({ task, assignee, onKeyDown, onDrop }: TaskCardProps) {
  return (
    <ClickableCard
      label={task.title}
      href={href.task(task.showId, task.id)}
      padding={3}
      draggable
      data-task-id={task.id}
      onDragStart={(event) => {
        event.dataTransfer.setData("text/task-id", task.id);
        event.dataTransfer.effectAllowed = "move";
      }}
      onDragOver={(event) => event.preventDefault()}
      onDrop={onDrop}
      onKeyDown={onKeyDown}
    >
      <HStack gap={2} vAlign="center" justify="between">
        <Text>{task.title}</Text>
        {assignee && <Avatar name={assignee.name} size="xsm" />}
      </HStack>
    </ClickableCard>
  );
}

function AddTaskForm({ onAdd }: { onAdd: (title: string) => unknown }) {
  const [title, setTitle] = useState("");
  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        if (!title.trim()) return;
        onAdd(title.trim());
        setTitle("");
      }}
    >
      <HStack gap={2} vAlign="end">
        <TextInput
          label="New task"
          isLabelHidden
          placeholder="Add a task to To do, like “Tape the set list to the floor”"
          value={title}
          onChange={setTitle}
          width="100%"
        />
        <Button label="Add task" type="submit" variant="primary" isDisabled={!title.trim()} />
      </HStack>
    </form>
  );
}
