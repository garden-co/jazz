"use client";

import { useAll, useDb } from "jazz-tools/react";
import { app } from "../schema";

const PAGE_SIZE = 12;

/** A composed surface; each child owns only the query needed to render it. */
export function BinderWorkspace({
  workspaceId,
  rootPageId,
}: {
  workspaceId: string;
  rootPageId: string;
}) {
  return (
    <section className="binder" aria-label="Band binder">
      <PageNavigation workspaceId={workspaceId} parentPageId={rootPageId} />
      <Page pageId={rootPageId} workspaceId={workspaceId} />
      <TaskList workspaceId={workspaceId} />
      <Calendar workspaceId={workspaceId} />
      <SongIndex workspaceId={workspaceId} />
    </section>
  );
}

export function PageNavigation({
  workspaceId,
  parentPageId,
  offset = 0,
}: {
  workspaceId: string;
  parentPageId: string;
  offset?: number;
}) {
  const { data: pages = [] } = useAll(
    app.pages
      .where({ workspaceId, parentPageId })
      .orderBy("title", "asc")
      .offset(offset)
      .limit(PAGE_SIZE),
  );
  return (
    <nav aria-label="Child pages">
      {pages.map((page) => (
        <a href={`#page-${page.id}`} key={page.id}>
          {page.title}
        </a>
      ))}
    </nav>
  );
}

export function Page({ pageId, workspaceId }: { pageId: string; workspaceId: string }) {
  const { data: pages = [] } = useAll(app.pages.where({ id: pageId, workspaceId }).limit(1));
  const page = pages[0];
  if (!page) return <p>Loading page…</p>;
  return (
    <article id={`page-${page.id}`}>
      <h2>{page.title}</h2>
      <BlockList pageId={page.id} workspaceId={workspaceId} />
    </article>
  );
}

export function BlockList({
  pageId,
  workspaceId,
  offset = 0,
}: {
  pageId: string;
  workspaceId: string;
  offset?: number;
}) {
  const { data: blocks = [] } = useAll(
    app.blocks
      .where({ pageId, workspaceId })
      .orderBy("position", "asc")
      .offset(offset)
      .limit(PAGE_SIZE),
  );
  return (
    <ol aria-label="Page blocks" start={offset + 1}>
      {blocks.map((block) => (
        <li key={block.id}>
          <strong>{block.kind}</strong> {JSON.stringify(block.payload)}
          <SuggestionList workspaceId={workspaceId} blockId={block.id} />
          {block.kind === "attachment" && (
            <AttachmentList workspaceId={workspaceId} blockId={block.id} />
          )}
        </li>
      ))}
    </ol>
  );
}

export function TaskList({ workspaceId }: { workspaceId: string }) {
  const db = useDb();
  const { data: tasks = [] } = useAll(
    app.tasks.where({ workspaceId }).orderBy("dueAt", "asc").limit(PAGE_SIZE),
  );
  return (
    <section aria-label="Tasks">
      <h2>Tasks</h2>
      {tasks.map((task) => (
        <label key={task.id}>
          <input
            type="checkbox"
            checked={task.completed}
            onChange={() => db.update(app.tasks, task.id, { completed: !task.completed })}
          />
          {task.title}
        </label>
      ))}
    </section>
  );
}

export function Calendar({ workspaceId }: { workspaceId: string }) {
  const { data: events = [] } = useAll(
    app.calendarEvents.where({ workspaceId }).orderBy("startsAt", "asc").limit(PAGE_SIZE),
  );
  return (
    <section aria-label="Calendar">
      <h2>Calendar</h2>
      {events.map((event) => (
        <p key={event.id}>
          <time dateTime={event.startsAt.toISOString()}>{event.startsAt.toLocaleString()}</time>{" "}
          {event.title}
        </p>
      ))}
    </section>
  );
}

export function SongIndex({ workspaceId }: { workspaceId: string }) {
  const { data: songs = [] } = useAll(
    app.songs.where({ workspaceId }).orderBy("title", "asc").limit(PAGE_SIZE),
  );
  return (
    <section aria-label="Songs">
      <h2>Songs</h2>
      {songs.map((song) => (
        <p key={song.id}>
          {song.title} {song.key && <small>in {song.key}</small>}
        </p>
      ))}
    </section>
  );
}

export function SuggestionList({ workspaceId, blockId }: { workspaceId: string; blockId: string }) {
  const { data: suggestions = [] } = useAll(
    app.suggestions
      .where({ workspaceId, blockId, status: "open" })
      .select("*", "$createdAt")
      .orderBy("$createdAt", "asc")
      .limit(PAGE_SIZE),
  );
  return (
    <aside aria-label="Open suggestions">
      {suggestions.map((suggestion) => (
        <p key={suggestion.id}>{JSON.stringify(suggestion.payload)}</p>
      ))}
    </aside>
  );
}

export function AttachmentList({ workspaceId, blockId }: { workspaceId: string; blockId: string }) {
  const { data: attachments = [] } = useAll(
    app.attachments.where({ workspaceId, blockId }).orderBy("name", "asc").limit(PAGE_SIZE),
  );
  return (
    <ul aria-label="Attachments">
      {attachments.map((attachment) => (
        <li key={attachment.id}>
          {attachment.name} ({attachment.mediaType}, {attachment.bytes.byteLength} bytes)
        </li>
      ))}
    </ul>
  );
}
