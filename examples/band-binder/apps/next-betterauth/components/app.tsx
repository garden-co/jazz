"use client";

import { useState } from "react";
import { JazzProvider, useAll, useDb, useLocalFirstAuth, useSession } from "jazz-tools/react";
import { app } from "../schema";
import { bootstrapWorkspace } from "../bootstrap";
import { BinderWorkspace } from "./binder";

const appId = process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? "00000000-0000-0000-0000-000000000001";

export function BandBinderApp() {
  const auth = useLocalFirstAuth();
  if (!auth.secret) return <main>Creating a local identity…</main>;
  return (
    <JazzProvider
      config={{
        appId,
        env: "dev",
        secret: auth.secret,
        serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL,
      }}
      fallback={<main>Opening the local binder…</main>}
    >
      <WorkspaceShell />
    </JazzProvider>
  );
}

function WorkspaceShell() {
  const session = useSession();
  // The runtime's public session currently exposes JWT components, while
  // policies receive `session.author`. Store the same issuer-scoped logical
  // author used by the runtime — never a raw provider subject by itself.
  const author = session ? JSON.stringify([session.issuer, session.user_id]) : null;
  const { data: workspaces = [] } = useAll(app.workspaces.orderBy("name", "asc").limit(12));
  const selected = workspaces[0];
  return (
    <main>
      <p className="eyebrow">Jazz example · local-first Next.js variant</p>
      <h1>BandBinder</h1>
      <p>A shared band workspace for nested pages, tasks, calendars, songs, and attachments.</p>
      {!author ? (
        <p>Waiting for an identity…</p>
      ) : !selected ? (
        <BootstrapWorkspace author={author} />
      ) : (
        <Workspace workspaceId={selected.id} author={author} />
      )}
    </main>
  );
}

function BootstrapWorkspace({ author, workspaceId }: { author: string; workspaceId?: string }) {
  const db = useDb();
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const bootstrap = async () => {
    setCreating(true);
    setError(null);
    try {
      await bootstrapWorkspace(db, author, { workspaceId });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setCreating(false);
    }
  };
  return (
    <section aria-label="Create workspace">
      <h2>{workspaceId ? "Repair binder setup" : "Start a binder"}</h2>
      <p>Bootstrap is an explicit write action, separate from the initial workspace query.</p>
      {error ? <p role="alert">Could not create the binder: {error}</p> : null}
      <button disabled={creating} onClick={() => void bootstrap()}>
        {creating ? "Creating…" : "Create demo workspace"}
      </button>
    </section>
  );
}

function Workspace({ workspaceId, author }: { workspaceId: string; author: string }) {
  const { data: roots = [] } = useAll(
    app.pages.where({ workspaceId, parentPageId: null }).orderBy("title", "asc").limit(1),
  );
  return roots[0] ? (
    <ReadyWorkspace workspaceId={workspaceId} rootPageId={roots[0].id} author={author} />
  ) : (
    <BootstrapWorkspace author={author} workspaceId={workspaceId} />
  );
}

function ReadyWorkspace({
  workspaceId,
  rootPageId,
  author,
}: {
  workspaceId: string;
  rootPageId: string;
  author: string;
}) {
  const { data: blocks = [] } = useAll(
    app.blocks.where({ workspaceId, pageId: rootPageId }).limit(1),
  );
  return blocks[0] ? (
    <BinderWorkspace workspaceId={workspaceId} rootPageId={rootPageId} />
  ) : (
    <BootstrapWorkspace author={author} workspaceId={workspaceId} />
  );
}
