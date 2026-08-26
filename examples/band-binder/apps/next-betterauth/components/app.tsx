"use client";

import { useState } from "react";
import { JazzProvider, useAll, useDb, useLocalFirstAuth, useSession } from "jazz-tools/react";
import { app } from "../schema";
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
        <Workspace workspaceId={selected.id} />
      )}
    </main>
  );
}

function BootstrapWorkspace({ author }: { author: string }) {
  const db = useDb();
  const [creating, setCreating] = useState(false);
  const bootstrap = async () => {
    setCreating(true);
    try {
      const workspace = await db
        .insert(app.workspaces, { name: "World tour" })
        .wait({ tier: "local" });
      await db
        .insert(app.members, { workspaceId: workspace.id, author, role: "owner" })
        .wait({ tier: "local" });
      const page = await db
        .insert(app.pages, { workspaceId: workspace.id, title: "Tour book" })
        .wait({ tier: "local" });
      await db
        .insert(app.blocks, {
          workspaceId: workspace.id,
          pageId: page.id,
          position: 10,
          kind: "text",
          payload: { text: "Add the first tour note" },
        })
        .wait({ tier: "local" });
    } finally {
      setCreating(false);
    }
  };
  return (
    <section aria-label="Create workspace">
      <h2>Start a binder</h2>
      <p>Bootstrap is an explicit write action, separate from the initial workspace query.</p>
      <button disabled={creating} onClick={() => void bootstrap()}>
        {creating ? "Creating…" : "Create demo workspace"}
      </button>
    </section>
  );
}

function Workspace({ workspaceId }: { workspaceId: string }) {
  const { data: roots = [] } = useAll(
    app.pages.where({ workspaceId, parentPageId: null }).orderBy("title", "asc").limit(1),
  );
  return roots[0] ? (
    <BinderWorkspace workspaceId={workspaceId} rootPageId={roots[0].id} />
  ) : (
    <p>Waiting for the root page…</p>
  );
}
