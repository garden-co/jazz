import type { Db } from "jazz-tools";
import { app } from "./schema.js";

export const bootstrapSteps = ["workspace", "membership", "page", "block"] as const;
export type BootstrapStep = (typeof bootstrapSteps)[number];

export async function bootstrapWorkspace(
  db: Db,
  author: string,
  options: {
    workspaceId?: string;
    afterStep?: (step: BootstrapStep) => void;
    tier?: "local" | "edge";
  } = {},
) {
  const afterStep = options.afterStep ?? (() => {});
  const tier = options.tier ?? "local";
  let workspace = options.workspaceId
    ? await db.one(app.workspaces.where({ id: options.workspaceId }), { tier: "local" })
    : null;
  workspace ??= await db.one(app.workspaces.orderBy("name", "asc").limit(1), { tier: "local" });
  if (!workspace) {
    workspace = await db.insert(app.workspaces, { name: "World tour" }).wait({ tier });
  }
  afterStep("workspace");

  const membership = await db.one(
    app.members.where({ workspaceId: workspace.id, author }).limit(1),
    {
      tier: "local",
    },
  );
  if (!membership) {
    await db
      .insert(app.members, { workspaceId: workspace.id, author, role: "owner" })
      .wait({ tier });
  }
  afterStep("membership");

  let page = await db.one(
    app.pages.where({ workspaceId: workspace.id, parentPageId: null }).limit(1),
    {
      tier: "local",
    },
  );
  if (!page) {
    page = await db
      .insert(app.pages, { workspaceId: workspace.id, title: "Tour book" })
      .wait({ tier });
  }
  afterStep("page");

  const block = await db.one(
    app.blocks.where({ workspaceId: workspace.id, pageId: page.id }).limit(1),
    {
      tier: "local",
    },
  );
  if (!block) {
    await db
      .insert(app.blocks, {
        workspaceId: workspace.id,
        pageId: page.id,
        position: 10,
        kind: "text",
        payload: { text: "Add the first tour note" },
      })
      .wait({ tier });
  }
  afterStep("block");
  return workspace;
}
