import type { Db } from "jazz-tools/backend";
import type { app as schemaApp } from "../schema.js";
import { validateSlug } from "./domain/slugs.js";
import type { IssueItem, ItemKind, ItemState, ItemStatus, VerifiedUser } from "./domain/types.js";

export type { IssueItem, ItemKind, ItemState, ItemStatus, VerifiedUser };

export interface ListedItem extends IssueItem {
  state: ItemState;
  assignee?: VerifiedUser;
}

export interface ListFilters {
  kind?: ItemKind;
  status?: ItemStatus;
}

type App = typeof schemaApp;
type ItemRow = App["items"]["_rowType"];
type ItemStateRow = App["itemStates"]["_rowType"];
type UserRow = App["users"]["_rowType"];

const MISSING_IDENTITY = "A local-first Jazz identity is required.";
const MISSING_VERIFICATION = "A verified GitHub identity is required. Run issues auth github.";

function toVerifiedUser(row: UserRow): VerifiedUser {
  return {
    id: row.id,
    githubUserId: row.githubUserId,
    githubLogin: row.githubLogin,
    verifiedAt: row.verifiedAt,
  };
}

function toIssueItem(row: ItemRow): IssueItem {
  return {
    kind: row.kind,
    title: row.title,
    description: row.description,
    slug: row.slug,
  };
}

function toItemState(row: ItemStateRow | null, slug: string): ItemState {
  if (!row) {
    return {
      itemSlug: slug,
      status: "open",
    };
  }

  return {
    itemSlug: row.itemSlug,
    status: row.status,
    assigneeUserId: row.assigneeUserId ?? undefined,
  };
}

export function createIssueRepository(db: Db, app: App) {
  async function currentUser(): Promise<VerifiedUser> {
    const userId = db.getAuthState().session?.user_id;
    if (!userId) {
      throw new Error(MISSING_IDENTITY);
    }

    const user = await db.one(app.users.where({ jazzUserId: userId }));
    if (!user) {
      throw new Error(MISSING_VERIFICATION);
    }

    return toVerifiedUser(user);
  }

  async function findItem(slug: string): Promise<ItemRow | null> {
    return await db.one(app.items.where({ slug }));
  }

  async function findState(slug: string): Promise<ItemStateRow | null> {
    return await db.one(app.itemStates.where({ itemSlug: slug }));
  }

  async function listItems(filters: ListFilters = {}): Promise<ListedItem[]> {
    const items = await db.all(
      filters.kind ? app.items.where({ kind: filters.kind }) : app.items.where({}),
    );
    const states = await db.all(app.itemStates.where({}));
    const users = await db.all(app.users.where({}));
    const statesBySlug = new Map(states.map((state) => [state.itemSlug, state]));
    const usersById = new Map(users.map((user) => [user.id, user]));

    return items
      .map((item): ListedItem => {
        const state = toItemState(statesBySlug.get(item.slug) ?? null, item.slug);
        const assigneeRow = state.assigneeUserId ? usersById.get(state.assigneeUserId) : undefined;

        return {
          ...toIssueItem(item),
          state,
          ...(assigneeRow ? { assignee: toVerifiedUser(assigneeRow) } : {}),
        };
      })
      .filter((item) => !filters.status || item.state.status === filters.status)
      .sort((left, right) => left.slug.localeCompare(right.slug));
  }

  async function getItem(slug: string): Promise<ListedItem | null> {
    validateSlug(slug);
    return (await listItems({})).find((item) => item.slug === slug) ?? null;
  }

  return {
    currentUser,

    async upsertVerifiedUser(user: VerifiedUser): Promise<VerifiedUser> {
      await db
        .upsert(
          app.users,
          {
            jazzUserId: user.id,
            githubUserId: user.githubUserId,
            githubLogin: user.githubLogin,
            verifiedAt: user.verifiedAt,
          },
          { id: user.id },
        )
        .wait({ tier: "edge" });

      const saved = await db.one(app.users.where({ id: user.id }));
      if (!saved) {
        throw new Error(`Verified user was not saved: ${user.id}`);
      }

      return toVerifiedUser(saved);
    },

    async upsertItem(item: IssueItem): Promise<ListedItem> {
      await currentUser();
      validateSlug(item.slug);

      const existing = await findItem(item.slug);
      if (existing && existing.kind !== item.kind) {
        throw new Error(`Item slug already exists for another kind: ${item.slug}`);
      }

      if (existing) {
        await db
          .update(app.items, existing.id, {
            kind: item.kind,
            title: item.title,
            description: item.description,
            slug: item.slug,
          })
          .wait({ tier: "edge" });
      } else {
        await db
          .insert(app.items, {
            kind: item.kind,
            title: item.title,
            description: item.description,
            slug: item.slug,
          })
          .wait({ tier: "edge" });

        await db
          .insert(app.itemStates, {
            itemSlug: item.slug,
            status: "open",
          })
          .wait({ tier: "edge" });
      }

      const saved = await getItem(item.slug);
      if (!saved) {
        throw new Error(`Item was not saved: ${item.slug}`);
      }
      return saved;
    },

    listItems,

    getItem,

    async assignMe(slug: string): Promise<ListedItem> {
      validateSlug(slug);
      const user = await currentUser();
      const item = await findItem(slug);
      if (!item) {
        throw new Error(`Item not found: ${slug}`);
      }

      const existingState = await findState(slug);
      const nextState = {
        itemSlug: slug,
        status:
          !existingState || existingState.status === "open" ? "in_progress" : existingState.status,
        assigneeUserId: user.id,
      } satisfies ItemState;

      if (existingState) {
        await db.update(app.itemStates, existingState.id, nextState).wait({ tier: "edge" });
      } else {
        await db.insert(app.itemStates, nextState).wait({ tier: "edge" });
      }

      const saved = await getItem(slug);
      if (!saved) {
        throw new Error(`Item not found after assignment: ${slug}`);
      }
      return saved;
    },

    async setStatus(slug: string, status: ItemStatus): Promise<ListedItem> {
      validateSlug(slug);
      await currentUser();
      const item = await findItem(slug);
      if (!item) {
        throw new Error(`Item not found: ${slug}`);
      }

      const existingState = await findState(slug);
      const nextState = {
        itemSlug: slug,
        status,
        ...(existingState?.assigneeUserId ? { assigneeUserId: existingState.assigneeUserId } : {}),
      } satisfies ItemState;

      if (existingState) {
        await db.update(app.itemStates, existingState.id, nextState).wait({ tier: "edge" });
      } else {
        await db.insert(app.itemStates, nextState).wait({ tier: "edge" });
      }

      const saved = await getItem(slug);
      if (!saved) {
        throw new Error(`Item not found after status update: ${slug}`);
      }
      return saved;
    },
  };
}
