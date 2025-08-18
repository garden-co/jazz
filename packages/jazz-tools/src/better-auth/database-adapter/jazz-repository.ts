import { CleanedWhere } from "better-auth/adapters";
import { CoList, CoMap, co, z } from "jazz-tools";
import type { Database } from "./schema.js";
import {
  filterListByWhere,
  isWhereById,
  paginateList,
  sortListByField,
} from "./utils.js";

export async function findOne<T>(
  database: co.loaded<Database>,
  db: Database,
  model: string,
  where: CleanedWhere[],
): Promise<T | null> {
  if (isWhereById(where)) {
    return findById<T>(database, db, model, where);
  }

  return findMany<T>(database, model, where).then(
    (users) => users?.at(0) ?? null,
  );
}

async function findById<T>(
  database: co.loaded<Database>,
  db: Database,
  model: string,
  where: [{ field: "id"; operator: "eq"; value: string; connector: "AND" }],
): Promise<T | null> {
  const id = where[0]!.value;

  if (!id.startsWith("co_")) {
    return null;
  }

  const node = await db.shape.tables.shape[model]?.element.load(id);

  if (!node) {
    return null;
  }

  if (node._raw.get("_deleted")) {
    return null;
  }

  return node as T;
}

export async function findMany<T>(
  database: co.loaded<Database>,
  model: string,
  where: CleanedWhere[] | undefined,
  limit?: number,
  sortBy?: { field: string; direction: "asc" | "desc" },
  offset?: number,
): Promise<T[]> {
  const resolvedRoot = await database.ensureLoaded({
    resolve: {
      tables: {
        [model]: {
          $each: true,
        },
      },
    },
  });

  const list = resolvedRoot.tables?.[model] as CoList<CoMap> | undefined;
  if (!list) {
    return [];
  }

  return [list.filter((item) => item !== null)]
    .map((list) => filterListByWhere(list, where))
    .map((list) => sortListByField(list, sortBy))
    .map((list) => paginateList(list, limit, offset))
    .at(0)! as T[];
}

export async function create<T extends z.z.core.$ZodLooseShape>(
  database: co.loaded<Database>,
  schema: co.Map<T>,
  model: string,
  data: T,
): Promise<T> {
  const resolved = await database.ensureLoaded({
    resolve: {
      tables: {
        [model]: {
          $each: true,
        },
      },
    },
  });

  const list = resolved.tables?.[model] as unknown as CoList<CoMap>;

  // Use the same owner of the table.
  const node = schema.create(data, list._owner);

  list.push(node);

  return node as T;
}

export async function update<T>(
  database: co.loaded<Database>,
  model: string,
  where: CleanedWhere[],
  update: T,
): Promise<T[]> {
  const nodes = await findMany<CoMap>(database, model, where);

  if (nodes.length === 0) {
    return [];
  }

  for (const node of nodes) {
    for (const [key, value] of Object.entries(update as Record<string, any>)) {
      // @ts-expect-error Can't know keys at static time
      node[key] = value;
    }
  }

  return nodes as unknown as T[];
}

export async function deleteValue(
  database: co.loaded<Database>,
  model: string,
  where: CleanedWhere[],
): Promise<number> {
  const items = await findMany<CoMap>(database, model, where);

  if (items.length === 0) {
    return 0;
  }

  const resolved = await database.ensureLoaded({
    resolve: {
      tables: {
        [model]: {
          $each: true,
        },
      },
    },
  });

  if (!resolved) {
    throw new Error("Unable to load values");
  }

  const list = resolved?.tables?.[model] as unknown as CoList<CoMap>;

  for (const toBeDeleted of items) {
    // Get entries without trigger the shallow load
    const index = [...list.entries()].findIndex(
      ([_, value]) => value && value.id === toBeDeleted.id,
    );

    toBeDeleted._raw.set("_deleted", true);

    if (index !== -1) {
      list.splice(index, 1);
    }
  }

  return items.length;
}

export async function count(
  database: co.loaded<Database>,
  model: string,
  where: CleanedWhere[] | undefined,
): Promise<number> {
  return findMany<CoMap>(database, model, where).then(
    (values) => values.length,
  );
}
