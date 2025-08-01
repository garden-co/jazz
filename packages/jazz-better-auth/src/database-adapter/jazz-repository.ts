import { CleanedWhere } from "better-auth/adapters";
import { Account, CoList, CoMap, co, z } from "jazz-tools";
import { filterListByWhere, paginateList, sortListByField } from "./utils.js";

export async function findOne<T>(
  worker: Account,
  model: string,
  where: CleanedWhere[],
): Promise<T | null> {
  return findMany<T>(worker, model, where).then(
    (users) => users?.at(0) ?? null,
  );
}

export async function findMany<T>(
  worker: Account,
  model: string,
  where: CleanedWhere[] | undefined,
  limit?: number,
  sortBy?: { field: string; direction: "asc" | "desc" },
  offset?: number,
): Promise<T[]> {
  const resolvedRoot = await worker.root?.ensureLoaded({
    resolve: {
      [model]: {
        $each: true,
      },
    },
  });

  const list = resolvedRoot?.[model] as CoList<CoMap> | undefined;
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
  worker: Account,
  schema: co.Map<T>,
  model: string,
  data: T,
): Promise<T> {
  const node = schema.create(data, worker);

  const resolved = await worker.root!.ensureLoaded({
    resolve: {
      [model]: true,
    },
  });

  const list = resolved[model] as unknown as CoList<CoMap>;

  list.push(node);

  return node as T;
}

export async function update<T>(
  worker: Account,
  model: string,
  where: CleanedWhere[],
  update: T,
): Promise<T[]> {
  const nodes = await findMany<CoMap>(worker, model, where);

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
  worker: Account,
  model: string,
  where: CleanedWhere[],
): Promise<number> {
  const values = await findMany<CoMap>(worker, model, where).then((values) =>
    values.map((value) => value.id),
  );

  if (values.length === 0) {
    return 0;
  }

  const resolved = await worker.root?.ensureLoaded({
    resolve: {
      [model]: true,
    },
  });

  if (!resolved) {
    throw new Error("Unable to load values");
  }

  const list = resolved[model] as unknown as CoList<CoMap>;

  for (const toBeDeleted of values) {
    // Get entries without trigger the shallow load
    const index = [...list.entries()].findIndex(
      ([_, value]) => value && value.id === toBeDeleted,
    );

    if (index !== -1) {
      list.splice(index, 1);
    }
  }

  return values.length;
}

export async function count(
  worker: Account,
  model: string,
  where: CleanedWhere[] | undefined,
): Promise<number> {
  return findMany<CoMap>(worker, model, where).then((values) => values.length);
}
