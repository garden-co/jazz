import { CleanedWhere } from "better-auth/adapters";
import { Account, CoList, CoMap, Group, co, z } from "../../../";
import type { Database } from "../schema.js";
import {
  filterListByWhere,
  isWhereBySingleField,
  paginateList,
  sortListByField,
} from "../utils.js";
import { BetterAuthDbSchema } from "better-auth/db";

export class JazzRepository {
  protected databaseSchema: Database;
  protected databaseRoot: co.loaded<Database, { group: true }>;
  protected worker: Account;
  protected owner: Group;
  protected betterAuthSchema: BetterAuthDbSchema;

  constructor(
    databaseSchema: Database,
    databaseRoot: co.loaded<Database, { group: true }>,
    worker: Account,
    betterAuthSchema: BetterAuthDbSchema = {},
  ) {
    this.databaseSchema = databaseSchema;
    this.databaseRoot = databaseRoot;
    this.worker = worker;
    this.owner = databaseRoot.group;
    this.betterAuthSchema = betterAuthSchema;
  }

  ensureSync() {
    return this.worker.waitForAllCoValuesSync();
  }

  async create<T extends z.z.core.$ZodLooseShape>(
    model: string,
    data: T,
    uniqueId?: string,
  ): Promise<{ id: string } & T> {
    const schema = this.getSchema(model);

    const resolved = await this.databaseRoot.ensureLoaded({
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
    const node = schema.create(data, { owner: list._owner, unique: uniqueId });

    list.push(node);

    return node;
  }

  async findOne<T extends CoMap>(
    model: string,
    where: CleanedWhere[],
  ): Promise<T | null> {
    return this.findMany<T>(model, where, 1).then(
      (users) => users?.at(0) ?? null,
    );
  }

  async findById<T extends CoMap>(
    model: string,
    where: [{ field: "id"; operator: "eq"; value: string; connector: "AND" }],
  ): Promise<T | null> {
    const id = where[0]!.value;

    if (!id.startsWith("co_")) {
      return null;
    }

    const node = await this.getSchema(model).load(id, { loadAs: this.worker });

    if (!node) {
      return null;
    }

    if (node._raw.get("_deleted")) {
      return null;
    }

    return node;
  }

  async findByUnique<T extends CoMap>(
    model: string,
    where: [{ field: string; operator: "eq"; value: string; connector: "AND" }],
  ): Promise<T | null> {
    const value = where[0]!.value;

    const node = await this.getSchema(model).loadUnique(value, this.owner.id, {
      loadAs: this.worker,
    });

    if (!node) {
      return null;
    }

    if (node._raw.get("_deleted")) {
      return null;
    }

    return node;
  }

  async findMany<T extends CoMap>(
    model: string,
    where: CleanedWhere[] | undefined,
    limit?: number,
    sortBy?: { field: string; direction: "asc" | "desc" },
    offset?: number,
  ): Promise<T[]> {
    // ensure schema exists
    this.getSchema(model);

    if (isWhereBySingleField("id", where)) {
      return this.findById<T>(model, where).then((node) =>
        node ? [node] : [],
      );
    }

    const resolvedRoot = await this.databaseRoot.ensureLoaded({
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

    return this.filterSortPaginateList<T>(list, where, limit, sortBy, offset);
  }

  async update<T>(
    model: string,
    where: CleanedWhere[],
    update: T,
  ): Promise<CoMap[]> {
    const nodes = await this.findMany<CoMap>(model, where);

    if (nodes.length === 0) {
      return [];
    }

    for (const node of nodes) {
      for (const [key, value] of Object.entries(
        update as Record<string, any>,
      )) {
        // @ts-expect-error Can't know keys at static time
        node[key] = value;
      }
    }

    return nodes;
  }

  async deleteValue(model: string, where: CleanedWhere[]): Promise<number> {
    const items = await this.findMany<CoMap>(model, where);

    if (items.length === 0) {
      return 0;
    }

    const resolved = await this.databaseRoot.ensureLoaded({
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

  async count(
    model: string,
    where: CleanedWhere[] | undefined,
  ): Promise<number> {
    return this.findMany<CoMap>(model, where).then((values) => values.length);
  }

  protected getSchema(model: string) {
    const schema = this.databaseSchema.shape.tables.shape[model]?.element;
    if (!schema) {
      throw new Error(`Schema for model "${model}" not found`);
    }
    return schema;
  }

  protected filterSortPaginateList<T extends CoMap>(
    list: CoList<CoMap | null>,
    where: CleanedWhere[] | undefined,
    limit?: number,
    sortBy?: { field: string; direction: "asc" | "desc" },
    offset?: number,
  ): T[] {
    // ignore nullable values and soft deleted items
    return [
      list.filter(
        (item) => item !== null && item._raw.get("_deleted") !== true,
      ),
    ]
      .map((list) => filterListByWhere(list, where))
      .map((list) => sortListByField(list, sortBy))
      .map((list) => paginateList(list, limit, offset))
      .at(0)! as T[];
  }
}
