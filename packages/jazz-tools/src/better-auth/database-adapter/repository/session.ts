import { BetterAuthDbSchema } from "better-auth/db";
import { CleanedWhere } from "better-auth/adapters";
import { co, CoMap, z, Account } from "../../../";
import { JazzRepository } from "./generic";
import { UserRepository } from "./user";
import {
  containWhereByField,
  extractWhereByField,
  filterListByWhere,
  isWhereBySingleField,
} from "../utils";
import type { Database } from "../schema";

type UserSchema = co.Map<{
  sessions: co.List<co.Map<any>>;
}>;

export class SessionRepository extends JazzRepository {
  protected userRepository: UserRepository;

  constructor(
    databaseSchema: Database,
    databaseRoot: co.loaded<Database, { group: true }>,
    worker: Account,
    betterAuthSchema: BetterAuthDbSchema = {},
  ) {
    super(databaseSchema, databaseRoot, worker, betterAuthSchema);
    this.userRepository = new UserRepository(
      databaseSchema,
      databaseRoot,
      worker,
      betterAuthSchema,
    );
  }

  /**
   * Custom logic: sessions are stored inside the user object
   */
  async create<T extends z.z.core.$ZodLooseShape>(
    model: string,
    data: T,
    uniqueId?: string,
  ): Promise<{ id: string } & T> {
    if (typeof data.token !== "string" || typeof data.userId !== "string") {
      throw new Error("Token and userId are required for session creation");
    }

    const user = await this.userRepository.findById<co.loaded<UserSchema>>(
      "user",
      [
        {
          field: "id",
          operator: "eq",
          value: data.userId,
          connector: "AND",
        },
      ],
    );

    if (!user) {
      throw new Error("User not found");
    }

    const { sessions } = await user.ensureLoaded({
      resolve: {
        sessions: true,
      },
    });

    const session = this.getSchema("session").create(data, {
      unique: data.token,
      owner: this.owner,
    });

    sessions.push(session);

    await session.waitForSync();
    await sessions.waitForSync();

    return session;
  }

  /**
   * Custom logic: sessions are stored inside the user object.
   */
  async findMany<T extends CoMap>(
    model: string,
    where: CleanedWhere[] | undefined,
    limit?: number,
    sortBy?: { field: string; direction: "asc" | "desc" },
    offset?: number,
  ): Promise<T[]> {
    if (isWhereBySingleField("id", where)) {
      return this.findById<T>(model, where).then((node) =>
        node ? [node] : [],
      );
    }

    if (isWhereBySingleField("token", where)) {
      return this.findByUnique<T>(model, where).then((node) =>
        node ? [node] : [],
      );
    }

    if (containWhereByField("userId", where)) {
      const [userIdWhere, otherWhere] = extractWhereByField("userId", where);

      const user = await this.userRepository.findById<co.loaded<UserSchema>>(
        "user",
        [
          {
            field: "id",
            operator: "eq",
            value: userIdWhere!.value as string,
            connector: "AND",
          },
        ],
      );

      if (!user) {
        throw new Error("User not found");
      }

      const { sessions } = await user.ensureLoaded({
        resolve: {
          sessions: {
            $each: true,
          },
        },
      });

      return this.filterSortPaginateList<T>(
        sessions,
        otherWhere,
        limit,
        sortBy,
        offset,
      );
    }

    throw new Error(
      "Unable to find session with where: " + JSON.stringify(where),
    );
  }

  /**
   * Custom logic: sessions are stored inside the user object.
   */
  async deleteValue(model: string, where: CleanedWhere[]): Promise<number> {
    if (
      isWhereBySingleField("token", where) ||
      isWhereBySingleField("id", where)
    ) {
      const [item] = await this.findMany<{ userId: string } & CoMap>(
        model,
        where,
      );
      if (!item) {
        return 0;
      }

      const userId = item.userId;

      return this.deleteSession(userId, [item]);
    }

    if (containWhereByField("userId", where)) {
      const [userIdWhere, otherWhere] = extractWhereByField("userId", where);

      const user = await this.userRepository.findById<co.loaded<UserSchema>>(
        "user",
        [
          {
            field: "id",
            operator: "eq",
            value: userIdWhere!.value as string,
            connector: "AND",
          },
        ],
      );

      if (!user) {
        throw new Error("User not found");
      }

      const { sessions } = await user.ensureLoaded({
        resolve: {
          sessions: {
            $each: true,
          },
        },
      });

      const filteredSessions = filterListByWhere(
        sessions.filter(
          (item) => item !== null && item._raw.get("_deleted") !== true,
        ),
        otherWhere,
      );

      return this.deleteSession(userIdWhere!.value as string, filteredSessions);
    }

    throw new Error(
      "Unable to delete session with where: " + JSON.stringify(where),
    );
  }

  private async deleteSession(userId: string, items: CoMap[]): Promise<number> {
    const user = await this.userRepository.findById<co.loaded<UserSchema>>(
      "user",
      [
        {
          field: "id",
          operator: "eq",
          value: userId,
          connector: "AND",
        },
      ],
    );

    if (!user) {
      throw new Error("User not found");
    }

    const { sessions } = await user.ensureLoaded({
      resolve: {
        sessions: true,
      },
    });

    for (const toBeDeleted of items) {
      // Get entries without trigger the shallow load
      const index = [...sessions.entries()].findIndex(
        ([_, value]) => value && value.id === toBeDeleted.id,
      );

      toBeDeleted._raw.set("_deleted", true);

      if (index !== -1) {
        sessions.splice(index, 1);
      }
    }

    await sessions.waitForSync();

    return items.length;
  }
}
