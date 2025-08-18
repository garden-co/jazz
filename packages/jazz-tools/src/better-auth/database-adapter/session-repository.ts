import type { co } from "../../tools";
import * as genericRepository from "./jazz-repository.js";
import { isWhereBySingleField } from "./utils";

export const update = genericRepository.update;
export const deleteValue = genericRepository.deleteValue;
export const count = genericRepository.count;
export const findById = genericRepository.findById;

type UserSchema = co.Map<{
  sessions: co.List<co.Map<any>>;
}>;

/**
 * Custom implementation: sessions are stored inside user's CoValue instead of session's table.
 */
export const create: typeof genericRepository.create = async (
  database,
  schema,
  model,
  data,
) => {
  if (typeof data.token !== "string" || typeof data.userId !== "string") {
    throw new Error("Token is required for session creation");
  }

  const user = await genericRepository.findOne<co.loaded<UserSchema>>(
    database,
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

  const SessionListSchema = database.schema.shape.tables.shape.session;

  if (!SessionListSchema) {
    throw new Error("Session list schema not found");
  }

  const session = SessionListSchema.element.create(data, {
    unique: data.token,
    owner: database.db.group,
  });

  sessions.push(session);

  await session.waitForSync();
  await sessions.waitForSync();

  return session;
};

// @ts-expect-error TODO: fix generic type
export const findOne: typeof genericRepository.findOne = async (
  database,
  model,
  where,
) => {
  if (isWhereBySingleField("id", where)) {
    return genericRepository.findById(database, model, where);
  }

  return findMany(database, model, where).then(
    (sessions) => sessions.at(0) ?? null,
  );
};

export const findMany: typeof genericRepository.findMany = async (
  database,
  model,
  where,
) => {
  if (isWhereBySingleField("userId", where)) {
    const user = await genericRepository.findOne<co.loaded<UserSchema>>(
      database,
      "user",
      [
        {
          field: "id",
          operator: "eq",
          value: where[0]!.value,
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

    return sessions.filter(
      (session) =>
        session._raw.get("_deleted") !== true && session.userId === user.id,
    );
  }

  if (isWhereBySingleField("token", where)) {
    const SessionSchema = database.schema.shape.tables.shape[model]?.element;
    if (!SessionSchema) {
      throw new Error("Session schema not found");
    }

    const session = await SessionSchema.loadUnique(
      where[0]!.value,
      database.db.group.id,
    );

    if (!session || session._raw.get("_deleted") === true) {
      return [];
    }

    return [session];
  }

  return genericRepository.findMany(database, model, where);
};
