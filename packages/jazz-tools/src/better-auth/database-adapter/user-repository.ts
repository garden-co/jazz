import { co, CoList, CoMap } from "../../tools";
import * as genericRepository from "./jazz-repository.js";
import { Database } from "./schema.js";

export const findOne = genericRepository.findOne;
export const findMany = genericRepository.findMany;
export const update = genericRepository.update;
export const deleteValue = genericRepository.deleteValue;
export const count = genericRepository.count;
export const findById = genericRepository.findById;

/**
 * Custom implementation: initialize sessions list in user's CoValue.
 */
export const create: typeof genericRepository.create = async (
  database,
  UserSchema,
  model,
  data,
) => {
  const user = await genericRepository.create(
    database,
    UserSchema,
    model,
    data,
  );

  const SessionListSchema = database.schema.shape.tables.shape.session;

  if (!SessionListSchema) {
    throw new Error("Session list schema not found");
  }

  // @ts-expect-error sessions is in user schema
  user.sessions = co.list(SessionListSchema).create([], database.db.group);

  return user;
};
