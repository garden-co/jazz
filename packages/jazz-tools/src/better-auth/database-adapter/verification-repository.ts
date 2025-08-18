import * as genericRepository from "./jazz-repository.js";
import { isWhereBySingleField } from "./utils";

export const findMany = genericRepository.findMany;
export const update = genericRepository.update;
export const deleteValue = genericRepository.deleteValue;
export const count = genericRepository.count;
export const findById = genericRepository.findById;
export const create: typeof genericRepository.create = async (
  database,
  schema,
  model,
  data,
) => {
  return genericRepository.create(
    database,
    schema,
    model,
    data,
    data["identifier"],
  );
};

export const findOne: typeof genericRepository.findOne = async (
  database,
  model,
  where,
) => {
  if (isWhereBySingleField("identifier", where)) {
    return genericRepository.findByUnique(database, model, where);
  }
  return genericRepository.findOne(database, model, where);
};
