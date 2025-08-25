import { CleanedWhere } from "better-auth/adapters";
import { CoMap, z } from "jazz-tools";
import { JazzRepository } from "./generic";
import { isWhereBySingleField } from "../utils";

export class VerificationRepository extends JazzRepository {
  /**
   * Custom logic: property identifier is used as uniqueId
   */
  async create<T extends z.z.core.$ZodLooseShape>(
    model: string,
    data: T,
    uniqueId?: string,
  ): Promise<{ id: string } & T> {
    return super.create<T>(model, data, data["identifier"]);
  }

  /**
   * Custom logic: property identifier is used as uniqueId
   * If we look for identifier, we use findByUnique instead of findMany
   */
  async findMany<T extends CoMap>(
    model: string,
    where: CleanedWhere[] | undefined,
    limit?: number,
    sortBy?: { field: string; direction: "asc" | "desc" },
    offset?: number,
  ): Promise<T[]> {
    if (isWhereBySingleField("identifier", where)) {
      return this.findByUnique<T>(model, where).then((node) =>
        node ? [node] : [],
      );
    }

    return super.findMany<T>(model, where, limit, sortBy, offset);
  }
}
