import { co, z } from "../../../";
import { JazzRepository } from "./generic";

export class UserRepository extends JazzRepository {
  /**
   * Custom logic: sessions are stored inside the user object.
   */
  async create<T extends z.z.core.$ZodLooseShape>(
    model: string,
    data: T,
    uniqueId?: string,
  ): Promise<{ id: string } & T> {
    const SessionListSchema = this.databaseSchema.shape.tables.shape.session;

    if (!SessionListSchema) {
      throw new Error("Session list schema not found");
    }

    const user = await super.create(model, data, uniqueId);

    // @ts-expect-error sessions is in user schema
    user.sessions = co.list(SessionListSchema).create([], user._owner);

    return user;
  }
}
