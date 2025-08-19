import { CleanedWhere } from "better-auth/adapters";
import { co, CoMap, z } from "../../../";
import { JazzRepository } from "./generic";
import { isWhereBySingleField } from "../utils";

const EmailIndex = co.map({ user: z.string().nullable() });

export class UserRepository extends JazzRepository {
  /**
   * Custom logic:
   * - sessions are stored inside the user object
   * - keep sync email index
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

    const userEmail = data[this.getEmailProperty()] as string;

    const emailIndex = await this.loadEmailIndex(userEmail);

    if (emailIndex?.user) {
      throw new Error("Email already exists");
    }

    const user = await super.create(model, data, uniqueId);

    await this.updateEmailIndex(userEmail, user.id);

    // @ts-expect-error sessions is in user schema
    user.sessions = co.list(SessionListSchema).create([], user._owner);

    return user;
  }

  /**
   * Custom logic:
   * - if the email is in the where clause, find by email
   */
  async findMany<T extends CoMap>(
    model: string,
    where: CleanedWhere[] | undefined,
    limit?: number,
    sortBy?: { field: string; direction: "asc" | "desc" },
    offset?: number,
  ): Promise<T[]> {
    if (isWhereBySingleField("email", where)) {
      return this.findByEmail<T>(where[0].value as string);
    }

    return super.findMany<T>(model, where, limit, sortBy, offset);
  }

  private getEmailProperty(): string {
    return this.betterAuthSchema.user?.fields.email?.fieldName || "email";
  }

  private async findByEmail<T extends CoMap>(email: string): Promise<T[]> {
    const emailIndex = await this.loadEmailIndex(email);

    const user = emailIndex?.user;

    if (!user) {
      return [];
    }

    return this.findById<T>("user", [
      { field: "id", operator: "eq", value: user, connector: "AND" },
    ]).then((user) => (user ? [user] : []));
  }

  /**
   * Custom logic:
   * - if the email is changed, update the email index
   */
  async update<T>(
    model: string,
    where: CleanedWhere[],
    update: T,
  ): Promise<CoMap[]> {
    const nodes = await this.findMany<CoMap>(model, where);
    if (nodes.length === 0) {
      return [];
    }

    const newEmail = (update as Record<string, any>)[this.getEmailProperty()] as
      | string
      | undefined;

    for (const node of nodes) {
      const oldEmail = node._raw.get(this.getEmailProperty()) as
        | string
        | undefined;
      for (const [key, value] of Object.entries(
        update as Record<string, any>,
      )) {
        // @ts-expect-error Can't know keys at static time
        node[key] = value;
      }

      // if the email is changed, update the email index
      if (
        oldEmail !== newEmail &&
        oldEmail !== undefined &&
        newEmail !== undefined
      ) {
        await this.updateEmailIndex(oldEmail, null);
        await this.updateEmailIndex(newEmail, node.id);
      }
    }

    return nodes;
  }

  async deleteValue(model: string, where: CleanedWhere[]): Promise<number> {
    const nodes = await this.findMany<CoMap>(model, where);

    const deleted = await super.deleteValue(model, where);

    for (const node of nodes) {
      const email = node._raw.get(this.getEmailProperty()) as
        | string
        | undefined;
      if (email) {
        await this.updateEmailIndex(email, null);
      }
    }

    return deleted;
  }

  private async loadEmailIndex(email: string) {
    const emailIndex = await EmailIndex.loadUnique(email, this.owner.id, {
      loadAs: this.worker,
    });

    return emailIndex;
  }

  private async updateEmailIndex(email: string, userId: string | null) {
    await EmailIndex.upsertUnique({
      value: {
        user: userId,
      },
      unique: email,
      owner: this.owner,
    });
  }
}
