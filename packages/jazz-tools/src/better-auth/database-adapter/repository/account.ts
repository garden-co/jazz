import { CleanedWhere } from "better-auth/adapters";
import { co, CoMap, z } from "../../../";
import { JazzRepository } from "./generic";
import { isWhereBySingleField } from "../utils";

const AccountIdIndex = co.list(z.string());

export class AccountRepository extends JazzRepository {
  /**
   * Custom logic:
   * - keep sync accountId index
   */
  async create<T extends z.z.core.$ZodLooseShape>(
    model: string,
    data: T,
    uniqueId?: string,
  ): Promise<{ id: string } & T> {
    const account = await super.create(model, data, uniqueId);

    await this.updateAccountIdIndex(account.id, this.getAccountIdProperty());

    return account;
  }

  /**
   * Custom logic:
   * - if the accountId is in the where clause, get the ids from the index
   */
  async findMany<T extends CoMap>(
    model: string,
    where: CleanedWhere[] | undefined,
    limit?: number,
    sortBy?: { field: string; direction: "asc" | "desc" },
    offset?: number,
  ): Promise<T[]> {
    if (isWhereBySingleField(this.getAccountIdProperty(), where)) {
      const accountIdIndex = await this.getAccountIdIndex(
        this.getAccountIdProperty(),
      );

      const ids = accountIdIndex ?? [];

      if (ids.length === 0) {
        return [];
      }

      // except for accountId clashing from different social providers,
      // ids should contain a single id, max two
      const results = await Promise.all(
        ids.map((id) =>
          super.findById<T>(model, [
            { field: "id", operator: "eq", value: id, connector: "AND" },
          ]),
        ),
      );

      return results.filter((value) => value !== null);
    }

    return super.findMany<T>(model, where, limit, sortBy, offset);
  }

  async deleteValue(model: string, where: CleanedWhere[]): Promise<number> {
    const nodes = await this.findMany<CoMap>(model, where);

    const deleted = await super.deleteValue(model, where);

    for (const node of nodes) {
      const accountId = node._raw.get(this.getAccountIdProperty()) as
        | string
        | undefined;
      if (accountId) {
        await this.deleteAccountIdIndex(accountId, node.id);
      }
    }

    return deleted;
  }
  private async getAccountIdIndex(accountIdProperty: string) {
    const accountIdIndex = await AccountIdIndex.loadUnique(
      accountIdProperty,
      this.owner.id,
      {
        loadAs: this.worker,
      },
    );

    return accountIdIndex;
  }

  private async updateAccountIdIndex(
    accountId: string,
    accountIdProperty: string,
  ) {
    const accountIdIndex = await this.getAccountIdIndex(accountIdProperty);

    const ids = accountIdIndex ?? [];

    await AccountIdIndex.upsertUnique({
      value: [...ids, accountId],
      unique: accountIdProperty,
      owner: this.owner,
    });
  }

  private async deleteAccountIdIndex(
    accountId: string,
    accountIdProperty: string,
  ) {
    const accountIdIndex = await this.getAccountIdIndex(accountIdProperty);

    const ids = accountIdIndex ?? [];

    await AccountIdIndex.upsertUnique({
      value: ids.filter((id) => id !== accountId),
      unique: accountIdProperty,
      owner: this.owner,
    });
  }

  private getAccountIdProperty(): string {
    return (
      this.betterAuthSchema.account?.fields.accountId?.fieldName || "accountId"
    );
  }
}
