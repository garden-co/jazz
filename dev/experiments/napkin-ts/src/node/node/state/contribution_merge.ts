// Durable admission boundary for contribution provenance.
import type { Value } from "../../../_extern/node-state.js";
import { type NodeState, nodeErr, invalidStored } from "../index.js";
import { schemaIdKey } from "../../../_extern/node-state.js";

export const contributionMergeMethods = {
  /** Every path that can persist a transaction passes through this first. */
  admitContributionMergeForStorage(this: NodeState, tx: any): Value {
    this.validateContributionMergeOperationIdentities(tx);
    return this.contributionMergeStorageValue(tx.contributionMerge);
  },

  /** Validate strategy-defined operation coordinates before a transaction becomes durable. */
  validateContributionMergeOperationIdentities(this: NodeState, tx: any): void {
    const provenance = tx.contributionMerge;
    if (!provenance) return;
    try {
      provenance.validate();
    } catch {
      throw invalidStored("transaction contribution provenance must be canonical");
    }
    const branchKey = (schema: any, table: any, key: any) => {
      try {
        schema.validateAuthoredBranchKey(table, key);
      } catch (e) {
        throw nodeErr("InvalidBranchKey", String((e as Error).message ?? e));
      }
    };
    for (const intent of provenance.branchWriteIntents) {
      const key = schemaIdKey(intent.authoredSchema);
      const catalogueSchema = this.catalogue.catalogueSchemas.get(key);
      if (!catalogueSchema) throw invalidStored("branch write intent schema is unknown");
      const mapping = this.catalogue.physicalMappings.get(key);
      if (!mapping) throw invalidStored("branch write intent physical mapping is missing");
      const entry = [...mapping.tables].find(
        ([, t]: any) => String(t.tableId) === String(intent.physicalTableId),
      );
      if (!entry) throw invalidStored("branch write intent table is unknown");
      const tableName = entry[0];
      const table = catalogueSchema.schema.tables.find((t: any) => t.name === tableName);
      if (!table) throw invalidStored("branch write intent table schema is missing");
      branchKey(catalogueSchema.schema, table, intent.head);
      if (intent.operation.kind === "ViewUpdateCopy") {
        const evidence = intent.operation.evidence;
        if (
          evidence.table !== tableName ||
          String(evidence.rowUuid) !== String(intent.rowUuid) ||
          !evidence.head.equals(intent.head)
        )
          throw invalidStored("branch write copy evidence is not bound to its intent");
        branchKey(catalogueSchema.schema, table, evidence.head);
        const base = evidence.base.kind === "Current" ? evidence.base.key : evidence.base.branch;
        branchKey(catalogueSchema.schema, table, base);
      }
    }
    // Linear history has no cross-branch contribution calculator.
    if (provenance.substitutions.length)
      throw invalidStored("contribution substitutions require the removed contribution calculator");
  },
};
